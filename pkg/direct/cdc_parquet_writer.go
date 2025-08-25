package direct

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// CDCParquetWriter handles stream phase parquet writing with CDC metadata
// Includes: CDC metadata columns, size-based rotation with transaction boundary awareness
type CDCParquetWriter struct {
	basePath             string
	maxFileSize          int64
	includePgFloMetadata bool
	logger               utils.Logger

	// Per-table file management
	tableWriters map[string]*tableWriter
}

// tableWriter manages a single table's parquet files
type tableWriter struct {
	tableName     string
	currentFile   *os.File
	currentWriter *pqarrow.FileWriter
	currentSize   int64
	fileCount     int
	schema        *arrow.Schema
}

// NewCDCParquetWriter creates a stream phase parquet writer
func NewCDCParquetWriter(basePath string, maxFileSize int64, includePgFloMetadata bool, logger utils.Logger) (*CDCParquetWriter, error) {
	streamDir := filepath.Join(basePath, "stream")
	if err := os.MkdirAll(streamDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create stream directory: %w", err)
	}

	return &CDCParquetWriter{
		basePath:             streamDir,
		maxFileSize:          maxFileSize,
		includePgFloMetadata: includePgFloMetadata,
		logger:               logger,
		tableWriters:         make(map[string]*tableWriter),
	}, nil
}

// WriteTransaction writes a consolidated transaction with CDC metadata
// Respects transaction boundaries while doing size-based rotation
func (w *CDCParquetWriter) WriteTransaction(operations []*utils.CDCMessage) error {
	if len(operations) == 0 {
		return nil
	}

	// Group operations by table
	tableOps := w.groupOperationsByTable(operations)

	// Write each table's operations
	for tableName, ops := range tableOps {
		if err := w.writeTableOperations(tableName, ops); err != nil {
			return fmt.Errorf("failed to write operations for table %s: %w", tableName, err)
		}
	}

	return nil
}

// Close closes all open files
func (w *CDCParquetWriter) Close() error {
	for _, tw := range w.tableWriters {
		if err := tw.close(); err != nil {
			w.logger.Error().Err(err).Str("table", tw.tableName).Msg("Failed to close table writer")
		}
	}
	return nil
}

// writeTableOperations writes operations for a single table
func (w *CDCParquetWriter) writeTableOperations(tableName string, operations []*utils.CDCMessage) error {
	tw, exists := w.tableWriters[tableName]
	if !exists {
		tw = &tableWriter{
			tableName: tableName,
			fileCount: 0,
		}
		w.tableWriters[tableName] = tw
	}

	// Estimate transaction size
	transactionSize := w.estimateTransactionSize(operations)

	// Check if we need to rotate before writing this transaction
	// This respects transaction boundaries - never split a transaction across files
	if tw.currentWriter != nil && tw.currentSize+transactionSize > w.maxFileSize && tw.currentSize > 0 {
		if err := tw.close(); err != nil {
			return fmt.Errorf("failed to close current file for rotation: %w", err)
		}

		w.logger.Info().
			Str("table", tableName).
			Int64("current_size", tw.currentSize).
			Int64("transaction_size", transactionSize).
			Int64("max_size", w.maxFileSize).
			Msg("Rotating file due to size limit (transaction boundary preserved)")
	}

	// Open new file if needed
	if tw.currentWriter == nil {
		if err := w.openNewFileForTable(tw, operations[0]); err != nil {
			return err
		}
	}

	// Write all operations in this transaction to the same file
	for _, op := range operations {
		if err := w.writeOperationWithCDC(tw, op); err != nil {
			return fmt.Errorf("failed to write operation: %w", err)
		}
	}

	tw.currentSize += transactionSize

	w.logger.Debug().
		Str("table", tableName).
		Int("operations", len(operations)).
		Int64("transaction_size", transactionSize).
		Int64("file_size", tw.currentSize).
		Msg("Wrote transaction to CDC parquet")

	return nil
}

// openNewFileForTable opens a new parquet file for a table
func (w *CDCParquetWriter) openNewFileForTable(tw *tableWriter, sampleOp *utils.CDCMessage) error {
	tw.fileCount++
	fileName := fmt.Sprintf("%s_stream_%s_%03d.parquet",
		tw.tableName,
		time.Now().Format("20060102_150405"),
		tw.fileCount)

	filePath := filepath.Join(w.basePath, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create stream file: %w", err)
	}

	// Create schema with CDC metadata
	schema := w.createCDCSchema(sampleOp)

	props := parquet.NewWriterProperties()
	arrowProps := pqarrow.DefaultWriterProps()

	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	tw.currentFile = file
	tw.currentWriter = writer
	tw.currentSize = 0
	tw.schema = schema

	w.logger.Info().
		Str("file", fileName).
		Str("table", tw.tableName).
		Int("file_number", tw.fileCount).
		Msg("Created new CDC parquet file")

	return nil
}

// writeOperationWithCDC writes a single operation with CDC metadata
func (w *CDCParquetWriter) writeOperationWithCDC(tw *tableWriter, op *utils.CDCMessage) error {
	recordBatch, err := w.createCDCRecordBatch(tw.schema, op)
	if err != nil {
		return fmt.Errorf("failed to create CDC record batch: %w", err)
	}
	defer recordBatch.Release()

	if err := tw.currentWriter.Write(recordBatch); err != nil {
		return fmt.Errorf("failed to write CDC record batch: %w", err)
	}

	return nil
}

// createCDCSchema creates Arrow schema with CDC metadata columns
func (w *CDCParquetWriter) createCDCSchema(op *utils.CDCMessage) *arrow.Schema {
	var fields []arrow.Field

	// Add table columns
	if op.Columns != nil {
		for _, col := range op.Columns {
			fields = append(fields, arrow.Field{
				Name: col.Name,
				Type: arrow.BinaryTypes.String, // Simplified
			})
		}
	}

	// Add CDC metadata columns
	fields = append(fields, []arrow.Field{
		{Name: "_pg_flo_operation", Type: arrow.BinaryTypes.String},           // INSERT/UPDATE/DELETE
		{Name: "_pg_flo_lsn", Type: arrow.BinaryTypes.String},                 // WAL LSN
		{Name: "_pg_flo_timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms}, // Commit timestamp
		{Name: "_pg_flo_schema", Type: arrow.BinaryTypes.String},              // Schema name
		{Name: "_pg_flo_table", Type: arrow.BinaryTypes.String},               // Table name
	}...)

	// Add old value columns for UPDATE/DELETE
	if op.Type == utils.OperationUpdate || op.Type == utils.OperationDelete {
		if op.Columns != nil {
			for _, col := range op.Columns {
				fields = append(fields, arrow.Field{
					Name: "_old_" + col.Name,
					Type: arrow.BinaryTypes.String,
				})
			}
		}
	}

	return arrow.NewSchema(fields, nil)
}

// createCDCRecordBatch creates record batch with CDC metadata
func (w *CDCParquetWriter) createCDCRecordBatch(schema *arrow.Schema, op *utils.CDCMessage) (arrow.Record, error) {
	pool := memory.NewGoAllocator()
	var arrays []arrow.Array

	// Build arrays for each field in schema
	for _, field := range schema.Fields() {
		switch field.Name {
		case "_pg_flo_operation":
			arrays = append(arrays, w.buildStringArray(pool, []string{string(op.Type)}))
		case "_pg_flo_lsn":
			arrays = append(arrays, w.buildStringArray(pool, []string{op.LSN}))
		case "_pg_flo_timestamp":
			arrays = append(arrays, w.buildTimestampArray(pool, []time.Time{op.EmittedAt}))
		case "_pg_flo_schema":
			arrays = append(arrays, w.buildStringArray(pool, []string{op.Schema}))
		case "_pg_flo_table":
			arrays = append(arrays, w.buildStringArray(pool, []string{op.Table}))
		default:
			// Handle table columns and old value columns
			arrays = append(arrays, w.buildColumnArray(pool, field.Name, op))
		}
	}

	return array.NewRecord(schema, arrays, 1), nil
}

// buildStringArray builds a string array
func (w *CDCParquetWriter) buildStringArray(pool memory.Allocator, values []string) arrow.Array {
	builder := array.NewStringBuilder(pool)
	defer builder.Release()

	for _, val := range values {
		builder.AppendString(val)
	}

	return builder.NewStringArray()
}

// buildTimestampArray builds a timestamp array
func (w *CDCParquetWriter) buildTimestampArray(pool memory.Allocator, values []time.Time) arrow.Array {
	builder := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Millisecond})
	defer builder.Release()

	for _, val := range values {
		builder.AppendTime(val)
	}

	return builder.NewTimestampArray()
}

// buildColumnArray builds array for table columns or old value columns
func (w *CDCParquetWriter) buildColumnArray(pool memory.Allocator, fieldName string, op *utils.CDCMessage) arrow.Array {
	builder := array.NewStringBuilder(pool)
	defer builder.Release()

	// Handle old value columns
	if len(fieldName) > 5 && fieldName[:5] == "_old_" {
		columnName := fieldName[5:]
		value := w.getColumnValue(columnName, op.OldTuple, op.Columns)
		if value != nil {
			builder.AppendString(string(value))
		} else {
			builder.AppendNull()
		}
		return builder.NewStringArray()
	}

	// Handle regular table columns
	value := w.getColumnValue(fieldName, op.NewTuple, op.Columns)
	if value != nil {
		builder.AppendString(string(value))
	} else {
		builder.AppendNull()
	}

	return builder.NewStringArray()
}

// getColumnValue extracts column value from tuple
func (w *CDCParquetWriter) getColumnValue(columnName string, tuple *pglogrepl.TupleData, columns []*pglogrepl.RelationMessageColumn) []byte {
	if tuple == nil || columns == nil {
		return nil
	}

	for i, col := range columns {
		if col.Name == columnName && i < len(tuple.Columns) {
			if tuple.Columns[i] != nil {
				return tuple.Columns[i].Data
			}
		}
	}

	return nil
}

// groupOperationsByTable groups operations by table name
func (w *CDCParquetWriter) groupOperationsByTable(operations []*utils.CDCMessage) map[string][]*utils.CDCMessage {
	result := make(map[string][]*utils.CDCMessage)

	for _, op := range operations {
		tableName := fmt.Sprintf("%s.%s", op.Schema, op.Table)
		result[tableName] = append(result[tableName], op)
	}

	return result
}

// estimateTransactionSize estimates the size of a transaction
func (w *CDCParquetWriter) estimateTransactionSize(operations []*utils.CDCMessage) int64 {
	size := int64(0)

	for _, op := range operations {
		size += w.estimateOperationSize(op)
	}

	return size
}

// estimateOperationSize estimates the size of an operation
func (w *CDCParquetWriter) estimateOperationSize(op *utils.CDCMessage) int64 {
	size := int64(200) // Base overhead for CDC metadata

	// Add table data size
	if op.NewTuple != nil {
		for _, col := range op.NewTuple.Columns {
			if col != nil {
				size += int64(len(col.Data))
			}
		}
	}

	// Add old tuple size for UPDATE/DELETE
	if op.OldTuple != nil {
		for _, col := range op.OldTuple.Columns {
			if col != nil {
				size += int64(len(col.Data))
			}
		}
	}

	return size
}

// close closes the table writer
func (tw *tableWriter) close() error {
	if tw.currentWriter != nil {
		if err := tw.currentWriter.Close(); err != nil {
			return err
		}
		tw.currentWriter = nil
	}

	if tw.currentFile != nil {
		if err := tw.currentFile.Close(); err != nil {
			return err
		}
		tw.currentFile = nil
	}

	return nil
}
