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
	"github.com/pgflo/pg_flo/pkg/utils"
)

// BulkParquetWriter handles copy phase parquet writing with size-based rotation
// NO transaction logic, NO consolidation - just bulk copy with optional metadata
type BulkParquetWriter struct {
	basePath             string
	maxFileSize          int64
	includePgFloMetadata bool
	logger               utils.Logger
	currentFile          *os.File
	currentWriter        *pqarrow.FileWriter
	currentSize          int64
	fileCount            int
	currentTable         string
}

// NewBulkParquetWriter creates a copy phase parquet writer
func NewBulkParquetWriter(basePath string, maxFileSize int64, includePgFloMetadata bool, logger utils.Logger) (*BulkParquetWriter, error) {
	copyDir := filepath.Join(basePath, "copy")
	if err := os.MkdirAll(copyDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create copy directory: %w", err)
	}

	return &BulkParquetWriter{
		basePath:             copyDir,
		maxFileSize:          maxFileSize,
		includePgFloMetadata: includePgFloMetadata,
		logger:               logger,
		fileCount:            0,
	}, nil
}

// WriteBulkData writes copy data for a table with size-based rotation
func (w *BulkParquetWriter) WriteBulkData(tableName string, operations []*utils.CDCMessage) (int64, int, error) {
	if len(operations) == 0 {
		return 0, 0, nil
	}

	// Switch table context if needed
	if w.currentTable != tableName {
		if err := w.closeCurrentFile(); err != nil {
			return 0, 0, err
		}
		w.currentTable = tableName
		w.fileCount = 0
	}

	var totalBytes int64
	filesCreated := 0

	for _, op := range operations {
		// Check if we need to rotate file (size-based)
		estimatedSize := w.estimateOperationSize(op)

		if w.currentWriter == nil || (w.currentSize+estimatedSize > w.maxFileSize && w.currentSize > 0) {
			if err := w.rotateFile(); err != nil {
				return totalBytes, filesCreated, err
			}
			filesCreated++
		}

		// Write operation to current file
		if err := w.writeOperation(op); err != nil {
			return totalBytes, filesCreated, err
		}

		totalBytes += estimatedSize
		w.currentSize += estimatedSize
	}

	return totalBytes, filesCreated + w.fileCount, nil
}

// Close closes the current file and cleans up
func (w *BulkParquetWriter) Close() error {
	return w.closeCurrentFile()
}

// rotateFile closes current file and opens a new one
func (w *BulkParquetWriter) rotateFile() error {
	if err := w.closeCurrentFile(); err != nil {
		return err
	}

	w.fileCount++
	fileName := fmt.Sprintf("%s_copy_%s_%03d.parquet",
		w.currentTable,
		time.Now().Format("20060102_150405"),
		w.fileCount)

	filePath := filepath.Join(w.basePath, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create copy file: %w", err)
	}

	w.currentFile = file
	w.currentSize = 0

	w.logger.Info().
		Str("file", fileName).
		Str("table", w.currentTable).
		Int("file_number", w.fileCount).
		Msg("Created new copy parquet file")

	return nil
}

// closeCurrentFile closes the current parquet file
func (w *BulkParquetWriter) closeCurrentFile() error {
	if w.currentWriter != nil {
		if err := w.currentWriter.Close(); err != nil {
			w.logger.Error().Err(err).Msg("Failed to close parquet writer")
		}
		w.currentWriter = nil
	}

	if w.currentFile != nil {
		if err := w.currentFile.Close(); err != nil {
			w.logger.Error().Err(err).Msg("Failed to close parquet file")
		}
		w.currentFile = nil
	}

	return nil
}

// writeOperation writes a single copy operation (no CDC metadata)
func (w *BulkParquetWriter) writeOperation(op *utils.CDCMessage) error {
	// Initialize writer for this table if needed
	if w.currentWriter == nil {
		schema := w.createCopySchema(op)

		props := parquet.NewWriterProperties()
		arrowProps := pqarrow.DefaultWriterProps()

		writer, err := pqarrow.NewFileWriter(schema, w.currentFile, props, arrowProps)
		if err != nil {
			return fmt.Errorf("failed to create parquet writer: %w", err)
		}
		w.currentWriter = writer
	}

	// Create record batch for this operation (bulk copy - no CDC metadata)
	recordBatch, err := w.createCopyRecordBatch(op)
	if err != nil {
		return fmt.Errorf("failed to create record batch: %w", err)
	}
	defer recordBatch.Release()

	if err := w.currentWriter.Write(recordBatch); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	return nil
}

// createCopySchema creates Arrow schema for copy phase (NO CDC metadata columns)
func (w *BulkParquetWriter) createCopySchema(op *utils.CDCMessage) *arrow.Schema {
	var fields []arrow.Field

	// Only include actual table columns - NO CDC metadata
	if op.Columns != nil {
		for _, col := range op.Columns {
			arrowType := w.postgresTypeToArrow(col.DataType)
			fields = append(fields, arrow.Field{
				Name: col.Name,
				Type: arrowType,
			})
		}
	}

	return arrow.NewSchema(fields, nil)
}

// createCopyRecordBatch creates record batch for copy data (NO CDC metadata)
func (w *BulkParquetWriter) createCopyRecordBatch(op *utils.CDCMessage) (arrow.Record, error) {
	pool := memory.NewGoAllocator()

	var arrays []arrow.Array
	schema := w.createCopySchema(op)

	// Build arrays from copy data only
	if op.CopyData != nil {
		for i, field := range schema.Fields() {
			if i < len(op.CopyData) {
				arr := w.buildCopyArray(pool, field.Type, [][]byte{op.CopyData[i]})
				arrays = append(arrays, arr)
			}
		}
	}

	return array.NewRecord(schema, arrays, 1), nil
}

// buildCopyArray builds Arrow array for copy data
func (w *BulkParquetWriter) buildCopyArray(pool memory.Allocator, _ arrow.DataType, values [][]byte) arrow.Array {
	// Simple string array for now - can be enhanced for specific types
	builder := array.NewStringBuilder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			builder.AppendString(string(val))
		}
	}

	return builder.NewStringArray()
}

// postgresTypeToArrow converts PostgreSQL types to Arrow types (simplified)
func (w *BulkParquetWriter) postgresTypeToArrow(_ uint32) arrow.DataType {
	// Simplified mapping - can be enhanced based on PostgreSQL OID types
	return arrow.BinaryTypes.String
}

// estimateOperationSize estimates the size of an operation for rotation
func (w *BulkParquetWriter) estimateOperationSize(op *utils.CDCMessage) int64 {
	size := int64(100) // Base overhead

	if op.CopyData != nil {
		for _, data := range op.CopyData {
			size += int64(len(data))
		}
	}

	return size
}
