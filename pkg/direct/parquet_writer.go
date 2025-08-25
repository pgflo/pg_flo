package direct

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/decimal128"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// LocalParquetWriter implements ParquetWriter for local disk storage
type LocalParquetWriter struct {
	basePath string
	logger   utils.Logger
}

// NewLocalParquetWriter creates a new local parquet writer
func NewLocalParquetWriter(basePath string, logger utils.Logger) (*LocalParquetWriter, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(basePath, 0750); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &LocalParquetWriter{
		basePath: basePath,
		logger:   logger,
	}, nil
}

// WriteTransaction writes a consolidated transaction to parquet files organized by table
// WriteTransaction writes consolidated CDC operations to local parquet files
func (w *LocalParquetWriter) WriteTransaction(operations []*utils.CDCMessage) error {
	if len(operations) == 0 {
		return nil
	}

	// Group operations by table for separate parquet files
	tableOps := make(map[string][]*utils.CDCMessage)
	for _, op := range operations {
		tableKey := fmt.Sprintf("%s.%s", op.Schema, op.Table)
		tableOps[tableKey] = append(tableOps[tableKey], op)
	}

	// Write each table to its own parquet file
	for tableKey, ops := range tableOps {
		if err := w.writeTableToParquet(tableKey, ops); err != nil {
			return fmt.Errorf("failed to write table %s to parquet: %w", tableKey, err)
		}
	}

	return nil
}

// writeTableToParquet writes operations for a single table to a parquet file
func (w *LocalParquetWriter) writeTableToParquet(tableKey string, operations []*utils.CDCMessage) error {
	if len(operations) == 0 {
		return nil
	}

	// Generate filename with timestamp
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.parquet", tableKey, timestamp)
	filePath := filepath.Join(w.basePath, filename)

	w.logger.Info().
		Str("table", tableKey).
		Str("file", filename).
		Int("operations", len(operations)).
		Msg("Writing table operations to parquet file")

	// Convert []*utils.CDCMessage to []utils.CDCMessage for the writeTableParquet function
	convertedOps := make([]utils.CDCMessage, len(operations))
	for i, op := range operations {
		convertedOps[i] = *op
	}

	// Write actual parquet file using Apache Arrow
	return w.writeTableParquet(filePath, convertedOps)
}

// writeOperationsAsJSON writes operations as JSON (placeholder for parquet)
func (w *LocalParquetWriter) writeOperationsAsJSON(filePath string, operations []*utils.CDCMessage) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			w.logger.Error().Err(closeErr).Msg("Failed to close parquet file")
		}
	}()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	for _, op := range operations {
		// Convert operation to a simple map for JSON output
		data := map[string]interface{}{
			"operation": string(op.Type),
			"schema":    op.Schema,
			"table":     op.Table,
			"lsn":       op.LSN,
			"timestamp": op.EmittedAt,
			"columns":   w.extractColumnData(op),
		}

		if err := encoder.Encode(data); err != nil {
			return fmt.Errorf("failed to encode operation: %w", err)
		}
	}

	w.logger.Info().
		Str("file", filePath).
		Int("operations", len(operations)).
		Msg("Successfully wrote operations to JSON file")

	return nil
}

// extractColumnData extracts column data from a CDC message
func (w *LocalParquetWriter) extractColumnData(op *utils.CDCMessage) map[string]interface{} {
	data := make(map[string]interface{})

	// Extract data based on operation type
	switch op.Type {
	case utils.OperationInsert:
		if op.NewTuple != nil {
			for i, col := range op.Columns {
				if i < len(op.NewTuple.Columns) && op.NewTuple.Columns[i] != nil {
					data[col.Name] = string(op.NewTuple.Columns[i].Data)
				}
			}
		} else if op.CopyData != nil {
			for i, col := range op.Columns {
				if i < len(op.CopyData) && op.CopyData[i] != nil {
					data[col.Name] = string(op.CopyData[i])
				}
			}
		}

	case utils.OperationUpdate:
		if op.NewTuple != nil {
			for i, col := range op.Columns {
				if i < len(op.NewTuple.Columns) && op.NewTuple.Columns[i] != nil {
					data[col.Name] = string(op.NewTuple.Columns[i].Data)
				}
			}
		}
		// Add old values with _old prefix
		if op.OldTuple != nil {
			for i, col := range op.Columns {
				if i < len(op.OldTuple.Columns) && op.OldTuple.Columns[i] != nil {
					data["_old_"+col.Name] = string(op.OldTuple.Columns[i].Data)
				}
			}
		}

	case utils.OperationDelete:
		if op.OldTuple != nil {
			for i, col := range op.Columns {
				if i < len(op.OldTuple.Columns) && op.OldTuple.Columns[i] != nil {
					data[col.Name] = string(op.OldTuple.Columns[i].Data)
				}
			}
		}
	}

	return data
}

// WriteTransactionLegacy is a legacy method for backward compatibility
func (w *LocalParquetWriter) WriteTransactionLegacy(_ context.Context, tx *Transaction) (string, error) {
	if len(tx.Operations) == 0 {
		return "", fmt.Errorf("transaction %s has no operations", tx.ID)
	}

	// Group operations by table
	tableOps := w.groupOperationsByTable(tx.Operations)

	// Generate transaction directory path
	txDir := filepath.Join(w.basePath, fmt.Sprintf("tx_%s_%d", tx.ID, tx.CommitLSN))
	if err := os.MkdirAll(txDir, 0750); err != nil {
		return "", fmt.Errorf("failed to create transaction directory: %w", err)
	}

	// Write parquet file for each table
	for tableName, operations := range tableOps {
		fileName := fmt.Sprintf("%s.parquet", tableName)
		filePath := filepath.Join(txDir, fileName)

		if err := w.writeTableParquet(filePath, operations); err != nil {
			return "", fmt.Errorf("failed to write parquet for table %s: %w", tableName, err)
		}

		w.logger.Debug().
			Str("file_path", filePath).
			Str("table", tableName).
			Int("operations", len(operations)).
			Msg("Wrote table parquet file")
	}

	w.logger.Info().
		Str("tx_id", tx.ID).
		Str("tx_dir", txDir).
		Int("tables", len(tableOps)).
		Int("total_operations", len(tx.Operations)).
		Msg("Wrote transaction parquet files")

	return txDir, nil
}

// Close cleans up any resources (no-op for local writer)
func (w *LocalParquetWriter) Close() error {
	return nil
}

// groupOperationsByTable groups CDC operations by their table name
func (w *LocalParquetWriter) groupOperationsByTable(operations []utils.CDCMessage) map[string][]utils.CDCMessage {
	result := make(map[string][]utils.CDCMessage)

	for _, op := range operations {
		tableName := fmt.Sprintf("%s_%s", op.Schema, op.Table)
		result[tableName] = append(result[tableName], op)
	}

	return result
}

// writeTableParquet writes operations for a single table to a parquet file
func (w *LocalParquetWriter) writeTableParquet(filePath string, operations []utils.CDCMessage) error {
	if len(operations) == 0 {
		return nil
	}

	// Create Arrow schema based on the operations
	schema := w.createArrowSchema(operations)

	// Create Arrow record batch from operations
	recordBatch, err := w.createRecordBatch(schema, operations)
	if err != nil {
		return fmt.Errorf("failed to create record batch: %w", err)
	}
	defer recordBatch.Release()

	// Write to parquet file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	props := parquet.NewWriterProperties() // Use default compression for now
	arrowProps := pqarrow.DefaultWriterProps()

	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer func() { _ = writer.Close() }()

	if err := writer.Write(recordBatch); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	return nil
}

// createArrowSchema creates an Arrow schema for CDC operations with proper type mapping
func (w *LocalParquetWriter) createArrowSchema(operations []utils.CDCMessage) *arrow.Schema {
	fields := []arrow.Field{
		{Name: "_pg_flo_operation", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_pg_flo_lsn", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_pg_flo_timestamp", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: false},
		{Name: "_pg_flo_tx_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_pg_flo_schema", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "_pg_flo_table", Type: arrow.BinaryTypes.String, Nullable: false},
	}

	// Collect all unique column names and their types from operations
	columnTypes := make(map[string]uint32)
	for _, op := range operations {
		for _, col := range op.Columns {
			if col != nil {
				columnTypes[col.Name] = col.DataType
				// For UPDATE/DELETE operations, also include old data columns
				if op.Type == utils.OperationUpdate || op.Type == utils.OperationDelete {
					columnTypes["_old_"+col.Name] = col.DataType
				}
			}
		}
	}

	// Add data columns with proper Arrow types based on PostgreSQL OIDs
	for col, dataType := range columnTypes {
		arrowType := w.postgresOIDToArrowType(dataType)
		fields = append(fields, arrow.Field{
			Name:     col,
			Type:     arrowType,
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil)
}

// postgresOIDToArrowType converts PostgreSQL OID types to Arrow types
// Based on pg_parquet approach
func (w *LocalParquetWriter) postgresOIDToArrowType(oid uint32) arrow.DataType {
	switch oid {
	case 16: // bool
		return arrow.FixedWidthTypes.Boolean
	case 21: // int2/smallint
		return arrow.PrimitiveTypes.Int16
	case 23: // int4/integer
		return arrow.PrimitiveTypes.Int32
	case 20: // int8/bigint
		return arrow.PrimitiveTypes.Int64
	case 700: // float4/real
		return arrow.PrimitiveTypes.Float32
	case 701: // float8/double precision
		return arrow.PrimitiveTypes.Float64
	case 1700: // numeric/decimal - use high precision decimal
		return &arrow.Decimal128Type{Precision: 38, Scale: 18}
	case 25, 1043: // text, varchar
		return arrow.BinaryTypes.String
	case 17: // bytea
		return arrow.BinaryTypes.Binary
	case 1082: // date
		return arrow.FixedWidthTypes.Date32
	case 1114: // timestamp without timezone
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case 1184: // timestamp with timezone
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case 1083: // time without timezone
		return arrow.FixedWidthTypes.Time64us
	case 1266: // time with timezone
		return arrow.FixedWidthTypes.Time64us
	case 2950: // uuid
		return arrow.BinaryTypes.String // Store UUID as string for simplicity
	case 114, 3802: // json, jsonb
		return arrow.BinaryTypes.String // JSON stored as string
	case 1009, 1015, 1007, 1001: // Arrays: text[], varchar[], int4[], bytea[]
		// For now, store arrays as strings - we can enhance this later
		return arrow.BinaryTypes.String
	default:
		// Fallback to string for unknown types
		return arrow.BinaryTypes.String
	}
}

// createRecordBatch creates an Arrow record batch from CDC operations with proper types
func (w *LocalParquetWriter) createRecordBatch(schema *arrow.Schema, operations []utils.CDCMessage) (arrow.Record, error) {
	mem := memory.NewGoAllocator()

	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = w.createBuilder(mem, field.Type)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()

	// Fill the builders with data
	for _, op := range operations {
		for i, field := range schema.Fields() {
			if err := w.appendFieldValue(builders[i], field, op); err != nil {
				return nil, fmt.Errorf("failed to append field %s: %w", field.Name, err)
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create record batch
	return array.NewRecord(schema, arrays, int64(len(operations))), nil
}

// createBuilder creates the appropriate builder for the given Arrow type
func (w *LocalParquetWriter) createBuilder(mem memory.Allocator, dataType arrow.DataType) array.Builder {
	switch dt := dataType.(type) {
	case *arrow.BooleanType:
		return array.NewBooleanBuilder(mem)
	case *arrow.Int16Type:
		return array.NewInt16Builder(mem)
	case *arrow.Int32Type:
		return array.NewInt32Builder(mem)
	case *arrow.Int64Type:
		return array.NewInt64Builder(mem)
	case *arrow.Float32Type:
		return array.NewFloat32Builder(mem)
	case *arrow.Float64Type:
		return array.NewFloat64Builder(mem)
	case *arrow.Decimal128Type:
		return array.NewDecimal128Builder(mem, dt)
	case *arrow.StringType:
		return array.NewStringBuilder(mem)
	case *arrow.BinaryType:
		return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	case *arrow.Date32Type:
		return array.NewDate32Builder(mem)
	case *arrow.TimestampType:
		return array.NewTimestampBuilder(mem, dt)
	case *arrow.Time64Type:
		return array.NewTime64Builder(mem, dt)
	default:
		// Fallback to string builder
		return array.NewStringBuilder(mem)
	}
}

// appendFieldValue appends the appropriate value to the builder based on field type
func (w *LocalParquetWriter) appendFieldValue(builder array.Builder, field arrow.Field, op utils.CDCMessage) error {
	switch field.Name {
	case "_pg_flo_operation":
		builder.(*array.StringBuilder).Append(string(op.Type))
	case "_pg_flo_lsn":
		builder.(*array.StringBuilder).Append(op.LSN)
	case "_pg_flo_timestamp":
		builder.(*array.TimestampBuilder).Append(arrow.Timestamp(op.EmittedAt.UnixMicro()))
	case "_pg_flo_tx_id":
		builder.(*array.StringBuilder).Append(op.LSN) // Use LSN as transaction ID for now
	case "_pg_flo_schema":
		builder.(*array.StringBuilder).Append(op.Schema)
	case "_pg_flo_table":
		builder.(*array.StringBuilder).Append(op.Table)
	default:
		// Handle data columns with proper type conversion
		return w.appendDataColumnValue(builder, field, op)
	}
	return nil
}

// appendDataColumnValue appends a data column value with proper type conversion
func (w *LocalParquetWriter) appendDataColumnValue(builder array.Builder, field arrow.Field, op utils.CDCMessage) error {
	value := w.getColumnValue(field.Name, op)

	if value == nil {
		builder.AppendNull()
		return nil
	}

	// Convert based on the target Arrow type
	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if boolVal, ok := w.convertToBool(value); ok {
			b.Append(boolVal)
		} else {
			b.AppendNull()
		}
	case *array.Int16Builder:
		if intVal, ok := w.convertToInt16(value); ok {
			b.Append(intVal)
		} else {
			b.AppendNull()
		}
	case *array.Int32Builder:
		if intVal, ok := w.convertToInt32(value); ok {
			b.Append(intVal)
		} else {
			b.AppendNull()
		}
	case *array.Int64Builder:
		if intVal, ok := w.convertToInt64(value); ok {
			b.Append(intVal)
		} else {
			b.AppendNull()
		}
	case *array.Float32Builder:
		if floatVal, ok := w.convertToFloat32(value); ok {
			b.Append(floatVal)
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if floatVal, ok := w.convertToFloat64(value); ok {
			b.Append(floatVal)
		} else {
			b.AppendNull()
		}
	case *array.Decimal128Builder:
		if decVal, ok := w.convertToDecimal128(value); ok {
			b.Append(decVal)
		} else {
			b.AppendNull()
		}
	case *array.StringBuilder:
		b.Append(fmt.Sprintf("%v", value))
	case *array.BinaryBuilder:
		if data, ok := value.([]byte); ok {
			b.Append(data)
		} else {
			b.AppendNull()
		}
	case *array.Date32Builder:
		if dateVal, ok := w.convertToDate32(value); ok {
			b.Append(dateVal)
		} else {
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		if tsVal, ok := w.convertToTimestamp(value); ok {
			b.Append(tsVal)
		} else {
			b.AppendNull()
		}
	case *array.Time64Builder:
		if timeVal, ok := w.convertToTime64(value); ok {
			b.Append(timeVal)
		} else {
			b.AppendNull()
		}
	default:
		// Fallback to string representation
		if sb, ok := builder.(*array.StringBuilder); ok {
			sb.Append(fmt.Sprintf("%v", value))
		} else {
			builder.AppendNull()
		}
	}

	return nil
}

// getColumnValue extracts a column value from a CDC operation optimized for parquet
func (w *LocalParquetWriter) getColumnValue(columnName string, op utils.CDCMessage) interface{} {
	if len(columnName) > 5 && columnName[:5] == "_old_" {
		realCol := columnName[5:]
		return w.getRawColumnValue(realCol, op, true)
	}
	return w.getRawColumnValue(columnName, op, false)
}

// getRawColumnValue gets decoded column data using existing pgx infrastructure
func (w *LocalParquetWriter) getRawColumnValue(columnName string, op utils.CDCMessage, useOldValues bool) interface{} {
	// Use the existing GetColumnValue method which already handles pgx decoding properly
	value, err := op.GetColumnValue(columnName, useOldValues)
	if err != nil {
		w.logger.Debug().
			Str("column", columnName).
			Str("use_old", fmt.Sprintf("%v", useOldValues)).
			Err(err).
			Msg("Failed to get column value, returning nil")
		return nil
	}

	// Return the decoded value directly - no additional conversion needed
	return value
}

// Note: optimizeForParquet function removed - we now use pgx decoding directly

// Type conversion functions using pgx-decoded values (minimal conversion)
func (w *LocalParquetWriter) convertToBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case string:
		// Only fallback to string parsing if pgx gave us a string
		return v == "t" || v == "true", true
	default:
		return false, false
	}
}

func (w *LocalParquetWriter) convertToInt16(value interface{}) (int16, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case int16:
		return v, true
	case int32:
		if v >= -32768 && v <= 32767 {
			return int16(v), true
		}
	case int64:
		if v >= -32768 && v <= 32767 {
			return int16(v), true
		}
	default:
		// pgx should give us the right type - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for int16 conversion")
	}
	return 0, false
}

func (w *LocalParquetWriter) convertToInt32(value interface{}) (int32, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case int32:
		return v, true
	case int16:
		return int32(v), true
	case int64:
		if v >= -2147483648 && v <= 2147483647 {
			return int32(v), true
		}
	default:
		// pgx should give us the right type - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for int32 conversion")
	}
	return 0, false
}

func (w *LocalParquetWriter) convertToInt64(value interface{}) (int64, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int16:
		return int64(v), true
	default:
		// pgx should give us the right type - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for int64 conversion")
	}
	return 0, false
}

func (w *LocalParquetWriter) convertToFloat32(value interface{}) (float32, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	default:
		// pgx should give us the right type - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for float32 conversion")
	}
	return 0, false
}

func (w *LocalParquetWriter) convertToFloat64(value interface{}) (float64, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	default:
		// pgx should give us the right type - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for float64 conversion")
	}
	return 0, false
}

func (w *LocalParquetWriter) convertToDecimal128(value interface{}) (decimal128.Num, bool) {
	// Handle pgtype.Numeric directly using its methods - no string conversion!
	switch v := value.(type) {
	case pgtype.Numeric:
		if !v.Valid || v.NaN || v.InfinityModifier != pgtype.Finite {
			return decimal128.Num{}, false
		}

		// Try Float64Value first for most numeric values
		if float64Val, err := v.Float64Value(); err == nil && float64Val.Valid {
			// Convert float64 to decimal128 using Arrow's method
			dec, err := decimal128.FromFloat64(float64Val.Float64, 38, 18)
			if err == nil {
				return dec, true
			}
		}

		// For very large numbers, try Int64Value
		if int64Val, err := v.Int64Value(); err == nil && int64Val.Valid {
			return decimal128.FromI64(int64Val.Int64), true
		}

		// Fallback: use the internal big.Int if available
		if v.Int != nil {
			// Convert big.Int to int64 if it fits
			if v.Int.IsInt64() {
				baseVal := v.Int.Int64()
				// Apply exponent scaling
				if v.Exp > 0 {
					// Positive exponent: multiply
					for i := int32(0); i < v.Exp; i++ {
						baseVal *= 10
					}
				} else if v.Exp < 0 {
					// Negative exponent: keep precision by scaling to fit decimal128
					// This preserves the fractional part
					scale := -v.Exp
					if scale <= 18 { // Within decimal128 scale limits
						return decimal128.New(baseVal, uint64(scale)), true
					}
				}
				return decimal128.FromI64(baseVal), true
			}
		}

		return decimal128.Num{}, false

	case string:
		// Fallback for string representation
		if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
			dec, err := decimal128.FromFloat64(floatVal, 38, 18)
			if err == nil {
				return dec, true
			}
		}

	default:
		// Log unexpected types for debugging
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for decimal conversion")
		return decimal128.Num{}, false
	}

	return decimal128.Num{}, false
}

func (w *LocalParquetWriter) convertToDate32(value interface{}) (arrow.Date32, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case time.Time:
		// pgx already decoded as time.Time - use directly
		days := int32(v.Unix() / 86400)
		return arrow.Date32(days), true
	default:
		// pgx should give us time.Time for date types - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for date conversion")
	}
	return 0, false
}

func (w *LocalParquetWriter) convertToTimestamp(value interface{}) (arrow.Timestamp, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case time.Time:
		// pgx already decoded as time.Time - use directly, no parsing needed!
		return arrow.Timestamp(v.UnixMicro()), true
	default:
		// pgx should give us time.Time for timestamp types - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for timestamp conversion")
	}
	return 0, false
}

func (w *LocalParquetWriter) convertToTime64(value interface{}) (arrow.Time64, bool) {
	// Use pgx decoded value directly - no string parsing needed
	switch v := value.(type) {
	case time.Time:
		// pgx decoded as time.Time - extract time part
		duration := time.Duration(v.Hour())*time.Hour +
			time.Duration(v.Minute())*time.Minute +
			time.Duration(v.Second())*time.Second +
			time.Duration(v.Nanosecond())*time.Nanosecond
		return arrow.Time64(duration.Microseconds()), true
	default:
		// pgx should give us time.Time for time types - log unexpected cases
		w.logger.Debug().
			Str("type", fmt.Sprintf("%T", v)).
			Interface("value", v).
			Msg("Unexpected type for time conversion")
	}
	return 0, false
}
