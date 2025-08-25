package utils

import (
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/jackc/pgx/v5/pgtype"
)

// PostgresTypeToArrowType converts PostgreSQL OID to Arrow type
// This mapping is inspired by pg_parquet project for compatibility
func PostgresTypeToArrowType(pgTypeOID uint32) arrow.DataType {
	switch pgTypeOID {
	case pgtype.BoolOID:
		return arrow.FixedWidthTypes.Boolean
	case pgtype.Int2OID:
		return arrow.PrimitiveTypes.Int16
	case pgtype.Int4OID:
		return arrow.PrimitiveTypes.Int32
	case pgtype.Int8OID:
		return arrow.PrimitiveTypes.Int64
	case pgtype.Float4OID:
		return arrow.PrimitiveTypes.Float32
	case pgtype.Float8OID:
		return arrow.PrimitiveTypes.Float64
	case pgtype.NumericOID:
		return arrow.BinaryTypes.String // Preserve precision as string
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return arrow.BinaryTypes.String
	case pgtype.ByteaOID:
		return arrow.BinaryTypes.Binary
	case pgtype.DateOID:
		return arrow.FixedWidthTypes.Date32
	case pgtype.TimeOID:
		return arrow.FixedWidthTypes.Time64us
	case pgtype.TimestampOID:
		// PostgreSQL timestamp (no timezone) → Arrow timestamp (no timezone)
		return arrow.FixedWidthTypes.Timestamp_us
	case pgtype.TimestamptzOID:
		// PostgreSQL timestamptz → Arrow timestamp with UTC timezone
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case pgtype.UUIDOID:
		return arrow.BinaryTypes.String
	case pgtype.JSONOID, pgtype.JSONBOID:
		return arrow.BinaryTypes.String
	// Array types - represent as strings to preserve PostgreSQL array format
	case pgtype.Int2ArrayOID, pgtype.Int4ArrayOID, pgtype.Int8ArrayOID,
		pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.TextArrayOID,
		pgtype.BoolArrayOID, pgtype.NumericArrayOID, pgtype.ByteaArrayOID,
		pgtype.TimestampArrayOID, pgtype.UUIDArrayOID, pgtype.DateArrayOID,
		pgtype.TimestamptzArrayOID, pgtype.JSONBArrayOID:
		return arrow.BinaryTypes.String
	default:
		// Unknown types as strings for safety
		return arrow.BinaryTypes.String
	}
}

// BuildArrowArrayFromPostgresData builds Arrow array from PostgreSQL raw data
func BuildArrowArrayFromPostgresData(pool memory.Allocator, dataType arrow.DataType, values [][]byte) arrow.Array {
	switch dt := dataType.(type) {
	case *arrow.BooleanType:
		return buildBooleanArray(pool, values)
	case *arrow.Int16Type:
		return buildInt16Array(pool, values)
	case *arrow.Int32Type:
		return buildInt32Array(pool, values)
	case *arrow.Int64Type:
		return buildInt64Array(pool, values)
	case *arrow.Float32Type:
		return buildFloat32Array(pool, values)
	case *arrow.Float64Type:
		return buildFloat64Array(pool, values)
	case *arrow.StringType:
		return buildStringArray(pool, values)
	case *arrow.BinaryType:
		return buildBinaryArray(pool, values)
	case *arrow.Date32Type:
		return buildDate32Array(pool, values)
	case *arrow.TimestampType:
		return buildTimestampArray(pool, values, dt)
	case *arrow.Time64Type:
		return buildTime64Array(pool, values)
	default:
		// Fallback to string for unknown types
		return buildStringArray(pool, values)
	}
}

// NewBuilder creates an appropriate Arrow array builder for the given type
func NewBuilder(allocator memory.Allocator, dataType arrow.DataType) array.Builder {
	switch dataType.ID() {
	case arrow.BOOL:
		return array.NewBooleanBuilder(allocator)
	case arrow.INT16:
		return array.NewInt16Builder(allocator)
	case arrow.INT32:
		return array.NewInt32Builder(allocator)
	case arrow.INT64:
		return array.NewInt64Builder(allocator)
	case arrow.FLOAT32:
		return array.NewFloat32Builder(allocator)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(allocator)
	case arrow.STRING:
		return array.NewStringBuilder(allocator)
	case arrow.BINARY:
		return array.NewBinaryBuilder(allocator, arrow.BinaryTypes.Binary)
	case arrow.DATE32:
		return array.NewDate32Builder(allocator)
	case arrow.TIME64:
		return array.NewTime64Builder(allocator, dataType.(*arrow.Time64Type))
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(allocator, dataType.(*arrow.TimestampType))
	default:
		// Fallback to string builder
		return array.NewStringBuilder(allocator)
	}
}

// AppendValueToBuilder appends a value to the appropriate builder
func AppendValueToBuilder(builder array.Builder, value interface{}, _ uint32) {
	if value == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Int16Builder:
		if v, ok := ToInt64(value); ok && v >= -32768 && v <= 32767 {
			b.Append(int16(v)) //nolint:gosec // Already validated bounds
		} else {
			b.AppendNull()
		}
	case *array.Int32Builder:
		if v, ok := ToInt64(value); ok && v >= -2147483648 && v <= 2147483647 {
			b.Append(int32(v)) //nolint:gosec // Already validated bounds
		} else {
			b.AppendNull()
		}
	case *array.Int64Builder:
		if v, ok := ToInt64(value); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Float32Builder:
		if v, ok := ToFloat64(value); ok {
			b.Append(float32(v))
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if v, ok := ToFloat64(value); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.StringBuilder:
		b.Append(ToString(value))
	case *array.BinaryBuilder:
		if data, ok := value.([]byte); ok {
			b.Append(data)
		} else {
			b.Append([]byte(ToString(value)))
		}
	case *array.TimestampBuilder:
		if ts, ok := ToTimestamp(value); ok {
			b.Append(ts)
		} else {
			b.AppendNull()
		}
	default:
		// Fallback: append as string to string builder
		if sb, ok := builder.(*array.StringBuilder); ok {
			sb.Append(ToString(value))
		} else {
			builder.AppendNull()
		}
	}
}

// Helper functions for building specific Arrow array types from PostgreSQL data

func buildBooleanArray(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewBooleanBuilder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			str := strings.TrimSpace(string(val))
			builder.Append(str == "t" || str == "true" || str == "1")
		}
	}
	return builder.NewBooleanArray()
}

func buildInt16Array(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewInt16Builder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if i, err := strconv.ParseInt(string(val), 10, 16); err == nil {
			builder.Append(int16(i))
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewInt16Array()
}

func buildInt32Array(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewInt32Builder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if i, err := strconv.ParseInt(string(val), 10, 32); err == nil {
			builder.Append(int32(i))
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewInt32Array()
}

func buildInt64Array(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewInt64Builder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if i, err := strconv.ParseInt(string(val), 10, 64); err == nil {
			builder.Append(i)
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewInt64Array()
}

func buildFloat32Array(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewFloat32Builder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if f, err := strconv.ParseFloat(string(val), 32); err == nil {
			builder.Append(float32(f))
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewFloat32Array()
}

func buildFloat64Array(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewFloat64Builder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if f, err := strconv.ParseFloat(string(val), 64); err == nil {
			builder.Append(f)
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewFloat64Array()
}

func buildStringArray(pool memory.Allocator, values [][]byte) arrow.Array {
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

func buildBinaryArray(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else {
			builder.Append(val)
		}
	}
	return builder.NewBinaryArray()
}

func buildDate32Array(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewDate32Builder(pool)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if t, err := time.Parse("2006-01-02", string(val)); err == nil {
			days := int32(t.Unix() / 86400) //nolint:gosec // Days calculation is safe
			builder.Append(arrow.Date32(days))
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewDate32Array()
}

func buildTimestampArray(pool memory.Allocator, values [][]byte, timestampType *arrow.TimestampType) arrow.Array {
	builder := array.NewTimestampBuilder(pool, timestampType)
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if t, err := time.Parse("2006-01-02 15:04:05", string(val)); err == nil {
			builder.AppendTime(t)
		} else if t, err := time.Parse(time.RFC3339, string(val)); err == nil {
			builder.AppendTime(t)
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewTimestampArray()
}

func buildTime64Array(pool memory.Allocator, values [][]byte) arrow.Array {
	builder := array.NewTime64Builder(pool, &arrow.Time64Type{Unit: arrow.Microsecond})
	defer builder.Release()

	for _, val := range values {
		if val == nil {
			builder.AppendNull()
		} else if t, err := time.Parse("15:04:05", string(val)); err == nil {
			micros := int64(t.Hour()*3600+t.Minute()*60+t.Second()) * 1000000
			builder.Append(arrow.Time64(micros))
		} else {
			builder.AppendNull()
		}
	}
	return builder.NewTime64Array()
}
