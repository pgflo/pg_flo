package utils

import (
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
		return arrow.FixedWidthTypes.Timestamp_us
	case pgtype.TimestamptzOID:
		return arrow.FixedWidthTypes.Timestamp_us
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
