package utils

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQLTypeConverter handles PostgreSQL data type conversions using pgx
type PostgreSQLTypeConverter struct {
	pgTypeMap *pgtype.Map
}

// NewPostgreSQLTypeConverter creates a new PostgreSQLTypeConverter
func NewPostgreSQLTypeConverter() *PostgreSQLTypeConverter {
	return &PostgreSQLTypeConverter{
		pgTypeMap: pgtype.NewMap(),
	}
}

// DecodePostgreSQLValue decodes PostgreSQL binary data using pgx natively
func (c *PostgreSQLTypeConverter) DecodePostgreSQLValue(data []byte, dataTypeOID uint32, format int16) (interface{}, error) {
	if data == nil {
		return nil, nil
	}

	// For binary format (WAL data), handle complex types that pgx struggles with as strings
	// This prevents encoding/decoding round-trip issues with complex structures
	if format == 0 { // Binary format from WAL
		switch dataTypeOID {
		case pgtype.Int4ArrayOID, pgtype.Int8ArrayOID, pgtype.TextArrayOID, pgtype.BoolArrayOID,
			pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.NumericArrayOID,
			pgtype.ByteaArrayOID, pgtype.TimestampArrayOID, pgtype.UUIDArrayOID:
			// Arrays: preserve exact PostgreSQL representation to avoid multidimensional flattening
			return string(data), nil
		case 3908, 3910, 3912, 3904, 3926, 3906: // Range types - tsrange, tstzrange, daterange, int4range, int8range, numrange
			return string(data), nil
		case 3614: // tsvector
			return string(data), nil
		case 3615: // tsquery
			return string(data), nil
		case 142: // xml
			return string(data), nil
		case pgtype.JSONOID, pgtype.JSONBOID: // json, jsonb
			// Preserve exact JSON formatting to maintain data integrity
			return string(data), nil
		case pgtype.ByteaOID:
			// Never convert bytea to string as it can contain null bytes
			// Let pgx handle it properly
			break
		}
	}

	dt, ok := c.pgTypeMap.TypeForOID(dataTypeOID)
	if !ok {
		// For unknown types, only convert to string if it's safe (text format or non-bytea)
		if format == 1 || dataTypeOID != pgtype.ByteaOID {
			return string(data), nil
		}
		// For binary bytea with unknown type, return raw bytes
		return data, nil
	}

	value, err := dt.Codec.DecodeValue(c.pgTypeMap, dataTypeOID, format, data)
	if err != nil {
		// On decode error, only convert to string if it's safe
		if format == 1 || dataTypeOID != pgtype.ByteaOID {
			return string(data), nil
		}
		// For binary bytea with decode error, return raw bytes
		return data, nil
	}

	return value, nil
}

// EncodePostgreSQLValue encodes a Go value to PostgreSQL text format using pgx
func (c *PostgreSQLTypeConverter) EncodePostgreSQLValue(value interface{}, dataTypeOID uint32) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	// Handle special cases that need specific PostgreSQL formats
	switch dataTypeOID {
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		if t, ok := value.(time.Time); ok {
			return []byte(t.Format("2006-01-02 15:04:05.999999-07:00")), nil
		}
	case pgtype.DateOID:
		if t, ok := value.(time.Time); ok {
			return []byte(t.Format("2006-01-02")), nil
		}
	case 3908, 3910, 3912, 3904, 3926, 3906: // Range types (tsrange, tstzrange, daterange, int4range, int8range, numrange)
		// For range types, convert back to string representation
		return []byte(fmt.Sprintf("%v", value)), nil
	case 3614: // tsvector
		return []byte(fmt.Sprintf("%v", value)), nil
	case 3615: // tsquery
		return []byte(fmt.Sprintf("%v", value)), nil
	case 142: // xml
		return []byte(fmt.Sprintf("%v", value)), nil
	case pgtype.JSONOID, pgtype.JSONBOID: // json, jsonb
		// For JSON types, preserve exact formatting
		return []byte(fmt.Sprintf("%v", value)), nil
	}

	dt, ok := c.pgTypeMap.TypeForOID(dataTypeOID)
	if !ok {
		return []byte(fmt.Sprintf("%v", value)), nil
	}

	plan := dt.Codec.PlanEncode(c.pgTypeMap, dataTypeOID, pgtype.TextFormatCode, value)
	if plan == nil {
		return []byte(fmt.Sprintf("%v", value)), nil
	}

	encoded, err := plan.Encode(value, nil)
	if err != nil {
		return []byte(fmt.Sprintf("%v", value)), nil
	}

	return encoded, nil
}

// Global instance for easy access
var GlobalPostgreSQLTypeConverter = NewPostgreSQLTypeConverter()
