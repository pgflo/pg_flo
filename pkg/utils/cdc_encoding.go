package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ConvertToPgCompatibleOutput converts a Go value to its PostgreSQL output format.
func ConvertToPgCompatibleOutput(value interface{}, oid uint32) (string, error) {
	if value == nil {
		return "", nil
	}

	switch oid {
	case pgtype.BoolOID:
		return strconv.FormatBool(value.(bool)), nil
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		switch v := value.(type) {
		case int:
			return strconv.FormatInt(int64(v), 10), nil
		case int32:
			return strconv.FormatInt(int64(v), 10), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		default:
			return fmt.Sprintf("%d", value), nil
		}
	case pgtype.Float4OID, pgtype.Float8OID:
		return strconv.FormatFloat(value.(float64), 'f', -1, 64), nil
	case pgtype.NumericOID:
		return fmt.Sprintf("%v", value), nil
	case pgtype.TextOID, pgtype.VarcharOID:
		return value.(string), nil
	case pgtype.ByteaOID:
		if byteaData, ok := value.([]byte); ok {
			return fmt.Sprintf("\\x%x", byteaData), nil
		}
		return "", fmt.Errorf("invalid bytea data type")
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return value.(time.Time).Format(time.RFC3339Nano), nil
	case pgtype.DateOID:
		return value.(time.Time).Format("2006-01-02"), nil
	case pgtype.JSONOID:
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			return "", fmt.Errorf("unsupported type for JSON data: %T", value)
		}
	case pgtype.JSONBOID:
		if jsonBytes, ok := value.([]byte); ok {
			return string(jsonBytes), nil
		}
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(jsonBytes), nil
	case pgtype.TextArrayOID, pgtype.VarcharArrayOID,
		pgtype.Int2ArrayOID, pgtype.Int4ArrayOID, pgtype.Int8ArrayOID,
		pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.BoolArrayOID:
		arrayBytes, err := EncodeArray(value)
		if err != nil {
			return "", err
		}
		return string(arrayBytes), nil
	default:
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("failed to marshal value to JSON: %w", err)
		}
		return string(jsonBytes), nil
	}
}

// EncodeArray encodes a slice of values into a PostgreSQL array format.
func EncodeArray(value interface{}) ([]byte, error) {
	var elements []string

	switch slice := value.(type) {
	case []interface{}:
		for _, v := range slice {
			elem, err := encodeArrayElement(v)
			if err != nil {
				return nil, err
			}
			elements = append(elements, elem)
		}
	case []string:
		elements = append(elements, slice...)
	case []int, []int32, []int64, []float32, []float64, []bool:
		sliceValue := reflect.ValueOf(slice)
		for i := 0; i < sliceValue.Len(); i++ {
			elem, err := encodeArrayElement(sliceValue.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			elements = append(elements, elem)
		}
	default:
		return nil, fmt.Errorf("unsupported slice type: %T", value)
	}

	return []byte("{" + strings.Join(elements, ",") + "}"), nil
}

// encodeArrayElement encodes a single array element into a string representation.
func encodeArrayElement(v interface{}) (string, error) {
	if v == nil {
		return "NULL", nil
	}

	switch val := v.(type) {
	case string:
		return val, nil
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", val), nil
	case bool:
		return strconv.FormatBool(val), nil
	case time.Time:
		return val.Format(time.RFC3339Nano), nil
	case []byte:
		return fmt.Sprintf("\\x%x", val), nil
	default:
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return "", fmt.Errorf("failed to marshal array element to JSON: %w", err)
		}
		return string(jsonBytes), nil
	}
}
