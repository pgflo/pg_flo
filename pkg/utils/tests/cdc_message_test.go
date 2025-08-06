package utils_tests

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeValue(t *testing.T) {
	testTime, err := time.Parse("2006-01-02 15:04:05.999999", "2023-01-01 12:30:00.123456")
	require.NoError(t, err)

	testDate, err := time.Parse("2006-01-02", "2023-01-01")
	require.NoError(t, err)

	testCases := []struct {
		name          string
		data          []byte
		dataType      uint32
		tupleType     uint8
		want          interface{}
		wantErr       bool
		errorContains string
	}{
		// Tuple Type Null
		{
			name:      "null tuple type",
			tupleType: pglogrepl.TupleDataTypeNull,
			want:      nil,
		},

		// Tuple Type Text (falls back to pgx decoder for some types)
		{
			name:      "text tuple type - string",
			data:      []byte("hello world"),
			dataType:  pgtype.TextOID,
			tupleType: pglogrepl.TupleDataTypeText,
			want:      "hello world",
		},
		{
			name:      "text tuple type - int",
			data:      []byte("123"),
			dataType:  pgtype.Int4OID,
			tupleType: pglogrepl.TupleDataTypeText,
			want:      int64(123),
		},

		// Standard types (binary representation as text)
		{
			name:     "bool true",
			data:     []byte("t"),
			dataType: pgtype.BoolOID,
			want:     true,
		},
		{
			name:     "bool false",
			data:     []byte("f"),
			dataType: pgtype.BoolOID,
			want:     false,
		},
		{
			name:     "integer",
			data:     []byte("12345"),
			dataType: pgtype.Int8OID,
			want:     int64(12345),
		},
		{
			name:     "float",
			data:     []byte("123.45"),
			dataType: pgtype.Float8OID,
			want:     123.45,
		},
		{
			name:     "float NULL",
			data:     []byte("NULL"),
			dataType: pgtype.Float8OID,
			want:     nil,
		},
		{
			name:     "numeric",
			data:     []byte("12345.6789"),
			dataType: pgtype.NumericOID,
			want:     "12345.6789",
		},
		{
			name:     "text",
			data:     []byte("some text"),
			dataType: pgtype.TextOID,
			want:     "some text",
		},
		{
			name:     "varchar",
			data:     []byte("some varchar"),
			dataType: pgtype.VarcharOID,
			want:     "some varchar",
		},
		{
			name:     "bytea hex format",
			data:     []byte("\\x68656c6c6f"), // "hello"
			dataType: pgtype.ByteaOID,
			want:     []byte("hello"),
		},
		{
			name:     "bytea literal",
			data:     []byte("hello"),
			dataType: pgtype.ByteaOID,
			want:     []byte("hello"),
		},
		{
			name:     "timestamp",
			data:     []byte("2023-01-01 12:30:00.123456"),
			dataType: pgtype.TimestampOID,
			want:     testTime,
		},
		{
			name:     "timestamptz",
			data:     []byte("2023-01-01 12:30:00.123456"),
			dataType: pgtype.TimestamptzOID,
			want:     testTime,
		},
		{
			name:     "date",
			data:     []byte("2023-01-01"),
			dataType: pgtype.DateOID,
			want:     testDate,
		},
		{
			name:     "json",
			data:     []byte(`{"key":"value"}`),
			dataType: pgtype.JSONOID,
			want:     `{"key":"value"}`,
		},
		{
			name:     "jsonb",
			data:     []byte(`{"key":"value"}`),
			dataType: pgtype.JSONBOID,
			want:     map[string]interface{}{"key": "value"},
		},
		{
			name:     "unknown type",
			data:     []byte("some data"),
			dataType: 99999, // Some OID that doesn't exist
			want:     "some data",
		},
		{
			name:      "empty data for text",
			data:      nil,
			dataType:  pgtype.TextOID,
			tupleType: pglogrepl.TupleDataTypeText,
			want:      []byte{},
		},
		{
			name:     "text array",
			data:     []byte(`{"hello","world"}`),
			dataType: pgtype.TextArrayOID,
			want:     []string{"hello", "world"},
		},
		{
			name:     "varchar array",
			data:     []byte(`{"hello","world"}`),
			dataType: pgtype.VarcharArrayOID,
			want:     []string{"hello", "world"},
		},
		{
			name:     "char array",
			data:     []byte(`["a","b"]`),
			dataType: pgtype.BPCharArrayOID,
			want:     []string{"a", "b"},
		},
		{
			name:     "qchar array",
			data:     []byte(`["a","b"]`),
			dataType: pgtype.QCharArrayOID,
			want:     []string{"a", "b"},
		},
		{
			name:     "int array",
			data:     []byte(`{1,2,3}`),
			dataType: pgtype.Int4ArrayOID,
			want:     []interface{}{int64(1), int64(2), int64(3)},
		},
		{
			name:     "int array with NULL",
			data:     []byte(`{1,NULL,3}`),
			dataType: pgtype.Int4ArrayOID,
			want:     []interface{}{int64(1), nil, int64(3)},
		},
		{
			name:     "float array",
			data:     []byte(`{1.1,2.2,3.3}`),
			dataType: pgtype.Float8ArrayOID,
			want:     []interface{}{1.1, 2.2, 3.3},
		},
		{
			name:     "bool array",
			data:     []byte(`{t,f,t}`),
			dataType: pgtype.BoolArrayOID,
			want:     []interface{}{true, false, true},
		},

		// Error cases
		{
			name:          "bytea invalid hex",
			data:          []byte("\\xinvalid"),
			dataType:      pgtype.ByteaOID,
			wantErr:       true,
			errorContains: "failed to decode bytea hex string",
		},
		{
			name:     "invalid bool",
			data:     []byte("not-a-bool"),
			dataType: pgtype.BoolOID,
			wantErr:  true,
		},
		{
			name:     "invalid int",
			data:     []byte("not-an-int"),
			dataType: pgtype.Int8OID,
			wantErr:  true,
		},
		{
			name:     "invalid float",
			data:     []byte("not-a-float"),
			dataType: pgtype.Float8OID,
			wantErr:  true,
		},
		{
			name:     "invalid timestamp",
			data:     []byte("not-a-timestamp"),
			dataType: pgtype.TimestampOID,
			wantErr:  true,
		},
		{
			name:     "invalid date",
			data:     []byte("not-a-date"),
			dataType: pgtype.DateOID,
			wantErr:  true,
		},
		{
			name:     "invalid jsonb",
			data:     []byte(`{key:"value"}`), // invalid json
			dataType: pgtype.JSONBOID,
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := utils.DecodeValue(tc.data, tc.dataType, tc.tupleType)

			if tc.wantErr {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}
