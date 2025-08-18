package utils_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"

	"github.com/pgflo/pg_flo/pkg/utils"
)

func TestPostgreSQLTypeConverter(t *testing.T) {
	converter := utils.NewPostgreSQLTypeConverter()

	t.Run("Basic types work correctly", func(t *testing.T) {
		testCases := []struct {
			name     string
			data     []byte
			dataType uint32
			want     interface{}
		}{
			{
				name:     "integer",
				data:     []byte("123"),
				dataType: pgtype.Int4OID,
				want:     int32(123),
			},
			{
				name:     "text",
				data:     []byte("hello world"),
				dataType: pgtype.TextOID,
				want:     "hello world",
			},
			{
				name:     "boolean true",
				data:     []byte("t"),
				dataType: pgtype.BoolOID,
				want:     true,
			},
			{
				name:     "boolean false",
				data:     []byte("f"),
				dataType: pgtype.BoolOID,
				want:     false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := converter.DecodePostgreSQLValue(tc.data, tc.dataType, 0)
				assert.NoError(t, err, "DecodePostgreSQLValue should not error")
				assert.Equal(t, tc.want, got, "Decoded value should match expected")
			})
		}
	})

	t.Run("Array types work correctly - the critical test", func(t *testing.T) {
		testCases := []struct {
			name     string
			data     []byte
			dataType uint32
			want     interface{}
		}{
			{
				name:     "simple text array",
				data:     []byte("{hello,world}"),
				dataType: pgtype.TextArrayOID,
				want:     "{hello,world}",
			},
			{
				name:     "text array with commas in values",
				data:     []byte(`{"hello world","test,value"}`),
				dataType: pgtype.TextArrayOID,
				want:     `{"hello world","test,value"}`,
			},
			{
				name:     "simple int array",
				data:     []byte("{1,2,3}"),
				dataType: pgtype.Int4ArrayOID,
				want:     "{1,2,3}",
			},
			{
				name:     "multidimensional int array - PRESERVED",
				data:     []byte("{{1,2},{3,4}}"),
				dataType: pgtype.Int4ArrayOID,
				want:     "{{1,2},{3,4}}",
			},
			{
				name:     "empty array",
				data:     []byte("{}"),
				dataType: pgtype.TextArrayOID,
				want:     "{}",
			},
			{
				name:     "array with nulls",
				data:     []byte("{a,NULL,c}"),
				dataType: pgtype.TextArrayOID,
				want:     "{a,NULL,c}",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				got, err := converter.DecodePostgreSQLValue(tc.data, tc.dataType, 0)
				assert.NoError(t, err, "DecodePostgreSQLValue should not error for %s", tc.name)

				if tc.want != nil {
					assert.Equal(t, tc.want, got, "Decoded value should match expected for %s", tc.name)
				} else {
					// For cases where we don't specify exact expected output, just ensure no error
					assert.NotNil(t, got, "Should decode to something for %s", tc.name)
				}
			})
		}
	})

	t.Run("DecodePostgreSQLValue works directly", func(t *testing.T) {
		// Test direct usage of DecodePostgreSQLValue - arrays now return as strings for data integrity
		got, err := utils.GlobalPostgreSQLTypeConverter.DecodePostgreSQLValue([]byte("{1,2,3}"), pgtype.Int4ArrayOID, 0)
		assert.NoError(t, err)
		assert.IsType(t, "", got, "Should return string for arrays to preserve structure")
		assert.Equal(t, "{1,2,3}", got, "Should preserve exact PostgreSQL array representation")
	})
}
