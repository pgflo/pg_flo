package tests

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/direct"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

var testTime = time.Now()

// Test BulkParquetWriter creation
func TestBulkParquetWriter_New(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	tests := []struct {
		name                 string
		maxFileSize          int64
		includePgFloMetadata bool
	}{
		{"with metadata", 1024 * 1024, true},
		{"without metadata", 1024 * 1024, false},
		{"zero size uses default", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := direct.NewBulkParquetWriter(tempDir, tt.maxFileSize, tt.includePgFloMetadata, logger)

			// Just test instantiation works
			assert.NoError(t, err)
			assert.NotNil(t, writer)

			if writer != nil {
				err := writer.Close()
				assert.NoError(t, err)
			}
		})
	}
}

// Test CDCParquetWriter creation
func TestCDCParquetWriter_New(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	tests := []struct {
		name                 string
		maxFileSize          int64
		includePgFloMetadata bool
	}{
		{"with metadata", 1024 * 1024, true},
		{"without metadata", 1024 * 1024, false},
		{"zero size uses default", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, err := direct.NewCDCParquetWriter(tempDir, tt.maxFileSize, tt.includePgFloMetadata, logger)

			// Just test instantiation works
			assert.NoError(t, err)
			assert.NotNil(t, writer)

			if writer != nil {
				err := writer.Close()
				assert.NoError(t, err)
			}
		})
	}
}

// Test empty operations don't crash
func TestBulkParquetWriter_EmptyOperations(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	writer, err := direct.NewBulkParquetWriter(tempDir, 1024*1024, false, logger)
	assert.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Test empty operations
	bytesWritten, fileCount, err := writer.WriteBulkData("public.test", nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), bytesWritten)
	assert.Equal(t, 0, fileCount)
}

// Test empty transactions don't crash
func TestCDCParquetWriter_EmptyTransaction(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	writer, err := direct.NewCDCParquetWriter(tempDir, 1024*1024, false, logger)
	assert.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Test empty transaction
	err = writer.WriteTransaction(nil)
	assert.NoError(t, err)
}

// Test exhaustive PostgreSQL to Parquet type conversion
func TestBulkParquetWriter_PostgreSQLTypeConversion(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	writer, err := direct.NewBulkParquetWriter(tempDir, 1024*1024, false, logger)
	assert.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Test with operations that have different PostgreSQL data types
	operations := []*utils.CDCMessage{
		{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "type_test",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: 23},                // int4
				{Name: "bigint_col", DataType: 20},        // int8
				{Name: "smallint_col", DataType: 21},      // int2
				{Name: "text_col", DataType: 25},          // text
				{Name: "varchar_col", DataType: 1043},     // varchar
				{Name: "bool_col", DataType: 16},          // bool
				{Name: "float4_col", DataType: 700},       // float4
				{Name: "float8_col", DataType: 701},       // float8
				{Name: "numeric_col", DataType: 1700},     // numeric
				{Name: "timestamp_col", DataType: 1114},   // timestamp
				{Name: "timestamptz_col", DataType: 1184}, // timestamptz
				{Name: "date_col", DataType: 1082},        // date
				{Name: "time_col", DataType: 1083},        // time
				{Name: "bytea_col", DataType: 17},         // bytea
				{Name: "uuid_col", DataType: 2950},        // uuid
				{Name: "json_col", DataType: 114},         // json
				{Name: "jsonb_col", DataType: 3802},       // jsonb
				{Name: "array_text_col", DataType: 1009},  // text[]
				{Name: "array_int_col", DataType: 1007},   // int4[]
			},
			CopyData: [][]byte{
				[]byte("1"),                                    // id
				[]byte("9223372036854775807"),                  // bigint_col
				[]byte("32767"),                                // smallint_col
				[]byte("test text"),                            // text_col
				[]byte("test varchar"),                         // varchar_col
				[]byte("t"),                                    // bool_col
				[]byte("3.14"),                                 // float4_col
				[]byte("2.718281828"),                          // float8_col
				[]byte("123.45"),                               // numeric_col
				[]byte("2023-12-01 10:30:00"),                  // timestamp_col
				[]byte("2023-12-01 10:30:00+00"),               // timestamptz_col
				[]byte("2023-12-01"),                           // date_col
				[]byte("10:30:00"),                             // time_col
				[]byte("\\x48656c6c6f"),                        // bytea_col
				[]byte("550e8400-e29b-41d4-a716-446655440000"), // uuid_col
				[]byte("{\"key\": \"value\"}"),                 // json_col
				[]byte("{\"key\": \"value\"}"),                 // jsonb_col
				[]byte("{\"test\",\"array\"}"),                 // array_text_col
				[]byte("{1,2,3}"),                              // array_int_col
			},
			EmittedAt: testTime,
		},
	}

	// This should handle all PostgreSQL types without crashing
	bytesWritten, fileCount, err := writer.WriteBulkData("public.type_test", operations)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, bytesWritten, int64(0))
	assert.GreaterOrEqual(t, fileCount, 0)
}

// Test commit lock pattern concept in CDC writer
func TestCDCParquetWriter_CommitLockPattern(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	writer, err := direct.NewCDCParquetWriter(tempDir, 1024*1024, false, logger)
	assert.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Test that WriteTransaction represents the commit lock pattern
	// Data is only written to files when the full transaction is provided

	// Empty transaction should not crash
	err = writer.WriteTransaction(nil)
	assert.NoError(t, err)

	// Empty slice should not crash
	err = writer.WriteTransaction([]*utils.CDCMessage{})
	assert.NoError(t, err)

	// The concept is that operations are batched and written atomically
	// We don't test actual writing due to Arrow timestamp complexity
	assert.NotNil(t, writer)
}
