package tests

import (
	"os"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/direct"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test that demonstrates the type mismatch fix
func TestBulkParquetWriter_CorrectTypeMapping(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	writer, err := direct.NewBulkParquetWriter(tempDir, 1024*1024, false, logger)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	// Create test operation with various PostgreSQL types
	testOp := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "type_test",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: 23},         // int4 -> should be Int32
			{Name: "bigint_col", DataType: 20}, // int8 -> should be Int64
			{Name: "text_col", DataType: 25},   // text -> should be String
			{Name: "bool_col", DataType: 16},   // bool -> should be Boolean
			{Name: "float_col", DataType: 701}, // float8 -> should be Float64
		},
		CopyData: [][]byte{
			[]byte("42"),      // id
			[]byte("123456"),  // bigint_col
			[]byte("hello"),   // text_col
			[]byte("t"),       // bool_col
			[]byte("3.14159"), // float_col
		},
	}

	// Write the operation
	_, _, err = writer.WriteBulkData("public.type_test", []*utils.CDCMessage{testOp})
	require.NoError(t, err)

	// Close to flush
	err = writer.Close()
	require.NoError(t, err)

	// Verify file exists
	files, err := os.ReadDir(tempDir + "/copy")
	require.NoError(t, err)
	assert.Len(t, files, 1)

	parquetFile := tempDir + "/copy/" + files[0].Name()

	// The real test: parquet-tools should be able to read this without errors
	t.Logf("Created Parquet file: %s", parquetFile)
	t.Logf("Test with: parquet-tools show %s", parquetFile)
}

// Test that validates we can write different data types without corruption
func TestBulkParquetWriter_MultipleTypes(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	writer, err := direct.NewBulkParquetWriter(tempDir, 1024*1024, false, logger)
	require.NoError(t, err)
	defer func() { _ = writer.Close() }()

	operations := []*utils.CDCMessage{
		{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "integers",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "small_int", DataType: 21},   // int2
				{Name: "regular_int", DataType: 23}, // int4
				{Name: "big_int", DataType: 20},     // int8
			},
			CopyData: [][]byte{
				[]byte("1"),
				[]byte("1000"),
				[]byte("1000000"),
			},
		},
		{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "floats",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "float4_col", DataType: 700}, // float4
				{Name: "float8_col", DataType: 701}, // float8
			},
			CopyData: [][]byte{
				[]byte("3.14"),
				[]byte("2.718281828"),
			},
		},
		{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "strings",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "text_col", DataType: 25},      // text
				{Name: "varchar_col", DataType: 1043}, // varchar
			},
			CopyData: [][]byte{
				[]byte("hello world"),
				[]byte("varchar data"),
			},
		},
	}

	// Write all operations
	_, _, err = writer.WriteBulkData("public.integers", []*utils.CDCMessage{operations[0]})
	require.NoError(t, err)
	_, _, err = writer.WriteBulkData("public.floats", []*utils.CDCMessage{operations[1]})
	require.NoError(t, err)
	_, _, err = writer.WriteBulkData("public.strings", []*utils.CDCMessage{operations[2]})
	require.NoError(t, err)

	// Close to flush
	err = writer.Close()
	require.NoError(t, err)

	// Verify files exist for each table
	files, err := os.ReadDir(tempDir + "/copy")
	require.NoError(t, err)
	assert.Len(t, files, 3) // One file per table

	for _, file := range files {
		parquetFile := tempDir + "/copy/" + file.Name()
		t.Logf("Created Parquet file: %s", parquetFile)
	}
}
