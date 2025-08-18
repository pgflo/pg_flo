package replicator_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCopyAndStreamReplicator(t *testing.T) {

	t.Run("CopyTable", func(t *testing.T) {
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)
		mockPoolConn := new(MockPgxPoolConn)
		mockTx := new(MockTx)
		mockRows := new(MockRows)

		mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)
		mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(query string) bool {
			return strings.Contains(query, "SELECT relpages")
		}), mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*uint32) = 201
				return nil
			},
		})

		mockOIDRows := new(MockRows)
		mockOIDRows.On("Next").Return(false)
		mockOIDRows.On("Err").Return(nil)
		mockOIDRows.On("Close").Return()

		mockPKRows := new(MockRows)
		mockPKRows.On("Next").Return(false)
		mockPKRows.On("Err").Return(nil)
		mockPKRows.On("Close").Return()

		mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
			return strings.Contains(q, "pg_type")
		}), mock.Anything).Return(mockOIDRows, nil)

		mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
			return strings.Contains(q, "table_info")
		}), mock.Anything).Return(mockPKRows, nil)

		mockPoolConn.On("BeginTx", mock.Anything, mock.MatchedBy(func(txOptions pgx.TxOptions) bool {
			return txOptions.IsoLevel == pgx.Serializable && txOptions.AccessMode == pgx.ReadOnly
		})).Return(mockTx, nil)

		mockTx.On("QueryRow", mock.Anything, "SELECT schemaname FROM pg_tables WHERE tablename = $1", mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*string) = "public"
				return nil
			},
		})

		mockTx.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(pgconn.CommandTag{}, nil)

		mockRows.On("Next").Return(true).Once().On("Next").Return(false)
		mockRows.On("Err").Return(nil)
		mockRows.On("Close").Return()
		mockRows.On("FieldDescriptions").Return([]pgconn.FieldDescription{
			{Name: "id", DataTypeOID: 23},
			{Name: "name", DataTypeOID: 25},
		})
		mockRows.On("RawValues").Return([][]byte{[]byte("1"), []byte("John Doe")})

		mockTx.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(mockRows, nil)
		mockTx.On("Commit", mock.Anything).Return(nil)
		mockPoolConn.On("Release").Return()

		mockNATSClient.On("PublishMessage", "pgflo.test_group", mock.AnythingOfType("[]uint8")).Return(nil)
		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.NewBaseReplicator(
				replicator.Config{
					Tables:   []string{"users"},
					Schema:   "public",
					Host:     "localhost",
					Port:     5432,
					User:     "testuser",
					Password: "testpassword",
					Database: "testdb",
					Group:    "test_group",
				},
				nil,
				mockStandardConn,
				mockNATSClient,
			),
			MaxCopyWorkersPerTable: 2,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := csr.CopyTable(ctx, "users", "snapshot-1")
		assert.NoError(t, err)

		mockStandardConn.AssertExpectations(t)
		mockPoolConn.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockRows.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
		mockOIDRows.AssertExpectations(t)
		mockPKRows.AssertExpectations(t)
	})

	t.Run("CopyTableRange", func(t *testing.T) {
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)
		mockPoolConn := new(MockPgxPoolConn)
		mockTx := new(MockTx)
		mockRows := new(MockRows)

		mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)

		mockPoolConn.On("BeginTx", mock.Anything, mock.MatchedBy(func(txOptions pgx.TxOptions) bool {
			return txOptions.IsoLevel == pgx.Serializable && txOptions.AccessMode == pgx.ReadOnly
		})).Return(mockTx, nil)

		mockTx.On("Exec", mock.Anything, mock.MatchedBy(func(sql string) bool {
			return strings.Contains(sql, "SET TRANSACTION SNAPSHOT")
		}), mock.Anything).Return(pgconn.CommandTag{}, nil).Once()

		mockTx.On("QueryRow", mock.Anything, "SELECT schemaname FROM pg_tables WHERE tablename = $1", mock.Anything).Return(MockRow{
			scanFunc: func(dest ...interface{}) error {
				*dest[0].(*string) = "public"
				return nil
			},
		})

		mockOIDRows := new(MockRows)
		mockOIDRows.On("Next").Return(false)
		mockOIDRows.On("Err").Return(nil)
		mockOIDRows.On("Close").Return()

		mockPKRows := new(MockRows)
		mockPKRows.On("Next").Return(false)
		mockPKRows.On("Err").Return(nil)
		mockPKRows.On("Close").Return()

		mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
			return strings.Contains(q, "pg_type")
		}), mock.Anything).Return(mockOIDRows, nil)

		mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
			return strings.Contains(q, "table_info")
		}), mock.Anything).Return(mockPKRows, nil)

		mockRows.On("Next").Return(true).Once().On("Next").Return(false)
		mockRows.On("Err").Return(nil)
		mockRows.On("Close").Return()
		mockRows.On("FieldDescriptions").Return([]pgconn.FieldDescription{
			{Name: "id", DataTypeOID: 23},
			{Name: "name", DataTypeOID: 25},
		})
		mockRows.On("RawValues").Return([][]byte{[]byte("1"), []byte("John Doe")})

		mockTx.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(mockRows, nil)
		mockTx.On("Commit", mock.Anything).Return(nil)
		mockPoolConn.On("Release").Return()

		mockNATSClient.On("PublishMessage", "pgflo.test_group", mock.Anything).Return(nil)

		csr := &replicator.CopyAndStreamReplicator{
			BaseReplicator: replicator.NewBaseReplicator(
				replicator.Config{
					Tables:   []string{"users"},
					Schema:   "public",
					Host:     "localhost",
					Port:     5432,
					User:     "testuser",
					Password: "testpassword",
					Database: "testdb",
					Group:    "test_group",
				},
				nil,
				mockStandardConn,
				mockNATSClient,
			),
		}

		rowsCopied, err := csr.CopyTableRange(context.Background(), "users", 0, 1000, "snapshot-1", 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rowsCopied)

		mockStandardConn.AssertExpectations(t)
		mockPoolConn.AssertExpectations(t)
		mockTx.AssertExpectations(t)
		mockRows.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
		mockOIDRows.AssertExpectations(t)
		mockPKRows.AssertExpectations(t)
	})
	t.Run("CopyTableRange with diverse data types", func(t *testing.T) {
		type expected struct {
			name  string
			value string
			oid   string
			tuple string
		}
		testCases := []struct {
			name           string
			relationFields []pgconn.FieldDescription
			tupleData      []interface{}
			expected       []map[string]interface{}
		}{
			{
				name: "Basic types",
				relationFields: []pgconn.FieldDescription{
					{Name: "id", DataTypeOID: pgtype.Int4OID},
					{Name: "name", DataTypeOID: pgtype.TextOID},
					{Name: "coach", DataTypeOID: pgtype.TextOID},
					{Name: "active", DataTypeOID: pgtype.BoolOID},
					{Name: "score", DataTypeOID: pgtype.Float8OID},
				},
				tupleData: []interface{}{
					int64(1), "John Doe", "", true, float64(9.99),
				},
				expected: []map[string]interface{}{
					{"name": "id", "type": "int4", "tupleType": pglogrepl.TupleDataTypeBinary, "value": int64(1)},
					{"name": "name", "type": "text", "tupleType": pglogrepl.TupleDataTypeText, "value": "John Doe"},
					{"name": "coach", "type": "text", "tupleType": pglogrepl.TupleDataTypeText, "value": ""},
					{"name": "active", "type": "bool", "tupleType": pglogrepl.TupleDataTypeBinary, "value": true},
					{"name": "score", "type": "float8", "tupleType": pglogrepl.TupleDataTypeBinary, "value": float64(9.99)},
				},
			},
			{
				name: "Complex types",
				relationFields: []pgconn.FieldDescription{
					{Name: "data", DataTypeOID: pgtype.JSONBOID},
					{Name: "tags", DataTypeOID: pgtype.TextArrayOID},
					{Name: "image", DataTypeOID: pgtype.ByteaOID},
					{Name: "created_at", DataTypeOID: pgtype.TimestamptzOID},
				},
				tupleData: []interface{}{
					[]byte(`{"key": "value"}`),
					[]string{"tag1", "tag2", "tag3"},
					[]byte{0x01, 0x02, 0x03, 0x04},
					time.Date(2023, time.May, 1, 12, 34, 56, 789000000, time.UTC),
				},
				expected: []map[string]interface{}{
					{"name": "data", "type": "jsonb", "tupleType": pglogrepl.TupleDataTypeBinary, "value": json.RawMessage(`{"key": "value"}`)},
					{"name": "tags", "type": "text[]", "tupleType": pglogrepl.TupleDataTypeBinary, "value": "{tag1,tag2,tag3}"},
					{"name": "image", "type": "bytea", "tupleType": pglogrepl.TupleDataTypeText, "value": []byte{0x01, 0x02, 0x03, 0x04}},
					{"name": "created_at", "type": "timestamptz", "tupleType": pglogrepl.TupleDataTypeBinary, "value": time.Date(2023, time.May, 1, 12, 34, 56, 789000000, time.UTC)},
				},
			},
			{
				name: "Numeric types",
				relationFields: []pgconn.FieldDescription{
					{Name: "small_int", DataTypeOID: pgtype.Int2OID},
					{Name: "big_int", DataTypeOID: pgtype.Int8OID},
					{Name: "numeric", DataTypeOID: pgtype.NumericOID},
				},
				tupleData: []interface{}{
					int64(32767), int64(9223372036854775807), "123456.789",
				},
				expected: []map[string]interface{}{
					{"name": "small_int", "type": "int2", "tupleType": pglogrepl.TupleDataTypeBinary, "value": int64(32767)},
					{"name": "big_int", "type": "int8", "tupleType": pglogrepl.TupleDataTypeBinary, "value": int64(9223372036854775807)},
					{"name": "numeric", "type": "numeric", "tupleType": pglogrepl.TupleDataTypeBinary, "value": "123456.789"},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockStandardConn := new(MockStandardConnection)
				mockNATSClient := new(MockNATSClient)
				mockPoolConn := new(MockPgxPoolConn)
				mockTx := new(MockTx)
				mockRows := new(MockRows)

				mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)
				mockPoolConn.On("BeginTx", mock.Anything, mock.AnythingOfType("pgx.TxOptions")).Return(mockTx, nil)
				mockTx.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(pgconn.CommandTag{}, nil)
				mockTx.On("QueryRow", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*string) = "public"
						return nil
					},
				})

				mockOIDRows := new(MockRows)
				mockOIDRows.On("Next").Return(false)
				mockOIDRows.On("Err").Return(nil)
				mockOIDRows.On("Close").Return()

				mockPKRows := new(MockRows)
				mockPKRows.On("Next").Return(false)
				mockPKRows.On("Err").Return(nil)
				mockPKRows.On("Close").Return()

				mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
					return strings.Contains(q, "pg_type")
				}), mock.Anything).Return(mockOIDRows, nil)

				mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
					return strings.Contains(q, "table_info")
				}), mock.Anything).Return(mockPKRows, nil)

				mockRows.On("Next").Return(true).Once().On("Next").Return(false)
				mockRows.On("Err").Return(nil)
				mockRows.On("Close").Return()
				mockRows.On("FieldDescriptions").Return(tc.relationFields)
				// Convert tupleData to raw bytes for RawValues()
				rawData := make([][]byte, len(tc.tupleData))
				for i, val := range tc.tupleData {
					switch v := val.(type) {
					case []byte:
						rawData[i] = v
					case string:
						rawData[i] = []byte(v)
					default:
						rawData[i] = []byte(fmt.Sprintf("%v", v))
					}
				}
				mockRows.On("RawValues").Return(rawData)

				mockTx.On("Query", mock.Anything, mock.AnythingOfType("string"), mock.Anything, mock.Anything).Return(mockRows, nil)
				mockTx.On("Commit", mock.Anything).Return(nil)
				mockPoolConn.On("Release").Return()

				mockNATSClient.On("PublishMessage", "pgflo.test_group", mock.AnythingOfType("[]uint8")).Return(nil)
				csr := &replicator.CopyAndStreamReplicator{
					BaseReplicator: replicator.NewBaseReplicator(
						replicator.Config{
							Tables:   []string{"test_table"},
							Schema:   "public",
							Host:     "localhost",
							Port:     5432,
							User:     "testuser",
							Password: "testpassword",
							Database: "testdb",
							Group:    "test_group",
						},
						nil,
						mockStandardConn,
						mockNATSClient,
					),
				}

				rowsCopied, err := csr.CopyTableRange(context.Background(), "test_table", 0, 1000, "snapshot-1", 0)
				assert.NoError(t, err)
				assert.Equal(t, int64(1), rowsCopied)

				// Assert expectations
				mockStandardConn.AssertExpectations(t)
				mockPoolConn.AssertExpectations(t)
				mockTx.AssertExpectations(t)
				mockRows.AssertExpectations(t)
				mockNATSClient.AssertExpectations(t)
				mockOIDRows.AssertExpectations(t)
				mockPKRows.AssertExpectations(t)
			})
		}
	})

}
