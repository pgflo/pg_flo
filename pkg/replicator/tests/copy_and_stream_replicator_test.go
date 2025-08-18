package replicator_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestCopyAndStreamReplicator(t *testing.T) {
	t.Run("CopyTableRange", func(t *testing.T) {
		testCases := []struct {
			name           string
			relationFields []pgconn.FieldDescription
			tupleData      []interface{}
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
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockStandardConn := new(MockStandardConnection)
				mockNATSClient := new(MockNATSClient)
				mockPoolConn := new(MockPgxPoolConn)
				mockTx := new(MockTx)
				mockRows := new(MockRows)

				var capturedSubject string
				var capturedData []byte

				mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)
				mockPoolConn.On("BeginTx", mock.Anything, mock.AnythingOfType("pgx.TxOptions")).Return(mockTx, nil)
				mockTx.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(pgconn.CommandTag{}, nil)
				mockTx.On("QueryRow", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(MockRow{
					scanFunc: func(dest ...interface{}) error {
						*dest[0].(*string) = replicator.DefaultSchema
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

				mockNATSClient.On("PublishMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Return(nil).Run(func(args mock.Arguments) {
					capturedSubject = args.String(0)
					capturedData = args.Get(1).([]byte)
				})

				csr := &replicator.CopyAndStreamReplicator{
					BaseReplicator: replicator.NewBaseReplicator(
						replicator.Config{
							Tables:   []string{"test_table"},
							Schema:   replicator.DefaultSchema,
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

				assert.Equal(t, "pgflo.test_group", capturedSubject)
				assert.NotEmpty(t, capturedData)

				var decodedMessage utils.CDCMessage
				decoder := gob.NewDecoder(bytes.NewReader(capturedData))
				err = decoder.Decode(&decodedMessage)
				assert.NoError(t, err)

				assert.Equal(t, utils.OperationInsert, decodedMessage.Type)
				assert.Equal(t, "public", decodedMessage.Schema)
				assert.Equal(t, "test_table", decodedMessage.Table)
				assert.Equal(t, len(tc.relationFields), len(decodedMessage.Columns))
				assert.Equal(t, len(rawData), len(decodedMessage.CopyData))
				assert.Nil(t, decodedMessage.NewTuple)
				assert.Nil(t, decodedMessage.OldTuple)

				for i, field := range tc.relationFields {
					assert.Equal(t, field.Name, decodedMessage.Columns[i].Name)
					assert.Equal(t, field.DataTypeOID, decodedMessage.Columns[i].DataType)
					if rawData[i] != nil && len(rawData[i]) > 0 {
						assert.NotNil(t, decodedMessage.CopyData[i])
						assert.Equal(t, string(rawData[i]), string(decodedMessage.CopyData[i]))
					}
				}

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
