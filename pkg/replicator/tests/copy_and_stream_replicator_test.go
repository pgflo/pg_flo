package replicator_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
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
				*dest[0].(*string) = replicator.DefaultSchema
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
}

func TestCopyAndStreamReplicator_CopyOnly_vs_CopyAndStream_Modes(t *testing.T) {
	tests := []struct {
		name         string
		copyOnly     bool
		expectStream bool
	}{
		{
			name:         "Copy-only mode should not start streaming",
			copyOnly:     true,
			expectStream: false,
		},
		{
			name:         "Copy-and-stream mode should start streaming after copy",
			copyOnly:     false,
			expectStream: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockStandardConn := new(MockStandardConnection)
			mockNATSClient := new(MockNATSClient)

			config := replicator.Config{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				User:     "testuser",
				Password: "testpass",
				Group:    "testgroup",
				Schema:   "public",
				Tables:   []string{"test_table"},
			}

			if tt.expectStream {
				mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
			}

			setupMockShutdownBase(mockStandardConn, mockNATSClient, mockReplicationConn)

			baseReplicator := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)
			copyStreamReplicator := replicator.NewCopyAndStreamReplicator(baseReplicator, 2, tt.copyOnly)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := copyStreamReplicator.Start(ctx)

			if tt.copyOnly {
				assert.NoError(t, err, "Copy-only mode should complete successfully")
			} else {
				assert.Error(t, err, "Copy-and-stream mode should get error from StartReplication mock")
			}

			mockReplicationConn.AssertExpectations(t)
			mockStandardConn.AssertExpectations(t)
			mockNATSClient.AssertExpectations(t)
		})
	}
}

func setupMockShutdownBase(mockStandardConn *MockStandardConnection, _ *MockNATSClient, mockReplicationConn *MockReplicationConnection) {
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
		return strings.Contains(q, "table_info") && strings.Contains(q, "relreplident")
	}), mock.Anything).Return(mockPKRows, nil)

	mockStandardConn.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(mockPKRows, nil)

	mockPublicationRow := MockRow{
		scanFunc: func(dest ...interface{}) error {
			if b, ok := dest[0].(*bool); ok {
				*b = false
			}
			return nil
		},
	}
	mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_publication")
	}), mock.Anything).Return(mockPublicationRow)

	mockTablesRows := new(MockRows)
	mockTablesRows.On("Next").Return(false)
	mockTablesRows.On("Err").Return(nil)
	mockTablesRows.On("Close").Return()
	mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_tables")
	}), mock.Anything).Return(mockTablesRows, nil)

	mockStandardConn.On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "CREATE PUBLICATION")
	}), mock.Anything).Return(pgconn.CommandTag{}, nil)

	mockSlotRow := MockRow{
		scanFunc: func(dest ...interface{}) error {
			if b, ok := dest[0].(*bool); ok {
				*b = false
			}
			return nil
		},
	}
	mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_replication_slots")
	}), mock.Anything).Return(mockSlotRow)

	mockReplicationConn.On("CreateReplicationSlot", mock.Anything, mock.Anything).Return(
		pglogrepl.CreateReplicationSlotResult{
			SlotName:        "test_slot",
			ConsistentPoint: "0/123456",
			SnapshotName:    "test_snapshot",
			OutputPlugin:    "pgoutput",
		}, nil)

	mockSnapshotTx := new(MockTx)
	mockStandardConn.On("BeginTx", mock.Anything, mock.AnythingOfType("pgx.TxOptions")).Return(mockSnapshotTx, nil)

	mockSnapshotRow := MockRow{
		scanFunc: func(dest ...interface{}) error {
			if snapshotID, ok := dest[0].(*string); ok {
				*snapshotID = "test_snapshot_123"
			}
			if lsn, ok := dest[1].(*pglogrepl.LSN); ok {
				*lsn = pglogrepl.LSN(12345)
			}
			return nil
		},
	}
	mockSnapshotTx.On("QueryRow", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_export_snapshot")
	})).Return(mockSnapshotRow)

	mockRelPagesRow := MockRow{
		scanFunc: func(dest ...interface{}) error {
			if pages, ok := dest[0].(*uint32); ok {
				*pages = 1
			}
			return nil
		},
	}
	mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "SELECT relpages")
	}), mock.Anything).Return(mockRelPagesRow)

	mockPoolConn := new(MockPgxPoolConn)
	mockCopyTx := new(MockTx)

	mockStandardConn.On("Acquire", mock.Anything).Return(mockPoolConn, nil)
	mockPoolConn.On("BeginTx", mock.Anything, mock.AnythingOfType("pgx.TxOptions")).Return(mockCopyTx, nil)
	mockPoolConn.On("Release").Return()

	mockCopyTx.On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "SET TRANSACTION SNAPSHOT")
	}), mock.Anything).Return(pgconn.CommandTag{}, nil)

	mockSchemaRow := MockRow{
		scanFunc: func(dest ...interface{}) error {
			if schema, ok := dest[0].(*string); ok {
				*schema = "public"
			}
			return nil
		},
	}
	mockCopyTx.On("QueryRow", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "SELECT schemaname")
	}), mock.Anything).Return(mockSchemaRow)

	mockCopyRows := new(MockRows)
	mockCopyRows.On("Next").Return(false)
	mockCopyRows.On("Err").Return(nil)
	mockCopyRows.On("Close").Return()
	mockCopyRows.On("FieldDescriptions").Return([]pgconn.FieldDescription{})

	mockCopyTx.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "ctid >=")
	}), mock.Anything).Return(mockCopyRows, nil)

	mockSnapshotTx.On("Commit", mock.Anything).Return(nil)
	mockCopyTx.On("Commit", mock.Anything).Return(nil)
}
