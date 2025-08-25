package direct

import (
	"context"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// DirectReplicator defines the interface for direct sink replication
type DirectReplicator interface {
	Bootstrap(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// MetadataStore defines the interface for PostgreSQL metadata storage
type MetadataStore interface {
	Connect(ctx context.Context) error
	Close() error
	EnsureSchema(ctx context.Context) error

	// Table management
	RegisterTables(ctx context.Context, groupName string, tables []TableInfo) error
	GetUnassignedTables(ctx context.Context, groupName string, maxTables int) ([]TableAssignment, error)
	AssignTables(ctx context.Context, workerID string, tableIDs []string) error
	UpdateHeartbeat(ctx context.Context, workerID string) error

	// Copy progress tracking
	SaveCopyProgress(ctx context.Context, assignmentID string, lastPage uint32, totalPages uint32) error
	GetCopyProgress(ctx context.Context, assignmentID string) (*CopyProgress, error)
	MarkCopyComplete(ctx context.Context, assignmentID string) error

	// Streaming progress
	SaveStreamingLSN(ctx context.Context, groupName string, lsn pglogrepl.LSN) error
	GetStreamingLSN(ctx context.Context, groupName string) (pglogrepl.LSN, error)
	GetLastLSN(ctx context.Context, assignmentID string) (pglogrepl.LSN, error)
	UpdateStreamingLSN(ctx context.Context, assignmentID string, lsn pglogrepl.LSN) error

	// S3 file tracking
	RecordS3File(ctx context.Context, filePath string, txID string, tableNames []string) error
	MarkS3FileProcessed(ctx context.Context, filePath string) error

	// CTID-based copy progress tracking
	SaveCopySnapshot(ctx context.Context, groupName, snapshotName, snapshotLSN string) error
	GetCopySnapshot(ctx context.Context, groupName string) (string, string, error)
	SaveCTIDCopyProgress(ctx context.Context, groupName, tableName, lastCTID string, bytesWritten int64, fileCount int, status string) error
	GetCTIDCopyProgress(ctx context.Context, groupName, tableName string) (string, int64, int, string, error)
	MarkCopyCompleted(ctx context.Context, groupName string) error
}

// TransactionStore defines the interface for transaction buffering and spilling
type TransactionStore interface {
	Store(key string, msg *utils.CDCMessage) error
	GetAllMessages() ([]*utils.CDCMessage, error)
	Clear() error
	GetMemoryUsage() int64
	Close() error
}

// OperationConsolidator defines the interface for key-based deduplication
type OperationConsolidator interface {
	ConsolidateTransaction(operations []*utils.CDCMessage) []*utils.CDCMessage
}

// ParquetWriter defines the interface for writing consolidated data to Parquet
type ParquetWriter interface {
	WriteTransaction(operations []*utils.CDCMessage) error
	Close() error
}
