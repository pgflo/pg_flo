package direct

import (
	"context"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// DirectReplicator defines the interface for direct sink replication
type DirectReplicator interface { //nolint:revive // Clear namespacing needed
	Bootstrap(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// MetadataStore defines the interface for PostgreSQL metadata storage
type MetadataStore interface {
	Connect(ctx context.Context) error
	Close() error
	EnsureSchema(ctx context.Context) error

	// Streaming progress tracking (essential for resumability)
	SaveStreamingLSN(ctx context.Context, groupName string, lsn pglogrepl.LSN) error
	GetStreamingLSN(ctx context.Context, groupName string) (pglogrepl.LSN, error)
	UpdateStreamingLSN(ctx context.Context, groupName string, lsn pglogrepl.LSN) error
	UpdateStreamingLSNWithBatch(ctx context.Context, groupName string, lsn pglogrepl.LSN, batchID int64) error

	// Copy snapshot management (essential for copy/stream handoff)
	SaveCopySnapshot(ctx context.Context, groupName, snapshotName, snapshotLSN string) error
	GetCopySnapshot(ctx context.Context, groupName string) (string, string, error)

	// CTID-based copy progress tracking (essential for resumability)
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
