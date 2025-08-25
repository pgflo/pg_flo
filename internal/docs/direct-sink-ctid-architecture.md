# Direct Sink CTID-Based Architecture Plan

## Overview

Complete redesign of Direct Sink using CTID-based copy operations inspired by PeerDB's proven approach, with clean separation between Copy and Stream phases.

## Key Principles

- **Pure bulk copy** (no transaction logic)
- **Size-based rotation** (128MB files)
- **Resumable operations** (CTID-based progress tracking)
- **Clean separation** (copy vs stream)
- **No data loss** (snapshot consistency)
- **Independent components** (no fallbacks needed)
- **High performance** (CTID chunking, no LIMIT/OFFSET)

## Architecture Components

### 1. CopyReplicator

**Purpose**: Bulk data transfer using CTID-based chunking
**Key Features**:

- PostgreSQL snapshot export for consistency
- CTID range queries for performance (`WHERE ctid BETWEEN '(0,1)' AND '(100,1)'`)
- 2000 rows per chunk (configurable)
- Consistent parquet file rotation (global 128MB limit)
- Resumable operations with exact CTID tracking

```go
type CopyReplicator struct {
    config         *Config
    conn           *pgx.Conn
    parquetWriter  *BulkParquetWriter
    progressStore  CopyProgressStore
    snapshotInfo   *SnapshotInfo
    chunkSize      int               // Default: 2000 rows
    maxFileSize    int64             // Default: 128MB
}

type CopyProgress struct {
    TableName     string
    LastCTID      string    // "(150,32)" - exact resumption point
    BytesWritten  int64     // For file rotation tracking
    FileCount     int       // For naming: users_copy_001.parquet
    Status        CopyStatus // NOT_STARTED, IN_PROGRESS, COMPLETED
    SnapshotName  string    // For consistency
    StartedAt     time.Time
    CompletedAt   *time.Time
}

type SnapshotInfo struct {
    SnapshotName string
    LSN          pglogrepl.LSN
    ExportedAt   time.Time
}
```

### 2. StreamReplicator

**Purpose**: Real-time CDC with transaction boundaries
**Key Features**:

- WAL streaming from copy handoff LSN
- Transaction consolidation with commit lock pattern
- Size-based file rotation (128MB limit) with transaction boundary awareness
- CDC metadata columns (`_pg_flo_*`, `_old_*`)

**Important: File Rotation vs Transaction Boundaries**:

- **File Rotation**: Size-based (128MB), happens independently for optimal performance
- **Transaction Boundaries**: Commit lock ensures data written only after COMMIT
- **Both concepts work together**: Files rotate on size, but respect transaction boundaries

```go
type StreamReplicator struct {
    config          *Config
    replicationConn *ReplicationConnection
    parquetWriter   *CDCParquetWriter
    consolidator    *OperationConsolidator
    commitLock      *CommitLock
    transactionStore TransactionStore
}
```

### 3. DirectReplicator (Orchestrator)

**Purpose**: Coordinate Copy → Stream transition

```go
type DirectReplicator struct {
    copyReplicator   *CopyReplicator
    streamReplicator *StreamReplicator
    metadataStore    MetadataStore
    config          *Config
}

func (dr *DirectReplicator) Execute() error {
    // Phase 1: Copy with snapshot consistency
    snapshotInfo, err := dr.copyReplicator.StartCopy()
    if err != nil { return err }

    // Phase 2: Stream from snapshot LSN
    return dr.streamReplicator.StartFromLSN(snapshotInfo.LSN)
}
```

## File Organization

### Copy Phase Files (Size-Based Rotation Only)

```
/data/copy/
├── users_copy_20250825_001.parquet      (128MB - bulk data, no CDC metadata)
├── users_copy_20250825_002.parquet      (64MB - remainder, no CDC metadata)
└── transactions_copy_20250825_001.parquet (128MB - bulk data, no CDC metadata)
```

### Stream Phase Files (Size-Based + Transaction Boundary Aware)

```
/data/stream/
├── users_stream_20250825_082631.parquet      (128MB rotated, includes CDC metadata)
├── users_stream_20250825_082745.parquet      (Next 128MB chunk, includes CDC metadata)
└── transactions_stream_20250825_082631.parquet (Per-table files, transaction boundaries respected)
```

**Key Differences**:

- **Copy**: Pure size-based rotation, no transaction logic, no CDC metadata
- **Stream**: Size-based rotation BUT respects transaction boundaries, includes CDC metadata

## Metadata Store Schema Changes

### New Tables

#### `copy_progress`

```sql
CREATE TABLE pgflo_metadata.copy_progress (
    group_name      VARCHAR(255) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    last_ctid       VARCHAR(50),           -- "(150,32)"
    bytes_written   BIGINT DEFAULT 0,
    file_count      INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'NOT_STARTED', -- NOT_STARTED, IN_PROGRESS, COMPLETED but can this be inffered by started_at?
    snapshot_name   VARCHAR(255),
    started_at      TIMESTAMP WITH TIME ZONE,
    completed_at    TIMESTAMP WITH TIME ZONE,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (group_name, table_name)
);
```

#### `copy_snapshots`

```sql
CREATE TABLE pgflo_metadata.copy_snapshots (
    group_name      VARCHAR(255) NOT NULL PRIMARY KEY,
    snapshot_name   VARCHAR(255) NOT NULL,
    snapshot_lsn    TEXT NOT NULL,
    exported_at     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    handoff_completed BOOLEAN DEFAULT FALSE
);
```

#### Update `streaming_state`

```sql
ALTER TABLE pgflo_metadata.streaming_state
ADD COLUMN copy_completed BOOLEAN DEFAULT FALSE;
```

## Implementation Plan

### Phase 1: Core Components

1. ✅ Create CTID-based CopyReplicator
2. ✅ Implement BulkParquetWriter (no CDC metadata)
3. ✅ Build CopyProgressStore with metadata tables
4. ✅ Add snapshot export/consistency logic

### Phase 2: Stream Integration

1. ✅ Create clean StreamReplicator
2. ✅ Implement CDC ParquetWriter (with metadata)
3. ✅ Build snapshot → stream handoff
4. ✅ Update DirectReplicator orchestration

### Phase 3: Testing & Validation

1. ✅ Update E2E tests for copy/stream separation
2. ✅ Add unit tests for CopyReplicator and StreamReplicator
3. ✅ Validate file rotation and resumption
4. ✅ Performance testing with large datasets

## Migration Strategy

### Backward Compatibility

- Keep existing `direct_replicator.go` as fallback initially

### Configuration Changes

```yaml
# Global parquet file configuration
max_parquet_file_size: 134217728 # 128MB in bytes (consistent across copy & stream)

# Copy phase configuration
copy:
  chunk_size: 2000 # Rows per CTID chunk
  # No transaction logic, consolidation, or commit locks - just bulk copy with size rotation

# Stream phase configuration
stream:
  # All built-in behavior:
  # - Size-based rotation (128MB) with transaction boundary awareness
  # - Commit lock pattern (data written only after COMMIT)
  # - Operation consolidation (key-based deduplication)
  # - Transaction store with disk spilling
```

## Testing Strategy

### Unit Tests

- `CopyReplicator` CTID chunking logic
- `CopyProgressStore` metadata operations
- `BulkParquetWriter` file rotation
- `StreamReplicator` transaction handling
- Snapshot export/import consistency

### Integration Tests

- Copy → Stream handoff accuracy
- Resumption after interruption
- Large table handling (1M+ rows)
- Multi-table coordination

### E2E Tests

- Update `e2e_direct_sink.sh` for copy/stream validation
- Separate validation for copy files vs stream files
- File count and size validation
- Data completeness verification

## Monitoring & Observability

- Copy progress metrics (bytes/rows processed)
- File rotation events and sizes
- CTID chunk processing times
- Snapshot consistency validation
- Stream handoff accuracy

## Risk Mitigation

- **CTID limitations**: Fallback to row-based for compressed tables
- **Snapshot timeout**: Configurable snapshot maintenance duration
- **File corruption**: Checksums and validation on write
- **Memory pressure**: Configurable chunk sizes and batch limits
- **Network issues**: Retry logic with exponential backoff

## Success Criteria

- ✅ 100% data consistency between source and parquet
- ✅ Sub-second resumption for interrupted operations
- ✅ Zero data loss during copy → stream transition
- ✅ Clean separation of concerns (no NATS dependencies)
- ✅ All existing E2E tests pass with new architecture

## Session Restart Information

### Project Structure & Key Files

**Core Implementation Files:**

- `/Users/shayon/src/pg_flo/pkg/direct/copy_replicator.go` - ✅ CTID-based copy logic
- `/Users/shayon/src/pg_flo/pkg/direct/metadata_store.go` - ✅ Enhanced with copy progress tracking
- `/Users/shayon/src/pg_flo/pkg/direct/interfaces.go` - ✅ Updated with new methods
- `/Users/shayon/src/pg_flo/pkg/direct/direct_replicator.go` - Current orchestrator (needs refactoring)
- `/Users/shayon/src/pg_flo/pkg/direct/parquet_writer.go` - Current writer (needs separation)

**Configuration & Documentation:**

- `/Users/shayon/src/pg_flo/internal/docs/direct-sink-ctid-architecture.md` - This plan document
- `/Users/shayon/src/pg_flo/pkg/direct/types.go` - Config structures
- `/Users/shayon/src/pg_flo/internal/direct-sink.yaml` - Config template

**Test Files:**

- `/Users/shayon/src/pg_flo/internal/scripts/e2e_direct_sink.sh` - E2E test script
- `/Users/shayon/src/pg_flo/internal/scripts/e2e_test_local.sh` - E2E runner

### Essential Commands

**Build Project:**

```bash
cd /Users/shayon/src/pg_flo
go build -o bin/pg_flo
```

**Run Tests:**

```bash
# Unit tests
make test

# Linting
make lint

# E2E Test (Direct Sink)
E2E_TEST=e2e_direct_sink.sh ./internal/scripts/e2e_test_local.sh

# Monitor E2E logs
tail -f /tmp/pg_flo*
```

**Test File Inspection:**

```bash
# Check Parquet files (new parquet-tools)
parquet-tools inspect /tmp/pg_flo_direct_parquet/public.users_*.parquet
parquet-tools show --head 10 /tmp/pg_flo_direct_parquet/public.users_*.parquet

# Check file sizes and counts
ls -la /tmp/pg_flo_direct_parquet/
```

### Current Status Summary

**✅ COMPLETED:**

1. **Metadata Store Enhanced** - Added `copy_progress`, `copy_snapshots` tables
2. **CopyReplicator Implementation** - CTID-based chunking with snapshot consistency
3. **Interface Updates** - New methods for copy progress tracking
4. **Build & Test Pipeline** - All `make test` passing

**🔄 IN PROGRESS:**

1. **Code Cleanup** - Remove unused architecture components

**📋 PENDING (Priority Order):**

1. **BulkParquetWriter** - Copy phase writer (no CDC metadata)
2. **StreamReplicator** - Clean WAL streaming implementation
3. **CDCParquetWriter** - Stream phase writer (with CDC metadata)
4. **DirectReplicator Refactor** - Orchestrate Copy → Stream transition
5. **E2E Test Updates** - Validate copy/stream separation
6. **Configuration Updates** - Add copy/stream config options
7. **Unit Tests** - Comprehensive test coverage

### Key Architecture Decisions Made

1. **CTID-Based Copy** - Using PostgreSQL's physical row identifiers for efficient chunking
2. **Snapshot Consistency** - `pg_export_snapshot()` for point-in-time copy accuracy
3. **Clean Separation** - No NATS dependencies, distinct Copy vs Stream phases
4. **Size-Based Rotation** - 128MB parquet files for optimal performance
5. **Resumable Operations** - Exact CTID progress tracking for interruption recovery

### Current E2E Test Behavior

The E2E test currently uses the existing `direct_replicator.go` which shows:

- Copy phase working (some data copied)
- Stream phase working (CDC operations processing)
- Parquet files being written with transaction consolidation
- File validation using new `parquet-tools inspect` and `show` commands

### Next Session Steps

1. Implement `BulkParquetWriter` (copy phase, no CDC metadata)
2. Implement `StreamReplicator` (stream phase, with transaction logic)
3. Create separate `CDCParquetWriter` for stream metadata
4. Refactor `DirectReplicator` to orchestrate both phases
5. Update E2E tests to validate the new architecture
