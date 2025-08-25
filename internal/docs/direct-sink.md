## Overview

This document defines the **Direct Sink architecture** - a high-performance replication mode that bypasses NATS for maximum throughput while implementing proven CDC optimization patterns including commit locks, transaction consolidation, and disk spilling.

**Key Goals:**

- 🎯 **10x performance improvement** over current architecture
- 🔒 **Transaction boundary consistency** with commit lock pattern
- 💾 **Unlimited transaction sizes** via memory + disk spilling
- 🗜️ **Operation consolidation** through key-based deduplication
- 📊 **Cloud warehouse support** with Parquet/S3 staging
- ⚡ **Parallel processing** for maximum throughput

## Problem Statement

### Current Architecture Limitations

**pg_flo Current Flow:**

```
PostgreSQL → BaseReplicator → NATS JetStream → Workers → Sinks
             ↑               ↑                ↑         ↑
         WAL capture     gob encoding    rules/routing  individual ops

```

**Critical Issues:**

1. **Double-buffering inefficiency**: Large transactions stored in both replicator memory AND worker memory
2. **Operation amplification**: Every CDC operation (INSERT/UPDATE/DELETE) becomes individual sink write
3. **Memory constraints**: No disk spilling for large transactions (multi-GB limit)
4. **Serialization overhead**: gob encoding/decoding for every message through NATS
5. **Missing consolidation**: 1000 operations on same row = 1000 sink operations (should be 1)

### Design Principles

**Industry-proven CDC patterns:**

- ✅ **Commit lock pattern**: Never flush mid-transaction
- ✅ **Raw table staging**: Intermediate storage for consolidation
- ✅ **AVRO/Parquet format**: Cloud warehouse native support
- ✅ **S3 staging**: Unlimited scalability for large datasets
- ✅ **Operation consolidation**: Key-based deduplication (10x reduction)

## Direct Sink Architecture Design

### Architecture Overview (Decoupled Two-Stage Design)

**Decoupled Architecture Pattern:**

```
Stage 1: PostgreSQL → DirectReplicator → S3/Parquet → Metadata DB
Stage 2: S3/Parquet → SyncWorker → Snowflake/Redshift/PostgreSQL
```

**Key Design Decisions:**

1. **Separate Metadata Database**: Following PeerDB's proven pattern, metadata is stored in a dedicated PostgreSQL database (not the source)
2. **Universal S3/Parquet Staging**: All destinations use S3 as intermediate format, enabling multi-destination support
3. **Transaction Boundary Preservation**: Complete transactions only, never mid-transaction flushing
4. **Constants Over Configuration**: 128MB Parquet files, PebbleDB spilling, cleanup policies built-in
5. **Zero Source Impact**: READ-ONLY replication from source, all writes go to separate metadata database

### Core Components

**1. DirectReplicator (Enhanced BaseReplicator)**

```go
type DirectReplicator struct {
    *BaseReplicator           // Reuse existing replication logic

    // Transaction management (commit lock pattern)
    commitLock       *pglogrepl.BeginMessage  // Prevents mid-transaction flushing
    txStore          *TransactionStore        // Memory + PebbleDB spilling
    consolidator     *OperationConsolidator   // Key-based deduplication

    // S3/Parquet staging (universal format)
    s3Client         *S3Client                // S3 connection
    parquetWriter    *ParquetWriter           // Parquet file writer
    metadataStore    *DirectSinkMetadataStore // SEPARATE metadata database

    maxMemoryBytes   int64                    // 128MB before PebbleDB spill
    maxFileBytes     int64                    // 128MB Parquet file target
}

```

**2. TransactionStore (Memory + PebbleDB Spilling)**

PebbleDB is ONLY used for transaction spill overflow, not general state management.

```go
type TransactionStore struct {
    inMemoryBuffer    map[string]*utils.CDCMessage
    memoryBytes       int64
    maxMemoryBytes    int64                    // 128MB limit

    diskStore         *pebble.DB               // ONLY for transaction spill
    diskBytes         int64
    diskPath          string
}

func (t *TransactionStore) Store(key string, msg *utils.CDCMessage) error {
    msgSize := int64(msg.EstimateSize())

    if t.memoryBytes + msgSize <= t.maxMemoryBytes {
        t.inMemoryBuffer[key] = msg
        t.memoryBytes += msgSize
        return nil
    }

    if t.diskStore == nil {
        if err := t.initPebbleDB(); err != nil {
            return fmt.Errorf("failed to initialize PebbleDB for transaction spill: %w", err)
        }
        r.Logger.Info().Msg("Transaction exceeds 128MB - spilling to PebbleDB")
    }

    encoded, err := msg.ToBytes()
    if err != nil {
        return err
    }

    t.diskBytes += msgSize
    return t.diskStore.Set([]byte(key), encoded, &pebble.WriteOptions{})
}

func (t *TransactionStore) Clear() error {
    t.inMemoryBuffer = make(map[string]*utils.CDCMessage)
    t.memoryBytes = 0

    if t.diskStore != nil {
        t.diskStore.Close()
        os.RemoveAll(t.diskPath)
        t.diskStore = nil
        t.diskBytes = 0
    }
    return nil
}

```

**3. PostgreSQL Metadata Store (Horizontal Scaling + Crash Recovery)**

**Worker Coordination Architecture:**

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                        Direct Sink Architecture                              │
│                    (Horizontally Scalable by Default)                        │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  Config: schemas.public: "*" → Auto-discover all tables                      │
│                                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Worker-1    │  │  Worker-2    │  │  Worker-3    │  │  Worker-N    │      │
│  │(Group: app)  │  │(Group: app)  │  │(Group: app)  │  │(Group: app)  │      │
│  │              │  │              │  │ (Crashed!)   │  │(Auto-scale)  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘      │
│        │                  │                  ✗                  │            │
│        ▼                  ▼                  ▼                  ▼            │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │              PostgreSQL Metadata Coordination                          │ │
│  │                                                                         │ │
│  │  🔒 Table Assignment (atomic, prevents duplication)                    │ │
│  │  📊 Copy Progress (range-level crash recovery)                         │ │
│  │  🌊 Streaming LSN (per-table position tracking)                        │ │
│  │  💾 Transaction Spill (PebbleDB for large transactions)                │ │
│  │  📁 S3 File Tracking (deduplication, cleanup after sync)               │ │
│  │                                                                         │ │
│  │  💡 Crash Recovery Scope:                                              │ │
│  │     - Copy ranges → Resume from last completed page                    │ │
│  │     - Streaming → Resume from last committed LSN                       │ │
│  │     - S3 writes → Avoid duplicate transaction files                    │ │
│  │     - Transaction memory → PebbleDB spill for unlimited size           │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│        │                  │                  │                  │            │
│        ▼                  ▼                  ▼                  ▼            │
│   users table       orders table     inventory table    events table        │
│ (copy-and-stream)  (copy-and-stream)  (stream-only)   (copy-and-stream)     │
│                                                                               │
│  📋 Each worker self-assigns tables atomically                              │ │
│  🔄 Failed workers → tables automatically reassigned                        │ │
│  ⚡ Each table spawns internal range-based copy workers                     │ │
│  🎯 One or more workers per group (horizontal scaling)                      │ │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Technical Distributed Systems Architecture:**

```
PostgreSQL Direct Sink Architecture (Queue-Based Auto-Scaling)

SOURCE TIER                    WORKER TIER (Auto-Scaling)               STORAGE TIER
┌─────────────────┐           ┌─────────────────────────────────────────────────────────────────────┐    ┌─────────────────┐
│ Production DB   │ READ-ONLY │                    Table Assignment Queue                          │    │      AWS S3     │
│                 ├──────────►│                                                                     ├───►│                 │
│ • WAL Stream   │           │  Server-1         Server-2         Server-3         Server-N      │    │ Parquet Files   │
│ • Logical Rep  │           │ ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │    │ • 128MB chunks  │
│ • Snapshots    │           │ │ Primary     │  │ Primary     │  │ Primary     │  │ Primary     │ │    │ • Transaction   │
└─────────────────┘           │ │ Worker      │  │ Worker      │  │ Worker      │  │ Worker      │ │    │   aligned       │
                              │ │ host1-1234- │  │ host2-5678- │  │ (crashed)   │  │ host4-3456- │ │    │ • Consolidated  │
┌─────────────────┐           │ │ a1b2c3d4    │  │ e5f6g7h8    │  │ OFFLINE     │  │ i9j0k1l2    │ │    └─────────────────┘
│ Metadata DB     │           │ │             │  │             │  │             │  │             │ │
│                 │           │ │ Heartbeat:  │  │ Heartbeat:  │  │ Heartbeat:  │  │ Heartbeat:  │ │
│ • table_assign  │◄──────────┤ │ 5s healthy  │  │ 5s healthy  │  │ >1min dead  │  │ 5s healthy  │ │
│ • copy_ranges   │           │ │             │  │             │  │             │  │             │ │
│ • s3_files      │           │ │ Tables:     │  │ Tables:     │  │ Tables:     │  │ Tables:     │ │    DESTINATION TIER
│ • replication   │           │ │ • users     │  │ • orders    │  │ • inventory │  │ • events    │ │    ┌─────────────────┐
└─────────────────┘           │ │ • logs      │  │ • payments  │  │   (orphaned)│  │ • analytics │ │    │ Snowflake       │
                              │ │             │  │             │  │             │  │             │ │    │ Redshift        │
                              │ │ Range       │  │ Range       │  │ Range       │  │ Range       │ │    │ PostgreSQL      │
                              │ │ Workers:    │  │ Workers:    │  │ Workers:    │  │ Workers:    │ │    │                 │
                              │ │ ├─ usr_r1   │  │ ├─ ord_r1   │  │ ├─ inv_r1   │  │ ├─ evt_r1   │ │    │ COPY FROM S3    │
                              │ │ ├─ usr_r2   │  │ ├─ ord_r2   │  │ ├─ inv_r2   │  │ ├─ evt_r2   │ │    │ FORMAT PARQUET  │
                              │ │ └─ usr_r3   │  │ └─ ord_r3   │  │ └─ inv_r3   │  │ └─ evt_r3   │ │    │ PURGE = TRUE    │
                              │ └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │    └─────────────────┘
                              └─────────────────────────────────────────────────────────────────────┘

Bootstrap & Scaling Logic:
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Bootstrap Process (Every Worker on Startup):                                                                          │
│ 1. Schema Discovery: Parse config → discover all tables from schemas.public: "*"                                     │
│ 2. Table Sync: INSERT ... ON CONFLICT DO UPDATE (idempotent)                                                          │
│ 3. Table Cleanup: DELETE tables not in config (first worker wins)                                                    │
│ 4. Self-Assignment: Claim available tables from queue                                                                 │
│                                                                                                                        │
│ Scenario 1: Single Worker Startup                                                                                     │
│ • Worker-1 boots: Discovers 15 tables, populates metadata, claims 5 tables                                           │
│ • Queue: 10 unassigned tables remaining                                                                               │
│ • Action: Worker processes 5 tables, queue has work waiting                                                           │
│                                                                                                                        │
│ Scenario 2: Rolling Restart (Config Changes)                                                                          │
│ • All workers restart with new config                                                                                 │
│ • Each worker: Re-discovers schema, syncs tables, removes old ones                                                    │
│ • Result: Metadata reflects new config, workers self-assign                                                           │
│                                                                                                                        │
│ Scenario 3: Auto-Scaling                                                                                              │
│ • Worker-2 starts: Skips schema discovery (already done), claims 5 tables from queue                                 │
│ • Worker-3 starts: Claims remaining 5 tables                                                                          │
│ • Queue: 0 tables remaining - perfect load distribution                                                               │
│                                                                                                                        │
│ Table Sync Algorithm (Idempotent):                                                                                    │
│ INSERT INTO table_assignments (...) VALUES (...) ON CONFLICT DO UPDATE SET sync_mode = EXCLUDED.sync_mode            │
│ DELETE FROM table_assignments WHERE group_name = $1 AND table NOT IN config AND assigned_worker_id IS NULL          │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

Key Properties:
• Server = pg_flo direct-replicator process (can run anywhere)
• Every worker does schema discovery on boot (idempotent operations)
• Rolling restart handles config changes automatically
• Queue-like behavior: PostgreSQL metadata acts as job queue
• Auto-scaling: New servers automatically claim work
• Fast failure detection: 1min heartbeat timeout
• No leader election needed: Atomic SQL handles all coordination
```

```sql
CREATE SCHEMA IF NOT EXISTS pg_flo_metadata;

-- Core replication state (replaces NATS State exactly)
CREATE TABLE pg_flo_metadata.replication_state (
    group_name VARCHAR(255) PRIMARY KEY,
    last_lsn TEXT NOT NULL,
    config_checksum TEXT NOT NULL,
    last_commit_time TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table assignments and crash recovery (horizontal scaling coordination)
CREATE TABLE pg_flo_metadata.table_assignments (
    group_name VARCHAR(255),
    schema_name VARCHAR(255),
    table_name VARCHAR(255),

    -- Primary worker coordination (hostname + PID/UUID)
    assigned_worker_id VARCHAR(255),       -- hostname-1234-uuid or NULL
    assigned_at TIMESTAMP,                 -- When assigned or NULL
    last_heartbeat TIMESTAMP,              -- Updated every 5s by primary worker

    -- Copy progress tracking (crash recovery)
    sync_mode VARCHAR(50) NOT NULL DEFAULT 'copy-and-stream', -- copy-and-stream, stream-only
    last_copied_page BIGINT DEFAULT 0,
    total_pages BIGINT,
    copy_started_at TIMESTAMP,
    copy_completed_at TIMESTAMP,

    -- Column configuration
    include_columns TEXT[],
    exclude_columns TEXT[],
    added_at_lsn TEXT,

    -- Streaming state
    last_streamed_lsn TEXT,

    PRIMARY KEY (group_name, schema_name, table_name)
);

-- Range-based copy progress (internal worker crash recovery)
-- ROWS DELETED when completed to keep table lean
CREATE TABLE pg_flo_metadata.copy_ranges (
    group_name VARCHAR(255),
    schema_name VARCHAR(255),
    table_name VARCHAR(255),
    assigned_worker_id VARCHAR(255),
    range_id INTEGER,
    start_page BIGINT NOT NULL,
    end_page BIGINT NOT NULL,
    rows_copied BIGINT DEFAULT 0,
    started_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (group_name, schema_name, table_name, range_id)
);

-- S3 file tracking (transaction crash recovery)
-- ROWS DELETED after successful sync to destinations
CREATE TABLE pg_flo_metadata.s3_files (
    group_name VARCHAR(255),
    s3_key TEXT NOT NULL,
    transaction_id BIGINT,
    lsn_start TEXT NOT NULL,
    lsn_end TEXT NOT NULL,
    file_size_bytes BIGINT,
    record_count BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (group_name, s3_key)
);

```

**How Rolling Restart Works:**

1. **User updates config** → `pg-flo-direct-sink.yaml`
2. **Rolling restart** → DirectReplicator reads config + metadata
3. **Config comparison** → Detect added/removed tables/columns
4. **Automatic sync** → Added tables get copied from current LSN
5. **Clean removal** → Removed tables stop syncing (no data cleanup)

**Complete Bootstrap Process (Modular/Idempotent):**

```go
func (r *DirectReplicator) Bootstrap(ctx context.Context) error {
    if err := r.ensureMetadataSchema(); err != nil {
        return fmt.Errorf("metadata schema setup failed: %w", err)
    }

    // Every worker does schema discovery on boot (idempotent)
    discoveredTables := r.discoverTablesFromConfig()

    if err := r.syncTablesWithConfig(ctx, discoveredTables); err != nil {
        return fmt.Errorf("table sync failed: %w", err)
    }

    assignedTables, err := r.selfAssignTables(ctx)
    if err != nil {
        return fmt.Errorf("table assignment failed: %w", err)
    }

    go r.startHeartbeatLoop(ctx)

    return r.startAssignedWork(ctx, assignedTables)
}

func (r *DirectReplicator) startHeartbeatLoop(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            r.updateHeartbeat(ctx)
        }
    }
}

func (r *DirectReplicator) updateHeartbeat(ctx context.Context) {
    _, err := r.metadataStore.Exec(ctx, `
        UPDATE pg_flo_metadata.table_assignments
        SET last_heartbeat = NOW()
        WHERE group_name = $1 AND assigned_worker_id = $2`,
        r.groupName, r.workerID)

    if err != nil {
        r.Logger.Error().Err(err).Msg("Failed to update heartbeat")
        return
    }

    r.checkCapacityAndSuggestScaling(ctx)
}

func (r *DirectReplicator) checkCapacityAndSuggestScaling(ctx context.Context) {
    var unassignedCount, activeWorkerCount int

    err := r.metadataStore.QueryRow(ctx, `
        SELECT
            COUNT(*) FILTER (WHERE assigned_worker_id IS NULL) as unassigned,
            COUNT(DISTINCT assigned_worker_id) FILTER (WHERE assigned_worker_id IS NOT NULL
                AND last_heartbeat > NOW() - INTERVAL '2 minutes') as active_workers
        FROM pg_flo_metadata.table_assignments
        WHERE group_name = $1`,
        r.groupName).Scan(&unassignedCount, &activeWorkerCount)

    if err != nil {
        r.Logger.Error().Err(err).Msg("Failed to check capacity")
        return
    }

    if unassignedCount > 0 && activeWorkerCount >= r.maxWorkersPerServer {
        r.Logger.Warn().
            Int("unassigned_tables", unassignedCount).
            Int("active_workers", activeWorkerCount).
            Int("max_workers_per_server", r.maxWorkersPerServer).
            Msg("Server at capacity - consider adding another pg_flo direct-replicator on a different server")
    }
}

func (r *DirectReplicator) discoverTablesFromConfig() []TableInfo {
    var tables []TableInfo

    for schemaName, schemaDef := range r.config.Schemas {
        if schemaDef == "*" {
            discoveredTables := r.discoverAllTablesInSchema(schemaName)
            tables = append(tables, discoveredTables...)
        } else {
            for tableName, tableDef := range schemaDef {
                tables = append(tables, TableInfo{
                    Schema:         schemaName,
                    Table:          tableName,
                    SyncMode:       tableDef.SyncMode,  // copy-and-stream (default) or stream-only
                    IncludeColumns: tableDef.IncludeColumns,
                    ExcludeColumns: tableDef.ExcludeColumns,
                })
            }
        }
    }
    return tables
}

func (r *DirectReplicator) discoverAllTablesInSchema(schemaName string) []TableInfo {
    rows, err := r.StandardConn.Query(ctx, `
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_type = 'BASE TABLE'`,
        schemaName)

    var tables []TableInfo
    for rows.Next() {
        var tableName string
        rows.Scan(&tableName)
        tables = append(tables, TableInfo{
            Schema:   schemaName,
            Table:    tableName,
            SyncMode: "copy-and-stream", // Default mode
        })
    }
    return tables
}

func (r *DirectReplicator) syncTablesWithConfig(ctx context.Context, configTables []TableInfo) error {
    // 1. Add/update tables from config (idempotent)
    for _, table := range configTables {
        _, err := r.metadataStore.Exec(ctx, `
            INSERT INTO pg_flo_metadata.table_assignments
            (group_name, schema_name, table_name, sync_mode, include_columns, exclude_columns, added_at_lsn)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (group_name, schema_name, table_name) DO UPDATE SET
                sync_mode = EXCLUDED.sync_mode,
                include_columns = EXCLUDED.include_columns,
                exclude_columns = EXCLUDED.exclude_columns`,
            r.groupName, table.Schema, table.Table, table.SyncMode,
            table.IncludeColumns, table.ExcludeColumns, r.getCurrentLSN().String())

        if err != nil {
            return fmt.Errorf("failed to sync table %s.%s: %w", table.Schema, table.Table, err)
        }
    }

    // 2. Remove tables not in config (first worker wins)
    configTableNames := make([]string, len(configTables))
    for i, table := range configTables {
        configTableNames[i] = fmt.Sprintf("%s.%s", table.Schema, table.Table)
    }

    result, err := r.metadataStore.Exec(ctx, `
        DELETE FROM pg_flo_metadata.table_assignments
        WHERE group_name = $1
        AND (schema_name || '.' || table_name) NOT IN (SELECT unnest($2::text[]))
        AND assigned_worker_id IS NULL`,  -- Only remove unassigned tables
        r.groupName, pq.Array(configTableNames))

    if err != nil {
        return fmt.Errorf("failed to cleanup removed tables: %w", err)
    }

    if rowsAffected, _ := result.RowsAffected(); rowsAffected > 0 {
        r.Logger.Info().Int64("removed_tables", rowsAffected).Msg("Cleaned up tables removed from config")
    }

    return nil
}

func (r *DirectReplicator) selfAssignTables(ctx context.Context) ([]TableAssignment, error) {
    // Generate worker ID: hostname-pid-uuid
    workerID := fmt.Sprintf("%s-%d-%s", r.hostname, os.Getpid(), uuid.New().String()[:8])

    rows, err := r.metadataStore.Query(ctx, `
        UPDATE pg_flo_metadata.table_assignments
        SET assigned_worker_id = $2,
            assigned_at = NOW(),
            last_heartbeat = NOW()
        WHERE group_name = $1
          AND (assigned_worker_id IS NULL
               OR last_heartbeat < NOW() - INTERVAL '1 minute'
               OR assigned_at < NOW() - INTERVAL '10 minutes')
          AND copy_completed_at IS NULL
        LIMIT $3
        RETURNING schema_name, table_name, sync_mode, last_copied_page, total_pages`,
        r.groupName, workerID, r.maxTablesPerWorker)

    var assignments []TableAssignment
    for rows.Next() {
        var assignment TableAssignment
        rows.Scan(&assignment.Schema, &assignment.Table, &assignment.SyncMode,
                 &assignment.CopyStatus, &assignment.LastCopiedPage, &assignment.TotalPages)
        assignments = append(assignments, assignment)
    }

        r.Logger.Info().Int("assigned_tables", len(assignments)).Str("worker_id", workerID).Msg("Self-assigned tables")

    return assignments, nil
}

func (r *DirectReplicator) startAssignedWork(ctx context.Context, assignments []TableAssignment) error {
    var wg sync.WaitGroup
    errChan := make(chan error, len(assignments))

    for _, assignment := range assignments {
        wg.Add(1)
        go func(a TableAssignment) {
            defer wg.Done()
            if err := r.handleTableAssignment(ctx, a); err != nil {
                errChan <- fmt.Errorf("failed to handle table %s.%s: %w", a.Schema, a.Table, err)
            }
        }(assignment)
    }

    wg.Wait()
    close(errChan)
    return r.collectErrors(errChan)
}

func (r *DirectReplicator) handleTableAssignment(ctx context.Context, assignment TableAssignment) error {
    switch assignment.SyncMode {
    case "copy-and-stream":
        if assignment.CopyStatus != "completed" {
            if err := r.resumeOrStartCopy(ctx, assignment); err != nil {
                return err
            }
        }
        return r.startStreaming(ctx, assignment)

    case "stream-only":
        return r.startStreaming(ctx, assignment)

    default:
        return fmt.Errorf("unknown sync mode: %s", assignment.SyncMode)
    }
}

func (r *DirectReplicator) resumeOrStartCopy(ctx context.Context, assignment TableAssignment) error {
    r.metadataStore.Exec(ctx, `
        UPDATE pg_flo_metadata.table_assignments
        SET copy_started_at = NOW()
        WHERE group_name = $1 AND schema_name = $2 AND table_name = $3`,
        r.groupName, assignment.Schema, assignment.Table)

    if assignment.CopyStartedAt == nil {
        return r.startFreshCopy(ctx, assignment)
    } else {
        return r.resumeCopyFromProgress(ctx, assignment)
    }
}

func (r *DirectReplicator) startFreshCopy(ctx context.Context, assignment TableAssignment) error {
    relPages, err := r.getRelPages(ctx, assignment.Table)
    if err != nil {
        return err
    }

    r.metadataStore.Exec(ctx, `
        UPDATE pg_flo_metadata.table_assignments
        SET total_pages = $4, copy_status = 'copying'
        WHERE group_name = $1 AND schema_name = $2 AND table_name = $3`,
        r.groupName, assignment.Schema, assignment.Table, relPages)

    ranges := r.generateRanges(relPages)

    for i, rng := range ranges {
        r.metadataStore.Exec(ctx, `
            INSERT INTO pg_flo_metadata.copy_ranges
            (group_name, schema_name, table_name, assigned_worker_id, range_id, start_page, end_page)
            VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            r.groupName, assignment.Schema, assignment.Table, r.workerID, i, rng[0], rng[1])
    }

    return r.executeCopyWithRanges(ctx, assignment)
}

func (r *DirectReplicator) resumeCopyFromProgress(ctx context.Context, assignment TableAssignment) error {
    return r.executeCopyWithRanges(ctx, assignment)
}

func (r *DirectReplicator) executeCopyWithRanges(ctx context.Context, assignment TableAssignment) error {
    pendingRanges, err := r.getPendingRanges(ctx, assignment)
    if err != nil {
        return err
    }

    rangesChan := make(chan CopyRange, len(pendingRanges))
    for _, rng := range pendingRanges {
        rangesChan <- rng
    }
    close(rangesChan)

    var wg sync.WaitGroup
    errChan := make(chan error, r.MaxCopyWorkersPerTable)

    for i := 0; i < r.MaxCopyWorkersPerTable; i++ {
        wg.Add(1)
        go r.copyRangeWorker(ctx, &wg, errChan, rangesChan, assignment, i)
    }

    wg.Wait()
    close(errChan)

    if err := r.collectErrors(errChan); err != nil {
        r.markTableFailed(ctx, assignment)
        return err
    }

    return r.markCopyCompleted(ctx, assignment)
}

func (r *DirectReplicator) copyRangeWorker(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error,
                                         rangesChan <-chan CopyRange, assignment TableAssignment, workerID int) {
    defer wg.Done()

    for rng := range rangesChan {
        r.metadataStore.Exec(ctx, `
            UPDATE pg_flo_metadata.copy_ranges
            SET status = 'copying', started_at = NOW()
            WHERE group_name = $1 AND schema_name = $2 AND table_name = $3 AND range_id = $4`,
            r.groupName, assignment.Schema, assignment.Table, rng.RangeID)

        rowsCopied, err := r.copyTableRange(ctx, assignment, rng, workerID)

                if err != nil {
            errChan <- err
            return
        }

        r.metadataStore.Exec(ctx, `
            DELETE FROM pg_flo_metadata.copy_ranges
            WHERE group_name = $1 AND schema_name = $2 AND table_name = $3 AND range_id = $4`,
            r.groupName, assignment.Schema, assignment.Table, rng.RangeID)

        r.Logger.Info().
            Str("table", fmt.Sprintf("%s.%s", assignment.Schema, assignment.Table)).
            Int("range_id", rng.RangeID).
            Int64("rows_copied", rowsCopied).
            Int("worker_id", workerID).
            Msg("Range copy completed")
    }
}

func (r *DirectReplicator) markCopyCompleted(ctx context.Context, assignment TableAssignment) error {
    _, err := r.metadataStore.Exec(ctx, `
        UPDATE pg_flo_metadata.table_assignments
        SET copy_status = 'completed',
            copy_completed_at = NOW(),
            worker_status = 'streaming'
        WHERE group_name = $1 AND schema_name = $2 AND table_name = $3`,
        r.groupName, assignment.Schema, assignment.Table)

    r.Logger.Info().
        Str("table", fmt.Sprintf("%s.%s", assignment.Schema, assignment.Table)).
        Msg("Copy phase completed, switching to streaming")

    return err
}

func (r *DirectReplicator) gracefulShutdown(ctx context.Context) error {
    r.Logger.Info().Str("worker_id", r.workerID).Msg("Graceful shutdown: releasing assignments")

    _, err := r.metadataStore.Exec(ctx, `
        UPDATE pg_flo_metadata.table_assignments
        SET assigned_worker_id = NULL,
            assigned_at = NULL
        WHERE group_name = $1 AND assigned_worker_id = $2`,
        r.groupName, r.workerID)

    return err
}
```

### State Management Migration

**Current (NATS KeyValue):**

```go
// pkg/pgflonats/pgflonats.go
type State struct {
    LSN              pglogrepl.LSN `json:"lsn"`
    LastProcessedSeq map[string]uint64
}

func (nc *NATSClient) SaveState(state State) error {
    kv, err := nc.js.KeyValue(nc.stateBucket)
    data, err := json.Marshal(state)
    _, err = kv.Put("state", data)
}

```

**New (PostgreSQL Metadata):**

```go
type DirectSinkMetadataStore struct {
    conn      *pgx.Pool
    groupName string
}

func (d *DirectSinkMetadataStore) SaveState(lsn pglogrepl.LSN) error {
    _, err := d.conn.Exec(context.Background(), `
        INSERT INTO pg_flo_metadata.replication_state
        (group_name, last_lsn, last_commit_time, updated_at)
        VALUES ($1, $2, NOW(), NOW())
        ON CONFLICT (group_name)
        DO UPDATE SET
            last_lsn = EXCLUDED.last_lsn,
            last_commit_time = NOW(),
            updated_at = NOW()`,
        d.groupName, lsn.String())
    return err
}

func (d *DirectSinkMetadataStore) GetLastLSN() (pglogrepl.LSN, error) {
    var lsnStr string
    err := d.conn.QueryRow(context.Background(), `
        SELECT last_lsn FROM pg_flo_metadata.replication_state
        WHERE group_name = $1`, d.groupName).Scan(&lsnStr)

    if errors.Is(err, pgx.ErrNoRows) {
        return 0, nil
    }
    return pglogrepl.ParseLSN(lsnStr)
}

```

## Core Optimizations

### 1. Commit Lock Pattern (Transaction Boundaries)

**Current Issue:**

```go
func (r *BaseReplicator) HandleInsertMessage(msg *pglogrepl.InsertMessage, lsn pglogrepl.LSN) error {
    cdcMessage := utils.CDCMessage{...}
    r.currentTxBuffer = append(r.currentTxBuffer, cdcMessage)
    return r.PublishToNATS(cdcMessage)
}

```

**Direct Sink Solution:**

```go
func (r *DirectReplicator) HandleBeginMessage(msg *pglogrepl.BeginMessage) error {
    r.commitLock = msg
    r.txStore.Clear()
    r.Logger.Debug().Uint64("xid", msg.Xid).Msg("Transaction BEGIN - locked")
    return nil
}

func (r *DirectReplicator) HandleInsertMessage(msg *pglogrepl.InsertMessage, lsn pglogrepl.LSN) error {
    cdcMessage := utils.CDCMessage{...}

    key := fmt.Sprintf("%s.%s.%s", cdcMessage.Schema, cdcMessage.Table, cdcMessage.GetPrimaryKeyString())
    return r.txStore.Store(key, &cdcMessage)
}

func (r *DirectReplicator) HandleCommitMessage(msg *pglogrepl.CommitMessage) error {
    r.commitLock = nil

    return r.flushCompleteTransaction(msg.CommitLSN)
}

func (r *DirectReplicator) flushCompleteTransaction(commitLSN pglogrepl.LSN) error {
    allMessages := r.txStore.GetAllMessages()

    consolidated := r.consolidator.ConsolidateTransaction(allMessages)

    r.Logger.Info().
        Int("original_ops", len(allMessages)).
        Int("consolidated_ops", len(consolidated)).
        Float64("reduction_ratio", float64(len(allMessages))/float64(len(consolidated))).
        Str("commit_lsn", commitLSN.String()).
        Msg("Transaction consolidation complete")

    parquetFiles, err := r.writeTransactionToS3(consolidated, r.commitLock.Xid)
    if err != nil {
        return err
    }

    return r.metadataStore.RecordTransactionCommit(TransactionCommit{
        GroupName:     r.Config.Group,
        CommitLSN:     commitLSN,
        TransactionID: r.commitLock.Xid,
        ParquetFiles:  parquetFiles,
    })
}

```

### 2. Operation Consolidation (Key-Based Deduplication)

**Operation Reduction Example:**

```
Transaction Input (10 operations):
INSERT users(1, 'Alice', 'alice@old.com')
UPDATE users(1, name='Alice Smith')
UPDATE users(1, email='alice@new.com')
UPDATE users(1, name='Alice Johnson')
INSERT orders(100, user_id=1, amount=50.00)
UPDATE orders(100, amount=75.00)
DELETE orders(101)
INSERT orders(102, user_id=1, amount=25.00)
UPDATE users(1, email='alice@final.com')
DELETE users(2)

Consolidation Logic (key = table.pk):
1. users.1: INSERT → UPDATE → UPDATE → UPDATE → UPDATE = INSERT(1, 'Alice Johnson', 'alice@final.com')
2. orders.100: INSERT → UPDATE = INSERT(100, user_id=1, amount=75.00)
3. orders.101: DELETE = DELETE(101)
4. orders.102: INSERT = INSERT(102, user_id=1, amount=25.00)
5. users.2: DELETE = DELETE(2)

Transaction Output (5 operations):
INSERT users(1, 'Alice Johnson', 'alice@final.com')  -- 5 ops → 1 op
INSERT orders(100, user_id=1, amount=75.00)          -- 2 ops → 1 op
DELETE orders(101)                                   -- 1 op → 1 op
INSERT orders(102, user_id=1, amount=25.00)          -- 1 op → 1 op
DELETE users(2)                                      -- 1 op → 1 op

Result: 10 operations → 5 operations (50% reduction)

```

**Implementation:**

```go
type OperationConsolidator struct {
    logger utils.Logger
}

func (c *OperationConsolidator) ConsolidateTransaction(messages []*utils.CDCMessage) []*utils.CDCMessage {
    keyMap := make(map[string]*utils.CDCMessage)

    // Ensure chronological order by LSN
    slices.SortFunc(messages, func(a, b *utils.CDCMessage) int {
        return cmp.Compare(a.LSN, b.LSN)
    })

    for _, msg := range messages {
        // Create unique key per row: schema.table.primary_key
        key := fmt.Sprintf("%s.%s.%s", msg.Schema, msg.Table, msg.GetPrimaryKeyString())

        switch msg.Type {
        case utils.OperationDelete:
            // DELETE eliminates all prior operations on this row
            keyMap[key] = msg

        case utils.OperationInsert:
            existing := keyMap[key]
            if existing == nil || existing.Type == utils.OperationDelete {
                keyMap[key] = msg  // First INSERT or INSERT after DELETE
            } else {
                // Merge INSERT with previous INSERT/UPDATE → final INSERT
                keyMap[key] = c.mergeToFinalState(existing, msg)
            }

        case utils.OperationUpdate:
            existing := keyMap[key]
            if existing == nil {
                // UPDATE without prior operation (shouldn't happen, but handle gracefully)
                keyMap[key] = msg
            } else if existing.Type == utils.OperationDelete {
                // UPDATE after DELETE (shouldn't happen, but handle gracefully)
                keyMap[key] = msg
            } else {
                // Merge UPDATE with previous INSERT/UPDATE → final state
                keyMap[key] = c.mergeToFinalState(existing, msg)
            }
        }
    }

    // Convert map back to slice
    consolidated := make([]*utils.CDCMessage, 0, len(keyMap))
    for _, msg := range keyMap {
        consolidated = append(consolidated, msg)
    }

    return consolidated
}

```

### 3. Disk Spilling for Unlimited Transaction Sizes

**Current Limitation:**

```go
// Workers have memory-only limits
type WorkerBuffer struct {
    maxMessages int    // 10,000 message cap
    maxBytes    int64  // 64MB memory cap
    // No disk spilling - large transactions fail
}

```

**Direct Sink Solution:**

```go
func (t *TransactionStore) Store(key string, msg *utils.CDCMessage) error {
    msgSize := int64(msg.EstimateSize())

    if t.memoryBytes + msgSize <= t.maxMemoryBytes {
        t.inMemoryBuffer[key] = msg
        t.memoryBytes += msgSize
        return nil
    }

    if t.diskStore == nil {
        if err := t.initPebbleDB(); err != nil {
            return fmt.Errorf("failed to initialize disk store: %w", err)
        }
        log.Info().Msg("Transaction size exceeded memory - spilling to disk")
    }

    encoded, err := msg.ToBytes()
    if err != nil {
        return err
    }

    t.diskBytes += msgSize
    return t.diskStore.Set([]byte(key), encoded, &pebble.WriteOptions{})
}

```

## Parallel Processing Architecture

### Parallel Copy Operations

**Direct Sink Copy-and-Stream Mode leveraging existing patterns:**

```go
func (r *DirectReplicator) ParallelCopyAndStream(ctx context.Context) error {
    tx, err := r.startSnapshotTransaction(ctx)
    if err != nil {
        return err
    }

    snapshotID, startLSN, err := r.getSnapshotInfo(tx)
    if err != nil {
        return err
    }

    r.Logger.Info().Str("snapshotID", snapshotID).Str("startLSN", startLSN.String()).Msg("Starting parallel copy")

    if err := r.CopyTablesWithRanges(ctx, r.Config.Tables, snapshotID); err != nil {
        return err
    }

    if err := tx.Commit(context.Background()); err != nil {
        return err
    }

    r.LastLSN = startLSN
    return r.StartReplicationFromLSN(ctx, startLSN, r.stopChan)
}

func (r *DirectReplicator) CopyTablesWithRanges(ctx context.Context, tables []string, snapshotID string) error {
    var wg sync.WaitGroup
    errChan := make(chan error, len(tables))

    for _, table := range tables {
        wg.Add(1)
        go func(tableName string) {
            defer wg.Done()
            if err := r.CopyTableWithRanges(ctx, tableName, snapshotID); err != nil {
                errChan <- fmt.Errorf("failed to copy table %s: %v", tableName, err)
            }
        }(table)
    }

    wg.Wait()
    close(errChan)

    return r.collectErrors(errChan)
}

func (r *DirectReplicator) CopyTableWithRanges(ctx context.Context, tableName, snapshotID string) error {
    relPages, err := r.getRelPages(ctx, tableName)
    if err != nil {
        return fmt.Errorf("failed to get table pages for %s: %v", tableName, err)
    }

    ranges := r.generateRanges(relPages)

    if err := r.metadataStore.RecordCopyRanges(r.Config.Group, tableName, ranges); err != nil {
        return err
    }

    rangesChan := make(chan [2]uint32, len(ranges))
    for _, rng := range ranges {
        rangesChan <- rng
    }
    close(rangesChan)

    var wg sync.WaitGroup
    errChan := make(chan error, r.MaxCopyWorkersPerTable)

    for i := 0; i < r.MaxCopyWorkersPerTable; i++ {
        wg.Add(1)
        go r.CopyTableRangeWorker(ctx, &wg, errChan, rangesChan, tableName, snapshotID, i)
    }

    wg.Wait()
    close(errChan)

    return r.collectErrors(errChan)
}

func (r *DirectReplicator) CopyTableRangeWorker(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error, rangesChan <-chan [2]uint32, tableName, snapshotID string, workerID int) {
    defer wg.Done()

    for rng := range rangesChan {
        startPage, endPage := rng[0], rng[1]

        rowsCopied, err := r.CopyTableRangeToS3(ctx, tableName, startPage, endPage, snapshotID, workerID)
        if err != nil {
            if err == context.Canceled {
                r.Logger.Info().Msg("Copy operation canceled")
                return
            }
            errChan <- fmt.Errorf("failed to copy table range: %v", err)
            return
        }

        r.metadataStore.UpdateRangeProgress(r.Config.Group, tableName, startPage, endPage, workerID, rowsCopied)

        r.Logger.Info().
            Str("table", tableName).
            Uint32("startPage", startPage).
            Uint32("endPage", endPage).
            Int64("rowsCopied", rowsCopied).
            Int("workerID", workerID).
            Msg("Copied table range to S3")
    }
}

```

### Parallel Transaction Processing

```go
// Parallel processing within transactions
func (r *DirectReplicator) flushCompleteTransaction(commitLSN pglogrepl.LSN) error {
    allMessages := r.txStore.GetAllMessages()

    // Parallel consolidation by table
    tableGroups := r.groupMessagesByTable(allMessages)

    var wg sync.WaitGroup
    consolidatedChan := make(chan []*utils.CDCMessage, len(tableGroups))
    errChan := make(chan error, len(tableGroups))

    // Parallel consolidation per table
    for table, messages := range tableGroups {
        wg.Add(1)
        go func(tableName string, msgs []*utils.CDCMessage) {
            defer wg.Done()

            consolidated := r.consolidator.ConsolidateTable(tableName, msgs)
            consolidatedChan <- consolidated
        }(table, messages)
    }

    wg.Wait()
    close(consolidatedChan)
    close(errChan)

    // Collect consolidated results
    var finalMessages []*utils.CDCMessage
    for consolidated := range consolidatedChan {
        finalMessages = append(finalMessages, consolidated...)
    }

    // Single batch write to sink
    if err := r.sink.WriteBatch(finalMessages); err != nil {
        return err
    }

    r.LastLSN = commitLSN
    r.txStore.Clear()
    return r.metadataStore.SaveState(commitLSN)
}

```

## Configuration & Interfaces

### Factory Pattern (Following root.go)

```go
// pkg/replicator/factory.go - extend existing factory pattern
type DirectSinkReplicatorFactory struct {
    MaxCopyWorkersPerTable int
    TransactionStore       TransactionStoreConfig
    MetadataDB             PostgresConfig
    S3Config               *S3Config  // Optional cloud warehouse
}

func (f *DirectSinkReplicatorFactory) CreateReplicator(config Config, metadataStore *PostgresMetadataStore) (Replicator, error) {
    // Create base replicator (reuse existing logic)
    replicationConn := NewReplicationConnection(config)
    standardConn := NewStandardConnection(config)

    baseReplicator := NewBaseReplicator(config, replicationConn, standardConn, nil) // No NATS client

    // Create direct sink components
    txStore, err := NewTransactionStore(f.TransactionStore)
    if err != nil {
        return nil, fmt.Errorf("failed to create transaction store: %w", err)
    }

    consolidator := NewOperationConsolidator()

    sink, err := NewDirectSink(config, f.S3Config)
    if err != nil {
        return nil, fmt.Errorf("failed to create direct sink: %w", err)
    }

    return &DirectReplicator{
        BaseReplicator:         baseReplicator,
        commitLock:            nil,
        txStore:               txStore,
        consolidator:          consolidator,
        sink:                  sink,
        metadataStore:         metadataStore,
        MaxCopyWorkersPerTable: f.MaxCopyWorkersPerTable,
    }, nil
}

```

### Configuration Structure

```go
// pkg/replicator/config.go - extend existing config
type DirectSinkConfig struct {
    Config                    // Embed existing config

    Mode                     string  // "nats" or "direct"
    TransactionStore         TransactionStoreConfig
    Consolidation           ConsolidationConfig
    MetadataDB              PostgresConfig  // For state storage
    S3Staging               *S3Config       // Optional cloud warehouse
    ParallelWorkers         int             // Copy workers per table
}

type TransactionStoreConfig struct {
    MaxMemoryMB       int     // 128MB default
    DiskPath          string  // "/tmp/pg_flo_transactions"
    // Note: Always uses PebbleDB for disk spilling - no configuration needed
}

type ConsolidationConfig struct {
    Enabled           bool    // true for direct sink
    MaxOperations     int     // Consolidate only if > threshold
    KeyTimeout        time.Duration // Max time to wait for key completion
}

type PostgresConfig struct {
    Host              string
    Port              int
    Database          string
    User              string
    Password          string
    Schema            string  // "pg_flo_metadata" default
}

```

### Command Line Interface (Updated root.go)

**Two-Stage Architecture with Config-First Approach:**

```bash
# Stage 1: Source → S3 Parquet
pg_flo direct-replicator \\
    --config pg-flo-direct-sink.yaml \\
    --group myapp

# Stage 2: S3 → Destinations
pg_flo direct-sync \\
    --config pg-flo-direct-sink.yaml \\
    --worker snowflake-prod \\
    --group myapp

pg_flo direct-sync \\
    --config pg-flo-direct-sink.yaml \\
    --worker postgres-analytics \\
    --group myapp

```

### YAML Configuration

**Clean Direct Sink Configuration (Easy UX):**

```yaml
# pg-flo-direct-sink.yaml - Rolling Restart Configuration
# Source PostgreSQL Connection (READ-ONLY replication)
host: "source-postgres.example.com"
port: 5432
dbname: "production_app"
user: "pg_flo_replication_user"
password: "replication_secret"
group: "myapp"

# Direct Sink Configuration
direct-sink:
  enabled: true
  mode: "copy-and-stream"

  # Simple worker configuration
  max_tables_per_worker: 5 # Limit tables per worker process
  max_workers_per_server: 3 # Prevent resource exhaustion

  # Metadata Database (can be same as source for simplicity)
  metadata-db:
    # Users can use same connection for convenience
    host: "source-postgres.example.com" # Same as source
    port: 5432 # Same as source
    dbname: "production_app" # Same as source
    user: "pg_flo_replication_user" # Same user
    password: "replication_secret" # Same password

  # S3 Configuration
  s3:
    bucket: "my-data-lake"
    region: "us-east-1"

# Schema-Based Table Configuration (Rolling Restart Support)
schemas:
  public: "*" # Auto-discover all tables in public schema (default: copy-and-stream)

  # Or explicit table configuration with modes
  # public:
  #   users:
  #     sync_mode: "copy-and-stream"  # Default mode
  #     include_columns: ["id", "email", "name", "created_at"]
  #     exclude_columns: ["password_hash", "ssn"]
  #
  #   orders:
  #     sync_mode: "copy-and-stream"
  #     include_columns: "*"  # All columns
  #
  #   inventory:
  #     sync_mode: "stream-only"  # Only new changes, no historical copy
  #     include_columns: ["id", "product_id", "quantity", "warehouse_id"]

  analytics: "*" # Auto-discover all tables (will be copy-and-stream by default)

# Built-in Constants (not configurable for simplicity):
# - 128MB Parquet files, PebbleDB spilling, cleanup after sync

---
# Sync Worker Configuration (pg-flo-sync-workers.yaml)
group: "myapp"

# Same Metadata Database
metadata-db:
  host: "source-postgres.example.com"
  port: 5432
  dbname: "production_app"
  user: "pg_flo_replication_user"
  password: "replication_secret"

# Multi-Destination Support
workers:
  snowflake-prod:
    type: "snowflake"
    account: "account.region"
    user: "sf_user"
    password: "sf_password"
    database: "TARGET_DB"
    schema: "PUBLIC"

  postgres-analytics:
    type: "postgres"
    host: "analytics-postgres.com"
    port: 5432
    dbname: "analytics_db"
    user: "analytics_user"
    password: "analytics_password"
```

## Key Interfaces

### Replicator Interface (Extend Existing)

```go
// pkg/replicator/interfaces.go - extend existing interfaces
type Replicator interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    // Add direct sink specific methods
    GetTransactionStats() TransactionStats
    GetConsolidationStats() ConsolidationStats
}

type TransactionStats struct {
    MemoryUsageBytes    int64
    DiskUsageBytes      int64
    CurrentTransaction  *TransactionInfo
    SpillEventsCount    int64
}

type ConsolidationStats struct {
    OperationsInput     int64
    OperationsOutput    int64
    ReductionRatio      float64
    LastConsolidation   time.Time
}

```

### Sink Interface (Extend Existing)

```go
// pkg/sinks/sink.go - extend existing sink interface
type Sink interface {
    WriteBatch(data []*utils.CDCMessage) error
    // Add direct sink methods
    CopyFromQuery(ctx context.Context, table string, rows pgx.Rows) (int64, error)
    GetBatchingStats() BatchingStats
}

type BatchingStats struct {
    CopyFromOperations     int64
    MultiRowOperations     int64
    PreparedStmtOperations int64
    AverageLatency         time.Duration
}

```

### TransactionStore Interface

```go
// pkg/storage/interfaces.go
type TransactionStore interface {
    Store(key string, msg *utils.CDCMessage) error
    GetAllMessages() []*utils.CDCMessage
    Clear() error
    GetStats() TransactionStats
    Close() error
}

type KeyValueStore interface {
    Set(key []byte, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    Close() error
}

```

## Cloud Warehouse Integration

### Snowflake & Redshift Compatibility

**Confirmed: Both platforms support Parquet from S3**

- ✅ **Snowflake**: `COPY INTO table FROM @s3_stage file_format=(TYPE='PARQUET')`
- ✅ **Redshift**: `COPY table FROM 's3://bucket/path' FORMAT AS PARQUET`

**File Organization Strategy (Finalized Design):**

```
s3://bucket/pg_flo/{group_name}/2024/01/15/10/
├── users_tx_12345.parquet      # Single table, single transaction
├── orders_tx_12345.parquet     # Same transaction, different table
├── users_tx_12346.parquet      # Next transaction
└── metadata_tx_12345.json      # Transaction metadata

Constants (non-configurable):
- 128MB max file size (optimal for cloud warehouses)
- Cleanup after successful sync (PeerDB PURGE=TRUE pattern)
- Hourly S3 prefixes for efficient organization
- One table per file, transaction boundaries preserved
- Key-based consolidation within transactions (50-90% reduction)

```

### Parquet Writer Implementation

**File Size Strategy (Constants):**

```go
const (
    PARQUET_TARGET_SIZE_BYTES = 128 * 1024 * 1024
    PARQUET_MAX_ROWS         = 5_000_000
    PARQUET_FLUSH_TIMEOUT    = 30 * time.Minute
)

```

```go
// pkg/cloud/parquet_writer.go
type ParquetWriter struct {
    bucket          string
    prefix          string
    schema          *parquet.Schema
    compressionType parquet.CompressionCodec

    // File size tracking
    currentSizeBytes int64
    currentRowCount  int64
    lastFlushTime    time.Time
}

func (p *ParquetWriter) ShouldFlushFile() bool {
    if p.currentSizeBytes >= PARQUET_TARGET_SIZE_BYTES {
        return true
    }

    if p.currentRowCount >= PARQUET_MAX_ROWS {
        return true
    }

    if time.Since(p.lastFlushTime) >= PARQUET_FLUSH_TIMEOUT {
        return true
    }

    return false
}

func (p *ParquetWriter) WriteTransaction(messages []*utils.CDCMessage) (string, error) {
    tableGroups := groupMessagesByTable(messages)

    var fileUrls []string
    for table, msgs := range tableGroups {
        estimatedSize := p.estimateMessageSize(msgs)
        if p.currentSizeBytes + estimatedSize >= PARQUET_TARGET_SIZE_BYTES && p.currentSizeBytes > 0 {
            if err := p.flushCurrentFile(); err != nil {
                return "", fmt.Errorf("failed to flush current file: %w", err)
            }
        }

        fileName := fmt.Sprintf("%s_%s_%d.parquet",
            table,
            time.Now().Format("15_04"),
            rand.Int63())

        fileUrl, err := p.writeTableParquet(table, msgs, fileName)
        if err != nil {
            return "", fmt.Errorf("failed to write %s to parquet: %w", table, err)
        }
        fileUrls = append(fileUrls, fileUrl)
    }

    return strings.Join(fileUrls, ","), nil
}

func (p *ParquetWriter) writeTableParquet(table string, messages []*utils.CDCMessage, fileName string) (string, error) {
    schema, err := p.createSchemaFromMessage(messages[0])
    if err != nil {
        return "", err
    }

    s3Key := fmt.Sprintf("%s/%s/%s", p.prefix, table, fileName)

    return fmt.Sprintf("s3://%s/%s", p.bucket, s3Key), nil
}

```

## Technical Implementation Notes

### Design Decisions (Non-Configurable)

**Spill Storage: PebbleDB Only**

- **Why PebbleDB**: Fast, embedded, LSM-tree based (Google leveldb successor)
- **Why not configurable**: Single dependency, proven performance, no user choice needed
- **Memory threshold**: 128MB (doubled from original 64MB for better batching)
- **Disk cleanup**: Automatic on transaction commit/abort

**Parquet File Sizes: Constants Only**

- **128MB target**: Optimal for Snowflake/Redshift parallel processing and S3 transfer
- **5M row limit**: Prevents memory issues with very wide tables
- **30min timeout**: Ensures files flush even in low-volume scenarios
- **Why not configurable**: Cloud warehouse optimization is well-established, user choice adds complexity without benefit

**Transaction Commit & File Strategy**

- **During transaction**: Memory → PebbleDB spilling to avoid memory pressure from long-running transactions
- **On transaction commit**: Consolidate operations and flush to current Parquet file
- **File rotation**: Independent size/time-based rotation (128MB target, 30min timeout)
- **No S3 spilling**: S3 used only for completed Parquet files, not transaction overflow

## Performance Expectations

### Throughput Improvements

**Current Baseline (pg_flo NATS mode):**

- 10,000 ops/sec sustained throughput
- 64MB memory limit per worker
- Individual operations to sink

**Direct Sink Target:**

- 100,000 ops/sec sustained throughput (10x improvement)
- Unlimited transaction sizes via disk spilling
- Consolidated operations (50-90% reduction)

**Breakdown by Component:**

- ❌ **Remove NATS overhead**: +30% (eliminate gob encoding/decoding)
- ❌ **Remove double-buffering**: +25% (single transaction store)
- ✅ **Add consolidation**: +300% (10x fewer sink operations)
- ✅ **Add advanced batching**: +200% (COPY FROM, multi-row INSERT)
- ✅ **Add parallel processing**: +200% (parallel copy, parallel consolidation)
- ✅ **Add disk spilling**: +∞% (handle unlimited transaction sizes)

### Memory Usage

**Current (per worker):**

- 64MB maximum memory usage
- Fails on large transactions

**Direct Sink:**

- 128MB hot memory + unlimited disk spilling
- Handles multi-GB transactions gracefully
- Automatic pressure management via PebbleDB

### Operational Benefits

**Reliability:**

- ✅ Perfect transaction boundaries (no partial transactions)
- ✅ Automatic retry with disk persistence
- ✅ Graceful handling of massive transactions via PebbleDB spilling
- ✅ Comprehensive state management in separate PostgreSQL metadata database
- ✅ Zero impact on source database (READ-ONLY replication)

**Monitoring:**

- ✅ Transaction size metrics (memory/disk/S3 usage)
- ✅ Consolidation effectiveness metrics (reduction ratio)
- ✅ S3 file lifecycle tracking (created → synced → cleaned up)
- ✅ LSN progress tracking per group
- ✅ Multi-destination sync coordination

**Operations:**

- ✅ No external NATS dependency for direct sink pipelines
- ✅ All state stored in familiar PostgreSQL (separate from source)
- ✅ Standard PostgreSQL backup/recovery for metadata
- ✅ Decoupled architecture: source issues don't affect sync workers
- ✅ Multi-destination support: one S3 stream → multiple warehouses

**Bootstrap & Validation:**

- ✅ pg_flo automatically creates required metadata schema on startup
- ✅ Validates connectivity to both source and metadata databases
- ✅ Graceful shutdown if connections cannot be established
- ✅ No manual schema setup required from users

## Implementation Handoff Notes

### Core Requirements Satisfied

**✅ Transaction Boundaries & Consolidation**

- Commit lock pattern prevents mid-transaction flushing
- Key-based consolidation reduces operations by 50-90%
- Memory → PebbleDB spilling for unlimited transaction sizes

**✅ Parallel Processing & Resumability**

- Parallel copy workers coordinated via `copy_partitions` table
- Copy progress tracked per table in `copy_progress` table
- LSN tracking in `replication_state` table for seamless resume

**✅ Clean User Interface**

- Simple CLI: `pg_flo replicate --direct-sink postgres`
- Extended pg-flo.yaml with minimal new configuration
- Technical constants (128MB files, PebbleDB) built-in, not configurable

**✅ Cloud Warehouse Ready**

- Parquet format with size/time-based rotation (128MB, 30min)
- S3 staging path includes group organization: `s3://bucket/pg_flo/{group_name}/`
- Snowflake/Redshift COPY commands confirmed compatible

## Deployment Examples

### **Single Worker (Simple Setup)**

```bash
pg_flo direct-replicator \
    --config pg-flo-direct-sink.yaml \
    --group myapp
# Auto-generates worker ID: hostname-pid-uuid
```

### **Horizontal Scaling (Simple Workers)**

```bash
# Worker 1 (auto-generates: host1-1234-a1b2c3d4)
pg_flo direct-replicator \
    --config pg-flo-direct-sink.yaml \
    --group myapp

# Worker 2 on different server (auto-generates: host2-5678-e5f6g7h8)
pg_flo direct-replicator \
    --config pg-flo-direct-sink.yaml \
    --group myapp

# Worker 3 on same server as worker 1 (auto-generates: host1-9012-i9j0k1l2)
pg_flo direct-replicator \
    --config pg-flo-direct-sink.yaml \
    --group myapp

# All workers automatically self-assign available tables
# Crashed workers → tables automatically reassigned after 10min timeout
```

### **Crash Recovery Scenario**

```bash
# If worker-2 crashes mid-copy:
# 1. Tables assigned to worker-2 marked as 'failed'
# 2. Copy ranges in 'copying' status become 'pending'
# 3. New worker picks up failed work and resumes from last completed range

pg_flo direct-replicator \
    --config pg-flo-direct-sink.yaml \
    --group myapp \
    --worker-id worker-2-replacement
# → Automatically resumes where worker-2 left off
```

## Key Benefits

### **Operational Excellence**

- ✅ **Zero-duplication work**: Atomic table assignment prevents overlap
- ✅ **Crash recovery**: Resume from exact point of failure (range-level)
- ✅ **Rolling restarts**: Config changes picked up automatically
- ✅ **Horizontal scaling**: Add more workers for higher throughput
- ✅ **Modular/Idempotent**: Workers can start/stop independently

### **Performance Gains**

- ✅ **10x improvement**: Consolidation + batching + direct sink
- ✅ **Transaction boundaries**: Perfect consistency with commit lock
- ✅ **Unlimited size**: Memory + PebbleDB handles any transaction
- ✅ **Parallel copy**: Range-based workers for initial data load
- ✅ **S3 staging**: Universal format for multi-destination support

### **Worker Types & Failure Detection**

**Worker Hierarchy:**

- 🖥️ **SERVER**: Physical or virtual machine running workers
- 📋 **PRIMARY WORKER**: Main process that coordinates tables for a group
  - Owns specific tables (`assigned_worker_id`)
  - Sends heartbeat every 5 seconds (`last_heartbeat`)
  - Spawns range workers for copy operations
- ⚡ **RANGE WORKER**: Internal goroutine for parallel copy operations
  - Spawned by primary worker for large table copies
  - Follows existing `copy_and_stream_replicator.go` pattern
  - No heartbeat (managed by primary worker)

**Fast Failure Detection (Two-Tier):**

1. **Primary Detection**: `last_heartbeat < NOW() - INTERVAL '1 minute'` ⚡ (Fast)
2. **Backup Detection**: `assigned_at < NOW() - INTERVAL '10 minutes'` 🛡️ (Fallback)

**Heartbeat Implementation:**

```go
// Every primary worker runs this loop (5s interval)
func (r *DirectReplicator) startHeartbeatLoop(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)
    for {
        r.updateHeartbeat(ctx)              // UPDATE last_heartbeat = NOW()
        r.checkCapacityAndSuggestScaling(ctx)  // Check if scaling needed
        <-ticker.C
    }
}

// Capacity monitoring runs with heartbeat
func (r *DirectReplicator) checkCapacityAndSuggestScaling(ctx context.Context) {
    if unassignedCount > 0 && activeWorkerCount >= maxWorkersPerServer {
        r.Logger.Warn().Msg("Server at capacity - consider adding another pg_flo direct-replicator on a different server")
    }
}
```

### **Crash Recovery Scope & Data Cleanup**

**PostgreSQL Metadata (State Management):**

- ✅ **Table assignments**: Worker coordination and failover
- ✅ **Copy progress**: Range-level resumability
- ✅ **Streaming position**: Per-table LSN tracking
- ✅ **S3 file tracking**: Deduplication and cleanup coordination

**PebbleDB (Transaction Spill ONLY):**

- ✅ **Large transactions**: Memory overflow (>128MB) spills to disk
- ✅ **Temporary storage**: Cleaned up after transaction commit
- ✅ **Crash recovery**: Restart clears spill directory
- ❌ **NOT used for**: Worker coordination, copy progress, or streaming state

**Data Cleanup Strategy (Lean Tables):**

- ✅ **copy_ranges**: Rows deleted when range completed (keeps table lean)
- ✅ **s3_files**: Rows deleted after successful sync to destinations
- ✅ **Timestamps over status**: Use `started_at`, `completed_at` instead of string status
- ✅ **Failed worker detection**: `assigned_at < NOW() - INTERVAL '10 minutes'`

### Ready for Implementation

This document provides a complete technical framework for implementing Direct Sink mode:

1. **Existing code reuse**: BaseReplicator, Config, connection patterns from `pkg/replicator/`
2. **New components**: DirectReplicator, TransactionStore, PostgresMetadataStore (separate DB)
3. **Database design**: Four focused metadata tables for state and S3 file lifecycle tracking
4. **Configuration**: Clean extension of existing pg-flo.yaml and CLI patterns in `cmd/root.go`
5. **Performance targets**: 10x improvement through consolidation and batching
6. **Decoupled architecture**: Two-stage process (Source→S3, S3→Destinations) following PeerDB patterns

**Key Implementation Principles:**

- ✅ **Zero source database impact**: READ-ONLY replication + separate metadata database
- ✅ **Constants over configuration**: 128MB files, PebbleDB spilling, cleanup policies built-in
- ✅ **Automatic schema management**: pg_flo creates required metadata tables on startup
- ✅ **Transaction boundary preservation**: Commit lock pattern prevents mid-transaction flushing
- ✅ **Multi-destination support**: Decoupled S3 staging enables multiple sync workers
- ✅ **Proven patterns**: Follows established metadata store and cleanup approaches

The design prioritizes pg_flo's strengths (parallel processing, resumability) while eliminating NATS overhead for maximum throughput to direct sinks and cloud warehouses.
