package direct

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lib/pq"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// PostgresMetadataStore implements MetadataStore using PostgreSQL
type PostgresMetadataStore struct {
	pool   *pgxpool.Pool
	logger utils.Logger
}

// NewPostgresMetadataStore creates a new PostgreSQL metadata store
func NewPostgresMetadataStore(connString string, logger utils.Logger) (*PostgresMetadataStore, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	return &PostgresMetadataStore{
		pool:   pool,
		logger: logger,
	}, nil
}

// Connect establishes the connection to PostgreSQL
func (ms *PostgresMetadataStore) Connect(ctx context.Context) error {
	return ms.pool.Ping(ctx)
}

// Close closes the connection pool
func (ms *PostgresMetadataStore) Close() error {
	ms.pool.Close()
	return nil
}

// EnsureSchema creates the metadata schema and tables if they don't exist
func (ms *PostgresMetadataStore) EnsureSchema(ctx context.Context) error {
	schemas := []string{
		`CREATE SCHEMA IF NOT EXISTS pgflo_metadata`,

		`CREATE TABLE IF NOT EXISTS pgflo_metadata.table_assignments (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			group_name TEXT NOT NULL,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL,
			sync_mode TEXT NOT NULL DEFAULT 'copy-and-stream',
			include_columns TEXT[],
			exclude_columns TEXT[],
			assigned_worker_id TEXT,
			assigned_at TIMESTAMP WITH TIME ZONE,
			last_heartbeat TIMESTAMP WITH TIME ZONE,
			last_copied_page INTEGER DEFAULT 0,
			total_pages INTEGER DEFAULT 0,
			copy_started_at TIMESTAMP WITH TIME ZONE,
			copy_completed_at TIMESTAMP WITH TIME ZONE,
			added_at_lsn TEXT NOT NULL,
			last_streamed_lsn TEXT DEFAULT '0/0',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			UNIQUE(group_name, schema_name, table_name)
		)`,

		`CREATE TABLE IF NOT EXISTS pgflo_metadata.replication_state (
			group_name TEXT PRIMARY KEY,
			last_lsn TEXT NOT NULL DEFAULT '0/0',
			copy_completed BOOLEAN DEFAULT FALSE,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,

		`CREATE TABLE IF NOT EXISTS pgflo_metadata.copy_ranges (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			assignment_id UUID NOT NULL REFERENCES pgflo_metadata.table_assignments(id) ON DELETE CASCADE,
			start_page INTEGER NOT NULL,
			end_page INTEGER NOT NULL,
			completed_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,

		`CREATE TABLE IF NOT EXISTS pgflo_metadata.s3_files (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			file_path TEXT NOT NULL UNIQUE,
			tx_id TEXT NOT NULL,
			table_names TEXT[] NOT NULL,
			processed_at TIMESTAMP WITH TIME ZONE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,

		`CREATE TABLE IF NOT EXISTS pgflo_metadata.copy_progress (
			group_name VARCHAR(255) NOT NULL,
			table_name VARCHAR(255) NOT NULL,
			last_ctid VARCHAR(50),
			bytes_written BIGINT DEFAULT 0,
			file_count INTEGER DEFAULT 0,
			status VARCHAR(20) DEFAULT 'NOT_STARTED',
			snapshot_name VARCHAR(255),
			started_at TIMESTAMP WITH TIME ZONE,
			completed_at TIMESTAMP WITH TIME ZONE,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			PRIMARY KEY (group_name, table_name)
		)`,

		`CREATE TABLE IF NOT EXISTS pgflo_metadata.copy_snapshots (
			group_name VARCHAR(255) NOT NULL PRIMARY KEY,
			snapshot_name VARCHAR(255) NOT NULL,
			snapshot_lsn TEXT NOT NULL,
			exported_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			handoff_completed BOOLEAN DEFAULT FALSE
		)`,
	}

	for _, schema := range schemas {
		if _, err := ms.pool.Exec(ctx, schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	ms.logger.Info().Msg("Metadata schema ensured")
	return nil
}

// RegisterTables adds or updates table configurations in the metadata store
func (ms *PostgresMetadataStore) RegisterTables(ctx context.Context, groupName string, tables []TableInfo) error {
	tx, err := ms.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, table := range tables {
		_, err := tx.Exec(ctx, `
			INSERT INTO pgflo_metadata.table_assignments
			(group_name, schema_name, table_name, sync_mode, include_columns, exclude_columns, added_at_lsn)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (group_name, schema_name, table_name) DO UPDATE SET
				sync_mode = EXCLUDED.sync_mode,
				include_columns = EXCLUDED.include_columns,
				exclude_columns = EXCLUDED.exclude_columns`,
			groupName, table.Schema, table.Table, table.SyncMode,
			pq.Array(table.IncludeColumns), pq.Array(table.ExcludeColumns), "0/0")

		if err != nil {
			return fmt.Errorf("failed to register table %s.%s: %w", table.Schema, table.Table, err)
		}
	}

	return tx.Commit(ctx)
}

// GetUnassignedTables retrieves unassigned tables for worker assignment
func (ms *PostgresMetadataStore) GetUnassignedTables(ctx context.Context, groupName string, maxTables int) ([]TableAssignment, error) {
	query := `
		SELECT id, group_name, schema_name, table_name, sync_mode,
			   include_columns, exclude_columns, added_at_lsn, last_streamed_lsn
		FROM pgflo_metadata.table_assignments
		WHERE group_name = $1
		AND (assigned_worker_id IS NULL OR last_heartbeat < NOW() - INTERVAL '1 minute')
		ORDER BY created_at
		LIMIT $2`

	rows, err := ms.pool.Query(ctx, query, groupName, maxTables)
	if err != nil {
		return nil, fmt.Errorf("failed to query unassigned tables: %w", err)
	}
	defer rows.Close()

	var assignments []TableAssignment
	for rows.Next() {
		var ta TableAssignment
		var includeColumns, excludeColumns []string

		err := rows.Scan(
			&ta.ID, &ta.GroupName, &ta.SchemaName, &ta.TableName, &ta.SyncMode,
			pq.Array(&includeColumns), pq.Array(&excludeColumns),
			&ta.AddedAtLSN, &ta.LastStreamedLSN)

		if err != nil {
			return nil, fmt.Errorf("failed to scan table assignment: %w", err)
		}

		ta.IncludeColumns = includeColumns
		ta.ExcludeColumns = excludeColumns
		assignments = append(assignments, ta)
	}

	return assignments, nil
}

// AssignTables assigns tables to a worker
func (ms *PostgresMetadataStore) AssignTables(ctx context.Context, workerID string, tableIDs []string) error {
	_, err := ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.table_assignments
		SET assigned_worker_id = $1, assigned_at = NOW(), last_heartbeat = NOW()
		WHERE id = ANY($2)
		AND (assigned_worker_id IS NULL OR last_heartbeat < NOW() - INTERVAL '1 minute')`,
		workerID, pq.Array(tableIDs))

	if err != nil {
		return fmt.Errorf("failed to assign tables: %w", err)
	}

	return nil
}

// UpdateHeartbeat updates the heartbeat for a worker
func (ms *PostgresMetadataStore) UpdateHeartbeat(ctx context.Context, workerID string) error {
	_, err := ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.table_assignments
		SET last_heartbeat = NOW()
		WHERE assigned_worker_id = $1`,
		workerID)

	if err != nil {
		return fmt.Errorf("failed to update heartbeat: %w", err)
	}

	return nil
}

// SaveCopyProgress saves the copy progress for a table
func (ms *PostgresMetadataStore) SaveCopyProgress(ctx context.Context, assignmentID string, lastPage uint32, totalPages uint32) error {
	_, err := ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.table_assignments
		SET last_copied_page = $2, total_pages = $3, copy_started_at = COALESCE(copy_started_at, NOW())
		WHERE id = $1`,
		assignmentID, lastPage, totalPages)

	if err != nil {
		return fmt.Errorf("failed to save copy progress: %w", err)
	}

	return nil
}

// GetCopyProgress retrieves the copy progress for a table
func (ms *PostgresMetadataStore) GetCopyProgress(ctx context.Context, assignmentID string) (*CopyProgress, error) {
	var cp CopyProgress
	var startedAt sql.NullTime

	err := ms.pool.QueryRow(ctx, `
		SELECT last_copied_page, total_pages, copy_started_at
		FROM pgflo_metadata.table_assignments
		WHERE id = $1`,
		assignmentID).Scan(&cp.LastPage, &cp.TotalPages, &startedAt)

	if err != nil {
		return nil, fmt.Errorf("failed to get copy progress: %w", err)
	}

	cp.AssignmentID = assignmentID
	if startedAt.Valid {
		cp.StartedAt = startedAt.Time
	}
	cp.LastUpdatedAt = time.Now()

	return &cp, nil
}

// MarkCopyComplete marks a table copy as completed
func (ms *PostgresMetadataStore) MarkCopyComplete(ctx context.Context, assignmentID string) error {
	_, err := ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.table_assignments
		SET copy_completed_at = NOW()
		WHERE id = $1`,
		assignmentID)

	if err != nil {
		return fmt.Errorf("failed to mark copy complete: %w", err)
	}

	return nil
}

// SaveStreamingLSN saves the current streaming LSN for a group
func (ms *PostgresMetadataStore) SaveStreamingLSN(ctx context.Context, groupName string, lsn pglogrepl.LSN) error {
	_, err := ms.pool.Exec(ctx, `
		INSERT INTO pgflo_metadata.replication_state (group_name, last_lsn, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (group_name) DO UPDATE SET
			last_lsn = EXCLUDED.last_lsn,
			updated_at = EXCLUDED.updated_at`,
		groupName, lsn.String())

	if err != nil {
		return fmt.Errorf("failed to save streaming LSN: %w", err)
	}

	return nil
}

// GetStreamingLSN retrieves the current streaming LSN for a group
func (ms *PostgresMetadataStore) GetStreamingLSN(ctx context.Context, groupName string) (pglogrepl.LSN, error) {
	var lsnStr string
	err := ms.pool.QueryRow(ctx, `
		SELECT last_lsn FROM pgflo_metadata.replication_state
		WHERE group_name = $1`,
		groupName).Scan(&lsnStr)

	if err != nil {
		if err == sql.ErrNoRows {
			return pglogrepl.LSN(0), nil
		}
		return pglogrepl.LSN(0), fmt.Errorf("failed to get streaming LSN: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return pglogrepl.LSN(0), fmt.Errorf("failed to parse LSN: %w", err)
	}

	return lsn, nil
}

// RecordS3File records a new S3 file in the metadata store
func (ms *PostgresMetadataStore) RecordS3File(ctx context.Context, filePath string, txID string, tableNames []string) error {
	_, err := ms.pool.Exec(ctx, `
		INSERT INTO pgflo_metadata.s3_files (file_path, tx_id, table_names)
		VALUES ($1, $2, $3)
		ON CONFLICT (file_path) DO NOTHING`,
		filePath, txID, pq.Array(tableNames))

	if err != nil {
		return fmt.Errorf("failed to record S3 file: %w", err)
	}

	return nil
}

// MarkS3FileProcessed marks an S3 file as processed
func (ms *PostgresMetadataStore) MarkS3FileProcessed(ctx context.Context, filePath string) error {
	_, err := ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.s3_files
		SET processed_at = NOW()
		WHERE file_path = $1`,
		filePath)

	if err != nil {
		return fmt.Errorf("failed to mark S3 file processed: %w", err)
	}

	return nil
}

// GetLastLSN retrieves the last LSN for a group
func (ms *PostgresMetadataStore) GetLastLSN(ctx context.Context, groupName string) (pglogrepl.LSN, error) {
	return ms.GetStreamingLSN(ctx, groupName)
}

// UpdateStreamingLSN updates the streaming LSN for a group
func (ms *PostgresMetadataStore) UpdateStreamingLSN(ctx context.Context, groupName string, lsn pglogrepl.LSN) error {
	_, err := ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.replication_state
		SET last_lsn = $2, updated_at = NOW()
		WHERE group_name = $1`,
		groupName, lsn.String())

	if err != nil {
		return fmt.Errorf("failed to update streaming LSN for group %s: %w", groupName, err)
	}

	return nil
}

// SaveCopySnapshot saves snapshot information for copy phase
func (ms *PostgresMetadataStore) SaveCopySnapshot(ctx context.Context, groupName, snapshotName, snapshotLSN string) error {
	_, err := ms.pool.Exec(ctx, `
		INSERT INTO pgflo_metadata.copy_snapshots (group_name, snapshot_name, snapshot_lsn)
		VALUES ($1, $2, $3)
		ON CONFLICT (group_name) DO UPDATE SET
			snapshot_name = EXCLUDED.snapshot_name,
			snapshot_lsn = EXCLUDED.snapshot_lsn,
			exported_at = NOW(),
			handoff_completed = FALSE`,
		groupName, snapshotName, snapshotLSN)

	if err != nil {
		return fmt.Errorf("failed to save copy snapshot: %w", err)
	}
	return nil
}

// GetCopySnapshot retrieves snapshot information for copy phase
func (ms *PostgresMetadataStore) GetCopySnapshot(ctx context.Context, groupName string) (string, string, error) {
	var snapshotName, snapshotLSN string
	err := ms.pool.QueryRow(ctx, `
		SELECT snapshot_name, snapshot_lsn FROM pgflo_metadata.copy_snapshots
		WHERE group_name = $1`,
		groupName).Scan(&snapshotName, &snapshotLSN)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", "", nil
		}
		return "", "", fmt.Errorf("failed to get copy snapshot: %w", err)
	}
	return snapshotName, snapshotLSN, nil
}

// SaveCTIDCopyProgress saves CTID-based copy progress for a table
func (ms *PostgresMetadataStore) SaveCTIDCopyProgress(ctx context.Context, groupName, tableName, lastCTID string, bytesWritten int64, fileCount int, status string) error {
	_, err := ms.pool.Exec(ctx, `
		INSERT INTO pgflo_metadata.copy_progress
		(group_name, table_name, last_ctid, bytes_written, file_count, status, started_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		ON CONFLICT (group_name, table_name) DO UPDATE SET
			last_ctid = EXCLUDED.last_ctid,
			bytes_written = EXCLUDED.bytes_written,
			file_count = EXCLUDED.file_count,
			status = EXCLUDED.status,
			completed_at = CASE WHEN EXCLUDED.status = 'COMPLETED' THEN NOW() ELSE copy_progress.completed_at END,
			updated_at = NOW()`,
		groupName, tableName, lastCTID, bytesWritten, fileCount, status)

	if err != nil {
		return fmt.Errorf("failed to save copy progress: %w", err)
	}
	return nil
}

// GetCTIDCopyProgress retrieves CTID-based copy progress for a table
func (ms *PostgresMetadataStore) GetCTIDCopyProgress(ctx context.Context, groupName, tableName string) (string, int64, int, string, error) {
	var lastCTID sql.NullString
	var bytesWritten int64
	var fileCount int
	var status string

	err := ms.pool.QueryRow(ctx, `
		SELECT last_ctid, bytes_written, file_count, status
		FROM pgflo_metadata.copy_progress
		WHERE group_name = $1 AND table_name = $2`,
		groupName, tableName).Scan(&lastCTID, &bytesWritten, &fileCount, &status)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", 0, 0, "NOT_STARTED", nil
		}
		return "", 0, 0, "", fmt.Errorf("failed to get copy progress: %w", err)
	}

	ctidStr := ""
	if lastCTID.Valid {
		ctidStr = lastCTID.String
	}

	return ctidStr, bytesWritten, fileCount, status, nil
}

// MarkCopyCompleted marks copy as completed for handoff to streaming
func (ms *PostgresMetadataStore) MarkCopyCompleted(ctx context.Context, groupName string) error {
	_, err := ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.replication_state
		SET copy_completed = TRUE, updated_at = NOW()
		WHERE group_name = $1`,
		groupName)

	if err != nil {
		return fmt.Errorf("failed to mark copy completed: %w", err)
	}

	_, err = ms.pool.Exec(ctx, `
		UPDATE pgflo_metadata.copy_snapshots
		SET handoff_completed = TRUE
		WHERE group_name = $1`,
		groupName)

	if err != nil {
		return fmt.Errorf("failed to mark snapshot handoff completed: %w", err)
	}

	return nil
}
