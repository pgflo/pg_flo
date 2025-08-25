package direct

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// CopyReplicator handles CTID-based bulk data copy with snapshot consistency
type CopyReplicator struct {
	config        *Config
	conn          *pgx.Conn
	bulkWriter    *BulkParquetWriter // Use BulkParquetWriter specifically
	metadataStore MetadataStore
	logger        utils.Logger
	chunkSize     int
}

// SnapshotInfo contains snapshot export information for consistency
type SnapshotInfo struct {
	Name       string
	LSN        pglogrepl.LSN
	ExportedAt time.Time
}

// CTIDRange represents a CTID range for chunked copying
type CTIDRange struct {
	Start string // "(0,1)"
	End   string // "(100,1)"
}

// CopyStatus represents the status of copy operation
type CopyStatus string

// CopyStatus constants
const (
	CopyStatusNotStarted CopyStatus = "NOT_STARTED"
	CopyStatusInProgress CopyStatus = "IN_PROGRESS"
	CopyStatusCompleted  CopyStatus = "COMPLETED"
	CopyStatusFailed     CopyStatus = "FAILED"
)

// NewCopyReplicator creates a new CTID-based copy replicator
func NewCopyReplicator(config *Config, conn *pgx.Conn, bulkWriter *BulkParquetWriter, metadataStore MetadataStore, logger utils.Logger) *CopyReplicator {
	return &CopyReplicator{
		config:        config,
		conn:          conn,
		bulkWriter:    bulkWriter,
		metadataStore: metadataStore,
		logger:        logger,
		chunkSize:     2000,
	}
}

// StartCopy begins the copy phase with snapshot export
func (cr *CopyReplicator) StartCopy(ctx context.Context, tables []TableInfo) (*SnapshotInfo, error) {
	cr.logger.Info().Int("tables", len(tables)).Msg("Starting CTID-based copy phase")

	// Export snapshot for consistency
	snapshotInfo, err := cr.exportSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to export snapshot: %w", err)
	}

	cr.logger.Info().
		Str("snapshot", snapshotInfo.Name).
		Str("lsn", snapshotInfo.LSN.String()).
		Msg("Snapshot exported successfully")

	// Save snapshot info to metadata store
	if err := cr.metadataStore.SaveCopySnapshot(ctx, cr.config.Group, snapshotInfo.Name, snapshotInfo.LSN.String()); err != nil {
		return nil, fmt.Errorf("failed to save snapshot info: %w", err)
	}

	// Copy all tables using snapshot
	for _, table := range tables {
		if err := cr.copyTable(ctx, table, snapshotInfo); err != nil {
			return nil, fmt.Errorf("failed to copy table %s.%s: %w", table.Schema, table.Table, err)
		}
	}

	// Mark copy phase as completed for handoff to streaming
	if err := cr.metadataStore.MarkCopyCompleted(ctx, cr.config.Group); err != nil {
		return nil, fmt.Errorf("failed to mark copy completed: %w", err)
	}

	cr.logger.Info().
		Str("snapshot_lsn", snapshotInfo.LSN.String()).
		Msg("Copy phase completed successfully - ready for stream handoff")
	return snapshotInfo, nil
}

// exportSnapshot exports a PostgreSQL snapshot for consistent point-in-time copy
func (cr *CopyReplicator) exportSnapshot(ctx context.Context) (*SnapshotInfo, error) {
	// Begin transaction with repeatable read isolation
	tx, err := cr.conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to begin snapshot transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			cr.logger.Warn().Err(rollbackErr).Msg("Failed to rollback transaction")
		}
	}()

	// Export snapshot and get current LSN
	var snapshotName string
	var lsnStr string
	err = tx.QueryRow(ctx, "SELECT pg_export_snapshot(), pg_current_wal_lsn()").Scan(&snapshotName, &lsnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to export snapshot: %w", err)
	}

	// Parse LSN
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LSN: %w", err)
	}

	// Commit the transaction to maintain snapshot
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit snapshot transaction: %w", err)
	}

	return &SnapshotInfo{
		Name:       snapshotName,
		LSN:        lsn,
		ExportedAt: time.Now(),
	}, nil
}

// copyTable copies a single table using CTID-based chunking
func (cr *CopyReplicator) copyTable(ctx context.Context, table TableInfo, snapshot *SnapshotInfo) error {
	tableName := fmt.Sprintf("%s.%s", table.Schema, table.Table)
	cr.logger.Info().Str("table", tableName).Msg("Starting table copy")

	// Check if copy already completed
	lastCTID, bytesWritten, fileCount, status, err := cr.metadataStore.GetCTIDCopyProgress(ctx, cr.config.Group, tableName)
	if err != nil {
		return fmt.Errorf("failed to get copy progress: %w", err)
	}

	if status == string(CopyStatusCompleted) {
		cr.logger.Info().Str("table", tableName).Msg("Table copy already completed")
		return nil
	}

	// Mark copy as in progress
	if err := cr.metadataStore.SaveCTIDCopyProgress(ctx, cr.config.Group, tableName, lastCTID, bytesWritten, fileCount, string(CopyStatusInProgress)); err != nil {
		return fmt.Errorf("failed to save copy progress: %w", err)
	}

	// Get table page count for CTID range calculation
	totalPages, err := cr.getTablePages(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to get table pages: %w", err)
	}

	cr.logger.Info().
		Str("table", tableName).
		Uint32("totalPages", totalPages).
		Str("resumeFromCTID", lastCTID).
		Msg("Starting CTID-based copy")

	// Generate CTID ranges for chunking
	ranges := cr.generateCTIDRanges(totalPages, lastCTID)

	// Copy each range
	totalRowsCopied := int64(0)
	for _, ctidRange := range ranges {
		rowsCopied, newBytesWritten, newFileCount, err := cr.copyTableRange(ctx, table, ctidRange, snapshot)
		if err != nil {
			// Save failed status
			if saveErr := cr.metadataStore.SaveCTIDCopyProgress(ctx, cr.config.Group, tableName, ctidRange.Start, bytesWritten, fileCount, string(CopyStatusFailed)); saveErr != nil {
				cr.logger.Warn().Err(saveErr).Msg("Failed to save failed copy progress")
			}
			return fmt.Errorf("failed to copy range %s-%s: %w", ctidRange.Start, ctidRange.End, err)
		}

		totalRowsCopied += rowsCopied
		bytesWritten += newBytesWritten
		fileCount += newFileCount

		// Save progress after each successful range
		if err := cr.metadataStore.SaveCTIDCopyProgress(ctx, cr.config.Group, tableName, ctidRange.End, bytesWritten, fileCount, string(CopyStatusInProgress)); err != nil {
			return fmt.Errorf("failed to save copy progress: %w", err)
		}

		cr.logger.Debug().
			Str("table", tableName).
			Str("range", fmt.Sprintf("%s-%s", ctidRange.Start, ctidRange.End)).
			Int64("rowsCopied", rowsCopied).
			Int64("totalRows", totalRowsCopied).
			Msg("CTID range copied")
	}

	// Mark copy as completed
	if err := cr.metadataStore.SaveCTIDCopyProgress(ctx, cr.config.Group, tableName, "", bytesWritten, fileCount, string(CopyStatusCompleted)); err != nil {
		return fmt.Errorf("failed to mark copy completed: %w", err)
	}

	cr.logger.Info().
		Str("table", tableName).
		Int64("totalRowsCopied", totalRowsCopied).
		Int64("totalBytesWritten", bytesWritten).
		Int("totalFiles", fileCount).
		Msg("Table copy completed")

	return nil
}

// getTablePages gets the number of pages in a table for CTID range calculation
func (cr *CopyReplicator) getTablePages(ctx context.Context, table TableInfo) (uint32, error) {
	var pages uint32
	query := `SELECT COALESCE(relpages, 1) FROM pg_class WHERE relname = $1 AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $2)`

	err := cr.conn.QueryRow(ctx, query, table.Table, table.Schema).Scan(&pages)
	if err != nil {
		return 0, fmt.Errorf("failed to get table pages: %w", err)
	}

	// Ensure at least 1 page to prevent empty ranges
	if pages == 0 {
		pages = 1
	}

	return pages, nil
}

// generateCTIDRanges creates CTID ranges for chunked copying
func (cr *CopyReplicator) generateCTIDRanges(totalPages uint32, resumeFromCTID string) []CTIDRange {
	var ranges []CTIDRange

	// Calculate rows per page (approximate)
	rowsPerPageVal := max(1, cr.chunkSize/10) // Conservative estimate
	if rowsPerPageVal > 4294967295 {
		rowsPerPageVal = 4294967295 // Max uint32
	}
	rowsPerPage := uint32(rowsPerPageVal) //nolint:gosec // Already validated bounds
	if rowsPerPage == 0 {
		rowsPerPage = 1
	}

	startPage := uint32(0)

	// Resume from specific CTID if provided
	if resumeFromCTID != "" {
		if page, err := cr.parseCTIDPage(resumeFromCTID); err == nil {
			startPage = page
		}
	}

	for page := startPage; page < totalPages; page += rowsPerPage {
		endPage := page + rowsPerPage
		if endPage > totalPages {
			endPage = totalPages
		}

		ranges = append(ranges, CTIDRange{
			Start: fmt.Sprintf("(%d,0)", page),
			End:   fmt.Sprintf("(%d,0)", endPage),
		})
	}

	return ranges
}

// parseCTIDPage extracts page number from CTID string like "(123,45)"
func (cr *CopyReplicator) parseCTIDPage(ctid string) (uint32, error) {
	// Remove parentheses and split by comma
	ctid = strings.Trim(ctid, "()")
	parts := strings.Split(ctid, ",")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid CTID format: %s", ctid)
	}

	page, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse page from CTID: %w", err)
	}

	return uint32(page), nil
}

// copyTableRange copies a specific CTID range from a table
func (cr *CopyReplicator) copyTableRange(ctx context.Context, table TableInfo, ctidRange CTIDRange, snapshot *SnapshotInfo) (int64, int64, int, error) {
	// Begin transaction with snapshot
	tx, err := cr.conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			cr.logger.Warn().Err(rollbackErr).Msg("Failed to rollback transaction")
		}
	}()

	// Set transaction snapshot for consistency
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT $1", snapshot.Name); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to set transaction snapshot: %w", err)
	}

	// Build CTID range query
	query := fmt.Sprintf("SELECT * FROM %s WHERE ctid >= $1::tid AND ctid < $2::tid",
		pgx.Identifier{table.Schema, table.Table}.Sanitize())

	// Execute query
	rows, err := tx.Query(ctx, query, ctidRange.Start, ctidRange.End)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to execute CTID range query: %w", err)
	}
	defer rows.Close()

	// Get column information
	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]*pglogrepl.RelationMessageColumn, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = &pglogrepl.RelationMessageColumn{
			Name:     fd.Name,
			DataType: fd.DataTypeOID,
		}
	}

	// Process rows and create CDC messages
	var operations []*utils.CDCMessage
	rowCount := int64(0)

	for rows.Next() {
		rawData := rows.RawValues()

		cdcMessage := &utils.CDCMessage{
			Type:      utils.OperationInsert, // Copy operations are always inserts
			Schema:    table.Schema,
			Table:     table.Table,
			Columns:   columns,
			CopyData:  rawData,
			EmittedAt: time.Now(),
		}

		operations = append(operations, cdcMessage)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return 0, 0, 0, fmt.Errorf("error during row iteration: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Write to parquet if we have data
	bytesWritten := int64(0)
	fileCount := 0

	if len(operations) > 0 {
		// Write using BulkParquetWriter (handles size-based rotation)
		tableName := fmt.Sprintf("%s.%s", table.Schema, table.Table)
		actualBytes, actualFiles, err := cr.bulkWriter.WriteBulkData(tableName, operations)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("failed to write to bulk parquet: %w", err)
		}

		bytesWritten = actualBytes
		fileCount = actualFiles
	}

	return rowCount, bytesWritten, fileCount, nil
}

// Resume resumes copy operation from where it left off
func (cr *CopyReplicator) Resume(ctx context.Context, tables []TableInfo) (*SnapshotInfo, error) {
	cr.logger.Info().Msg("Resuming copy operation")

	// Get existing snapshot info
	snapshotName, snapshotLSN, err := cr.metadataStore.GetCopySnapshot(ctx, cr.config.Group)
	if err != nil {
		return nil, fmt.Errorf("failed to get copy snapshot: %w", err)
	}

	if snapshotName == "" {
		cr.logger.Info().Msg("No existing snapshot found, starting fresh copy")
		return cr.StartCopy(ctx, tables)
	}

	lsn, err := pglogrepl.ParseLSN(snapshotLSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshot LSN: %w", err)
	}

	snapshotInfo := &SnapshotInfo{
		Name: snapshotName,
		LSN:  lsn,
	}

	cr.logger.Info().
		Str("snapshot", snapshotName).
		Str("lsn", snapshotLSN).
		Msg("Resuming from existing snapshot")

	// Continue copying remaining tables
	for _, table := range tables {
		if err := cr.copyTable(ctx, table, snapshotInfo); err != nil {
			return nil, fmt.Errorf("failed to resume copy for table %s.%s: %w", table.Schema, table.Table, err)
		}
	}

	return snapshotInfo, nil
}
