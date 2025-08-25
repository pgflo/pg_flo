package direct

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// DefaultDirectReplicator implements DirectReplicator with commit lock pattern
type DefaultDirectReplicator struct {
	config           Config
	metadataStore    MetadataStore
	transactionStore TransactionStore
	consolidator     OperationConsolidator
	parquetWriter    ParquetWriter
	logger           utils.Logger

	// Replication components
	replicationConn replicator.ReplicationConnection
	standardConn    replicator.StandardConnection

	// Worker coordination
	workerID    string
	hostname    string
	groupTables []TableInfo

	// WAL streaming state
	relations    map[uint32]*pglogrepl.RelationMessage
	lastLSN      pglogrepl.LSN
	commitLock   *pglogrepl.BeginMessage
	currentTxLSN pglogrepl.LSN

	// State management
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex
	started  bool
}

// NewDefaultDirectReplicator creates a new direct replicator
func NewDefaultDirectReplicator(config Config, logger utils.Logger) (*DefaultDirectReplicator, error) {
	// Generate worker ID
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	workerID := fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.New().String()[:8])

	// Create metadata store
	metadataStore, err := NewPostgresMetadataStore(config.Metadata.ConnectionString(), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata store: %w", err)
	}

	// Create transaction store
	diskPath := filepath.Join(os.TempDir(), fmt.Sprintf("pg_flo_direct_%s", workerID))
	transactionStore, err := NewTransactionStore(diskPath, config.MaxMemoryBytes, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction store: %w", err)
	}

	// Create consolidator
	consolidator := NewDefaultOperationConsolidator()

	// Create parquet writer
	var parquetWriter ParquetWriter
	if config.S3.LocalPath != "" {
		parquetWriter, err = NewLocalParquetWriter(config.S3.LocalPath, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create parquet writer: %w", err)
		}
	} else {
		return nil, fmt.Errorf("S3 writer not implemented yet, use local_path in config")
	}

	// Create replication connections
	replicatorConfig := replicator.Config{
		Host:     config.Source.Host,
		Port:     uint16(config.Source.Port),
		Database: config.Source.Database,
		User:     config.Source.User,
		Password: config.Source.Password,
		Group:    config.Group,
		Schema:   "public", // Default for now
	}

	replicationConn := replicator.NewReplicationConnection(replicatorConfig)
	standardConn, err := replicator.NewStandardConnection(replicatorConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create standard connection: %w", err)
	}

	return &DefaultDirectReplicator{
		config:           config,
		metadataStore:    metadataStore,
		transactionStore: transactionStore,
		consolidator:     consolidator,
		parquetWriter:    parquetWriter,
		logger:           logger,
		replicationConn:  replicationConn,
		standardConn:     standardConn,
		workerID:         workerID,
		hostname:         hostname,
		relations:        make(map[uint32]*pglogrepl.RelationMessage),
		stopChan:         make(chan struct{}),
	}, nil
}

// Bootstrap initializes the replicator and discovers tables
func (dr *DefaultDirectReplicator) Bootstrap(ctx context.Context) error {
	dr.logger.Info().Str("worker_id", dr.workerID).Msg("Starting direct replicator bootstrap")

	// Connect to metadata store
	if err := dr.metadataStore.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to metadata store: %w", err)
	}

	// Ensure metadata schema exists
	if err := dr.metadataStore.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("failed to ensure metadata schema: %w", err)
	}

	// Connect to source database
	if err := dr.replicationConn.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect replication connection: %w", err)
	}

	if err := dr.standardConn.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect standard connection: %w", err)
	}

	// FIRST: Discover tables from config (this is the key fix!)
	discoveredTables := dr.discoverTablesFromConfig()
	if len(discoveredTables) == 0 {
		return fmt.Errorf("no tables found to replicate in group %s", dr.config.Group)
	}

	dr.logger.Info().
		Int("discovered_tables", len(discoveredTables)).
		Strs("table_names", dr.getTableNamesFromInfo(discoveredTables)).
		Msg("Discovered tables from config")

	// THEN: Create publication and replication slot based on discovered tables
	if err := dr.createPublication(ctx, discoveredTables); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	if err := dr.createReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	// Store discovered tables for this group
	dr.groupTables = discoveredTables

	dr.logger.Info().
		Int("tables", len(discoveredTables)).
		Str("group", dr.config.Group).
		Msg("Bootstrap completed successfully")

	return nil
}

// Start begins replication for the group
func (dr *DefaultDirectReplicator) Start(ctx context.Context) error {
	dr.mu.Lock()
	if dr.started {
		dr.mu.Unlock()
		return fmt.Errorf("replicator already started")
	}
	dr.started = true
	dr.mu.Unlock()

	dr.logger.Info().
		Str("worker_id", dr.workerID).
		Str("group", dr.config.Group).
		Int("tables", len(dr.groupTables)).
		Msg("Starting direct replicator")

	// Get last LSN for this group
	lastLSN, err := dr.metadataStore.GetStreamingLSN(ctx, dr.config.Group)
	if err != nil {
		dr.logger.Info().Str("group", dr.config.Group).Msg("No previous LSN found for group, starting fresh replication from position 0")
		lastLSN = 0
		// Initialize replication state for new group
		if err := dr.metadataStore.SaveStreamingLSN(ctx, dr.config.Group, lastLSN); err != nil {
			dr.logger.Warn().Err(err).Msg("Failed to initialize replication state")
		}
	}
	dr.lastLSN = lastLSN

	// Check if any tables need copy phase
	copyTables := dr.getTablesForCopy()
	if len(copyTables) > 0 && lastLSN == 0 {
		dr.logger.Info().
			Int("copy_tables", len(copyTables)).
			Msg("Starting copy phase for new replication")

		if err := dr.performCopyPhase(ctx, copyTables); err != nil {
			return fmt.Errorf("copy phase failed: %w", err)
		}

		dr.logger.Info().Msg("Copy phase completed successfully")
	}

	// Start group-based replication (like regular replicator)
	dr.wg.Add(1)
	go dr.handleGroupReplication(ctx)

	// Wait for stop signal or context cancellation
	select {
	case <-ctx.Done():
		dr.logger.Info().Msg("Context cancelled, stopping replicator")
	case <-dr.stopChan:
		dr.logger.Info().Msg("Stop signal received")
	}

	return dr.shutdown(ctx)
}

// Stop gracefully stops the replicator
func (dr *DefaultDirectReplicator) Stop(ctx context.Context) error {
	dr.mu.Lock()
	if !dr.started {
		dr.mu.Unlock()
		return nil
	}
	dr.mu.Unlock()

	close(dr.stopChan)
	return dr.shutdown(ctx)
}

// discoverTablesFromConfig discovers tables based on schema configuration
func (dr *DefaultDirectReplicator) discoverTablesFromConfig() []TableInfo {
	var tables []TableInfo

	for schemaName, schemaConfig := range dr.config.Schemas {
		switch config := schemaConfig.(type) {
		case string:
			// Wildcard format: "schemas: { public: "*" }"
			if config == "*" {
				discoveredTables, err := dr.discoverAllTablesInSchema(schemaName)
				if err != nil {
					dr.logger.Error().
						Err(err).
						Str("schema", schemaName).
						Msg("Failed to discover tables in schema via wildcard")
					continue
				}

				dr.logger.Info().
					Str("schema", schemaName).
					Int("discovered_tables", len(discoveredTables)).
					Msg("Discovered tables via wildcard")

				tables = append(tables, discoveredTables...)
			}
		case map[string]interface{}, map[interface{}]interface{}:
			// Explicit table format: "schemas: { public: { tables: { users: {...} } } }"

			// Handle both map[string]interface{} and map[interface{}]interface{} from YAML
			var configMap map[string]interface{}
			switch v := config.(type) {
			case map[string]interface{}:
				configMap = v
			case map[interface{}]interface{}:
				configMap = make(map[string]interface{})
				for k, val := range v {
					if keyStr, ok := k.(string); ok {
						configMap[keyStr] = val
					}
				}
			default:
				continue
			}

			if tablesConfig, ok := configMap["tables"]; ok {
				// Handle both map types for tables config
				var tablesMap map[string]interface{}
				switch v := tablesConfig.(type) {
				case map[string]interface{}:
					tablesMap = v
				case map[interface{}]interface{}:
					tablesMap = make(map[string]interface{})
					for k, val := range v {
						if keyStr, ok := k.(string); ok {
							tablesMap[keyStr] = val
						}
					}
				default:
					continue
				}

				for tableName, tableConfigInterface := range tablesMap {
					// Parse table configuration
					syncMode := "copy-and-stream" // default
					var includeColumns, excludeColumns []string

					// Handle both map types for table config
					var tableConfigMap map[string]interface{}
					switch v := tableConfigInterface.(type) {
					case map[string]interface{}:
						tableConfigMap = v
					case map[interface{}]interface{}:
						tableConfigMap = make(map[string]interface{})
						for k, val := range v {
							if keyStr, ok := k.(string); ok {
								tableConfigMap[keyStr] = val
							}
						}
					case nil:
						// Empty table config, use defaults
						tableConfigMap = make(map[string]interface{})
					default:
						// Skip invalid config
						continue
					}

					if tableConfigMap != nil {
						if sm, ok := tableConfigMap["sync_mode"].(string); ok {
							syncMode = sm
						}
						if ic, ok := tableConfigMap["include_columns"].([]interface{}); ok {
							for _, col := range ic {
								if colStr, ok := col.(string); ok {
									includeColumns = append(includeColumns, colStr)
								}
							}
						}
						if ec, ok := tableConfigMap["exclude_columns"].([]interface{}); ok {
							for _, col := range ec {
								if colStr, ok := col.(string); ok {
									excludeColumns = append(excludeColumns, colStr)
								}
							}
						}
					}

					tables = append(tables, TableInfo{
						Schema:         schemaName,
						Table:          tableName,
						SyncMode:       syncMode,
						IncludeColumns: includeColumns,
						ExcludeColumns: excludeColumns,
					})
				}
			}
		default:
			dr.logger.Warn().
				Str("schema", schemaName).
				Str("type", fmt.Sprintf("%T", config)).
				Msg("Unknown schema configuration format")
		}
	}

	return tables
}

// discoverAllTablesInSchema queries the database to find all tables in a schema
func (dr *DefaultDirectReplicator) discoverAllTablesInSchema(schemaName string) ([]TableInfo, error) {
	rows, err := dr.standardConn.Query(context.Background(), `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1
		AND table_type = 'BASE TABLE'
		ORDER BY table_name`,
		schemaName)

	if err != nil {
		return nil, fmt.Errorf("failed to query tables in schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}

		tables = append(tables, TableInfo{
			Schema:   schemaName,
			Table:    tableName,
			SyncMode: "copy-and-stream", // Default sync mode for discovered tables
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table rows: %w", err)
	}

	return tables, nil
}

// syncTablesWithConfig adds/updates tables in metadata store and removes old ones
func (dr *DefaultDirectReplicator) syncTablesWithConfig(ctx context.Context, configTables []TableInfo) error {
	// Register/update tables from config
	if err := dr.metadataStore.RegisterTables(ctx, dr.config.Group, configTables); err != nil {
		return fmt.Errorf("failed to register tables: %w", err)
	}

	dr.logger.Info().
		Int("tables", len(configTables)).
		Str("group", dr.config.Group).
		Msg("Synced tables with config")

	return nil
}

// heartbeatLoop maintains worker heartbeat
func (dr *DefaultDirectReplicator) heartbeatLoop(ctx context.Context) {
	defer dr.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dr.stopChan:
			return
		case <-ticker.C:
			if err := dr.metadataStore.UpdateHeartbeat(ctx, dr.workerID); err != nil {
				dr.logger.Error().Err(err).Msg("Failed to update heartbeat")
			}

			// Check capacity and suggest scaling
			dr.checkCapacityAndSuggestScaling(ctx)
		}
	}
}

// checkCapacityAndSuggestScaling checks if server is at capacity
func (dr *DefaultDirectReplicator) checkCapacityAndSuggestScaling(ctx context.Context) {
	maxWorkers := dr.config.MaxWorkersPerServer
	if maxWorkers <= 0 {
		maxWorkers = 10 // Default
	}

	currentWorkers := len(dr.groupTables)
	if currentWorkers >= maxWorkers {
		dr.logger.Warn().
			Int("current_workers", currentWorkers).
			Int("max_workers", maxWorkers).
			Msg("Server at capacity - consider adding another pg_flo direct-replicator on a different server")
	}
}

// shutdown gracefully shuts down the replicator
func (dr *DefaultDirectReplicator) shutdown(ctx context.Context) error {
	dr.logger.Info().Msg("Shutting down direct replicator")

	// Wait for goroutines to finish
	done := make(chan struct{})
	go func() {
		dr.wg.Wait()
		close(done)
	}()

	// Wait for graceful shutdown or timeout
	shutdownTimeout := 30 * time.Second
	select {
	case <-done:
		dr.logger.Info().Msg("Graceful shutdown completed")
	case <-time.After(shutdownTimeout):
		dr.logger.Warn().Msg("Shutdown timeout reached")
	}

	// Close resources
	if dr.transactionStore != nil {
		_ = dr.transactionStore.Close()
	}

	if dr.parquetWriter != nil {
		_ = dr.parquetWriter.Close()
	}

	if dr.metadataStore != nil {
		_ = dr.metadataStore.Close()
	}

	if dr.replicationConn != nil {
		_ = dr.replicationConn.Close(ctx)
	}

	if dr.standardConn != nil {
		_ = dr.standardConn.Close(ctx)
	}

	return nil
}

// WAL Streaming Implementation (Commit Lock Pattern)

// startReplicationFromLSN initiates WAL streaming from the given LSN
func (dr *DefaultDirectReplicator) startReplicationFromLSN(ctx context.Context, startLSN pglogrepl.LSN) error {
	publicationName := replicator.GeneratePublicationName(dr.config.Group)
	dr.logger.Info().
		Str("startLSN", startLSN.String()).
		Str("publication", publicationName).
		Msg("Starting WAL replication")

	err := dr.replicationConn.StartReplication(ctx, publicationName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publicationName),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	dr.logger.Info().Str("startLSN", startLSN.String()).Msg("Replication started successfully")
	return dr.streamChanges(ctx)
}

// streamChanges continuously processes replication messages
func (dr *DefaultDirectReplicator) streamChanges(ctx context.Context) error {
	lastStatusUpdate := time.Now()
	standbyMessageTimeout := time.Second * 10

	for {
		select {
		case <-ctx.Done():
			dr.logger.Info().Msg("Context cancelled, stopping WAL streaming")
			return nil
		case <-dr.stopChan:
			dr.logger.Info().Msg("Stop signal received, exiting WAL streaming")
			return nil
		default:
			if err := dr.processNextMessage(ctx, &lastStatusUpdate, standbyMessageTimeout); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return err
			}
		}
	}
}

// processNextMessage processes the next WAL message
func (dr *DefaultDirectReplicator) processNextMessage(ctx context.Context, lastStatusUpdate *time.Time, standbyMessageTimeout time.Duration) error {
	msg, err := dr.replicationConn.ReceiveMessage(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}

		if dr.isConnectionError(err) {
			dr.logger.Warn().Err(err).Msg("Connection error detected, attempting recovery")
			return err
		}

		return err
	}

	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		if err := dr.handleCopyData(ctx, msg, lastStatusUpdate); err != nil {
			return err
		}
	case *pgproto3.CopyDone:
		dr.logger.Info().Msg("Received CopyDone message")
	case *pgproto3.ReadyForQuery:
		dr.logger.Info().Msg("Received ReadyForQuery message")
	case *pgproto3.ErrorResponse:
		dr.logger.Error().
			Str("severity", msg.Severity).
			Str("code", msg.Code).
			Str("message", msg.Message).
			Msg("Received ErrorResponse")
	default:
		dr.logger.Warn().
			Str("type", fmt.Sprintf("%T", msg)).
			Msg("Received unexpected message type")
	}

	// Send periodic standby status updates
	if time.Since(*lastStatusUpdate) >= standbyMessageTimeout {
		if err := dr.sendStandbyStatusUpdate(ctx); err != nil {
			return fmt.Errorf("failed to send standby status update: %w", err)
		}
		*lastStatusUpdate = time.Now()
	}

	return nil
}

// handleCopyData processes CopyData messages containing WAL data
func (dr *DefaultDirectReplicator) handleCopyData(ctx context.Context, msg *pgproto3.CopyData, lastStatusUpdate *time.Time) error {
	switch msg.Data[0] {
	case pglogrepl.XLogDataByteID:
		return dr.handleXLogData(msg.Data[1:], lastStatusUpdate)
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		return dr.handlePrimaryKeepaliveMessage(ctx, msg.Data[1:], lastStatusUpdate)
	default:
		dr.logger.Warn().Uint8("messageType", msg.Data[0]).Msg("Received unexpected CopyData message type")
	}
	return nil
}

// handleXLogData processes XLogData messages containing actual WAL records
func (dr *DefaultDirectReplicator) handleXLogData(data []byte, lastStatusUpdate *time.Time) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("failed to parse XLogData: %w", err)
	}

	if err := dr.processWALData(xld.WALData, xld.WALStart); err != nil {
		return fmt.Errorf("failed to process WAL data: %w", err)
	}

	*lastStatusUpdate = time.Now()
	return nil
}

// processWALData processes individual WAL records implementing commit lock pattern
func (dr *DefaultDirectReplicator) processWALData(walData []byte, lsn pglogrepl.LSN) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("failed to parse WAL data: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		dr.handleRelationMessage(msg)
	case *pglogrepl.BeginMessage:
		return dr.handleBeginMessage(msg)
	case *pglogrepl.InsertMessage:
		return dr.handleInsertMessage(msg, lsn)
	case *pglogrepl.UpdateMessage:
		return dr.handleUpdateMessage(msg, lsn)
	case *pglogrepl.DeleteMessage:
		return dr.handleDeleteMessage(msg, lsn)
	case *pglogrepl.CommitMessage:
		return dr.handleCommitMessage(msg)
	default:
		dr.logger.Warn().Type("message", msg).Msg("Received unexpected logical replication message")
	}

	return nil
}

// handleRelationMessage stores relation metadata for table definitions
func (dr *DefaultDirectReplicator) handleRelationMessage(msg *pglogrepl.RelationMessage) {
	dr.relations[msg.RelationID] = msg
	dr.logger.Info().
		Str("table", msg.RelationName).
		Uint32("id", msg.RelationID).
		Msg("Relation message received")
}

// handleBeginMessage handles transaction BEGIN messages
func (dr *DefaultDirectReplicator) handleBeginMessage(msg *pglogrepl.BeginMessage) error {
	dr.commitLock = msg
	dr.currentTxLSN = msg.FinalLSN
	// Only log BEGIN for transactions that actually contain operations (will be logged in COMMIT)

	// Clear transaction store for new transaction
	if err := dr.transactionStore.Clear(); err != nil {
		return fmt.Errorf("failed to clear transaction store: %w", err)
	}

	return nil
}

// handleInsertMessage stores INSERT operations in transaction store
func (dr *DefaultDirectReplicator) handleInsertMessage(msg *pglogrepl.InsertMessage, lsn pglogrepl.LSN) error {
	relation, ok := dr.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:      utils.OperationInsert,
		Schema:    relation.Namespace,
		Table:     relation.RelationName,
		Columns:   relation.Columns,
		EmittedAt: time.Now(),
		NewTuple:  msg.Tuple,
		LSN:       lsn.String(),
	}

	// Generate key for consolidation: schema.table.primary_key
	key := fmt.Sprintf("%s.%s.%s", cdcMessage.Schema, cdcMessage.Table, cdcMessage.GetPrimaryKeyString())

	return dr.transactionStore.Store(key, &cdcMessage)
}

// handleUpdateMessage stores UPDATE operations in transaction store
func (dr *DefaultDirectReplicator) handleUpdateMessage(msg *pglogrepl.UpdateMessage, lsn pglogrepl.LSN) error {
	relation, ok := dr.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:           utils.OperationUpdate,
		Schema:         relation.Namespace,
		Table:          relation.RelationName,
		Columns:        relation.Columns,
		NewTuple:       msg.NewTuple,
		OldTuple:       msg.OldTuple,
		LSN:            lsn.String(),
		EmittedAt:      time.Now(),
		ToastedColumns: make(map[string]bool),
	}

	// Handle TOAST columns
	for i, col := range relation.Columns {
		if msg.NewTuple != nil && i < len(msg.NewTuple.Columns) {
			newVal := msg.NewTuple.Columns[i]
			cdcMessage.ToastedColumns[col.Name] = newVal.DataType == 'u'
		}
	}

	// Generate key for consolidation
	key := fmt.Sprintf("%s.%s.%s", cdcMessage.Schema, cdcMessage.Table, cdcMessage.GetPrimaryKeyString())

	return dr.transactionStore.Store(key, &cdcMessage)
}

// handleDeleteMessage stores DELETE operations in transaction store
func (dr *DefaultDirectReplicator) handleDeleteMessage(msg *pglogrepl.DeleteMessage, lsn pglogrepl.LSN) error {
	relation, ok := dr.relations[msg.RelationID]
	if !ok {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:      utils.OperationDelete,
		Schema:    relation.Namespace,
		Table:     relation.RelationName,
		Columns:   relation.Columns,
		OldTuple:  msg.OldTuple,
		EmittedAt: time.Now(),
		LSN:       lsn.String(),
	}

	// Generate key for consolidation
	key := fmt.Sprintf("%s.%s.%s", cdcMessage.Schema, cdcMessage.Table, cdcMessage.GetPrimaryKeyString())

	return dr.transactionStore.Store(key, &cdcMessage)
}

// handleCommitMessage implements transaction consolidation and parquet writing
func (dr *DefaultDirectReplicator) handleCommitMessage(msg *pglogrepl.CommitMessage) error {
	if dr.commitLock == nil {
		dr.logger.Warn().Msg("Received COMMIT without corresponding BEGIN - skipping")
		return nil
	}

	// Get all messages from transaction store
	allMessages, err := dr.transactionStore.GetAllMessages()
	if err != nil {
		return fmt.Errorf("failed to get transaction messages: %w", err)
	}

	if len(allMessages) == 0 {
		dr.commitLock = nil
		return dr.saveState(msg.CommitLSN)
	}

	dr.logger.Info().
		Uint32("xid", dr.commitLock.Xid).
		Str("commit_lsn", msg.CommitLSN.String()).
		Msg("Processing transaction commit with consolidation")

	// Consolidate operations (key-based deduplication)
	consolidated := dr.consolidator.ConsolidateTransaction(allMessages)

	dr.logger.Info().
		Int("original_ops", len(allMessages)).
		Int("consolidated_ops", len(consolidated)).
		Float64("reduction_ratio", float64(len(allMessages))/float64(len(consolidated))).
		Msg("Transaction consolidation completed")

	// Write consolidated operations to Parquet
	if err := dr.parquetWriter.WriteTransaction(consolidated); err != nil {
		return fmt.Errorf("failed to write transaction to parquet: %w", err)
	}

	// Update replication state
	dr.lastLSN = msg.CommitLSN
	if err := dr.saveState(msg.CommitLSN); err != nil {
		return fmt.Errorf("failed to save replication state: %w", err)
	}

	// Clear transaction state
	dr.commitLock = nil
	if err := dr.transactionStore.Clear(); err != nil {
		return fmt.Errorf("failed to clear transaction store: %w", err)
	}

	return nil
}

// saveState saves the current LSN to metadata store
func (dr *DefaultDirectReplicator) saveState(lsn pglogrepl.LSN) error {
	ctx := context.Background()
	if err := dr.metadataStore.UpdateStreamingLSN(ctx, dr.config.Group, lsn); err != nil {
		return fmt.Errorf("failed to save LSN for group %s: %w", dr.config.Group, err)
	}
	return nil
}

// Connection management helpers
func (dr *DefaultDirectReplicator) isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "unexpected EOF") {
		return true
	}

	if strings.Contains(err.Error(), "connection") || strings.Contains(err.Error(), "closed") {
		return true
	}

	return false
}

func (dr *DefaultDirectReplicator) handlePrimaryKeepaliveMessage(ctx context.Context, data []byte, lastStatusUpdate *time.Time) error {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("failed to parse primary keepalive message: %w", err)
	}

	if pkm.ReplyRequested {
		if err := dr.sendStandbyStatusUpdate(ctx); err != nil {
			return fmt.Errorf("failed to send standby status update: %w", err)
		}
		*lastStatusUpdate = time.Now()
	}

	return nil
}

func (dr *DefaultDirectReplicator) sendStandbyStatusUpdate(ctx context.Context) error {
	standbyStatus := pglogrepl.StandbyStatusUpdate{
		WALWritePosition: dr.lastLSN,
		WALFlushPosition: dr.lastLSN,
		WALApplyPosition: dr.lastLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	}

	return dr.replicationConn.SendStandbyStatusUpdate(ctx, standbyStatus)
}

func (dr *DefaultDirectReplicator) createPublication(ctx context.Context, tables []TableInfo) error {
	publicationName := replicator.GeneratePublicationName(dr.config.Group)

	var exists bool
	err := dr.standardConn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publicationName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if publication exists: %w", err)
	}

	if exists {
		dr.logger.Info().Str("publication", publicationName).Msg("Publication already exists")
		return nil
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables found to replicate")
	}

	sanitizedTables := make([]string, len(tables))
	for i, table := range tables {
		sanitizedTables[i] = pgx.Identifier{table.Schema, table.Table}.Sanitize()
	}

	query := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
		pgx.Identifier{publicationName}.Sanitize(),
		strings.Join(sanitizedTables, ", "))

	_, err = dr.standardConn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	dr.logger.Info().
		Str("publication", publicationName).
		Int("tables", len(tables)).
		Strs("table_names", dr.getTableNamesFromInfo(tables)).
		Msg("Publication created successfully")
	return nil
}

func (dr *DefaultDirectReplicator) createReplicationSlot(ctx context.Context) error {
	publicationName := replicator.GeneratePublicationName(dr.config.Group)

	var exists bool
	err := dr.standardConn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", publicationName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check replication slot: %w", err)
	}

	if !exists {
		dr.logger.Info().Str("slot", publicationName).Msg("Creating replication slot")
		result, err := dr.replicationConn.CreateReplicationSlot(ctx, publicationName)
		if err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		dr.logger.Info().
			Str("slot", publicationName).
			Str("consistentPoint", result.ConsistentPoint).
			Str("snapshotName", result.SnapshotName).
			Msg("Replication slot created successfully")
	} else {
		dr.logger.Info().Str("slot", publicationName).Msg("Replication slot already exists")
	}

	return nil
}

func (dr *DefaultDirectReplicator) getTableNamesFromInfo(tables []TableInfo) []string {
	names := make([]string, len(tables))
	for i, table := range tables {
		names[i] = fmt.Sprintf("%s.%s", table.Schema, table.Table)
	}
	return names
}

// getTablesForCopy returns tables that need copy phase (copy-and-stream mode)
func (dr *DefaultDirectReplicator) getTablesForCopy() []TableInfo {
	var copyTables []TableInfo
	for _, table := range dr.groupTables {
		if table.SyncMode == "copy-and-stream" {
			copyTables = append(copyTables, table)
		}
	}
	return copyTables
}

// performCopyPhase performs the copy phase for specified tables
func (dr *DefaultDirectReplicator) performCopyPhase(ctx context.Context, tables []TableInfo) error {
	// Start snapshot transaction to get consistent point-in-time data
	tx, err := dr.startSnapshotTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to start snapshot transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Get snapshot info for consistent LSN tracking
	snapshotID, snapshotLSN, err := dr.getSnapshotInfo(tx)
	if err != nil {
		return fmt.Errorf("failed to get snapshot info: %w", err)
	}

	dr.logger.Info().
		Str("snapshotID", snapshotID).
		Str("snapshotLSN", snapshotLSN.String()).
		Msg("Starting copy phase with snapshot")

	// Update our LSN to the snapshot LSN
	dr.lastLSN = snapshotLSN

	// Copy all tables in parallel using the snapshot
	if err := dr.copyTablesParallel(ctx, tables, snapshotID); err != nil {
		return fmt.Errorf("failed to copy tables: %w", err)
	}

	// Commit the snapshot transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit snapshot transaction: %w", err)
	}

	// Save the snapshot LSN as our starting point for streaming
	if err := dr.metadataStore.SaveStreamingLSN(ctx, dr.config.Group, snapshotLSN); err != nil {
		return fmt.Errorf("failed to save snapshot LSN: %w", err)
	}

	return nil
}

// copyTablesParallel copies multiple tables in parallel
func (dr *DefaultDirectReplicator) copyTablesParallel(ctx context.Context, tables []TableInfo, snapshotID string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(tables))

	for _, table := range tables {
		wg.Add(1)
		go func(tbl TableInfo) {
			defer wg.Done()
			tableName := fmt.Sprintf("%s.%s", tbl.Schema, tbl.Table)
			if err := dr.copyTableWithSnapshot(ctx, tableName, snapshotID); err != nil {
				errChan <- fmt.Errorf("failed to copy table %s: %w", tableName, err)
			}
		}(table)
	}

	wg.Wait()
	close(errChan)

	// Collect any errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("copy failed for %d tables: %v", len(errors), errors[0])
	}

	return nil
}

func (dr *DefaultDirectReplicator) handleGroupReplication(ctx context.Context) {
	defer dr.wg.Done()

	dr.logger.Info().
		Str("group", dr.config.Group).
		Str("startLSN", dr.lastLSN.String()).
		Msg("Starting group-based WAL replication")

	// Start replication from last LSN
	if err := dr.startReplicationFromLSN(ctx, dr.lastLSN); err != nil {
		dr.logger.Error().Err(err).Msg("Failed to start WAL replication")
		return
	}
}

func (dr *DefaultDirectReplicator) startSnapshotTransaction(ctx context.Context) (pgx.Tx, error) {
	return dr.standardConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
}

func (dr *DefaultDirectReplicator) getSnapshotInfo(tx pgx.Tx) (string, pglogrepl.LSN, error) {
	var snapshotID string
	var startLSN pglogrepl.LSN
	err := tx.QueryRow(context.Background(), `
		SELECT pg_export_snapshot(), pg_current_wal_lsn()::text::pg_lsn
	`).Scan(&snapshotID, &startLSN)
	if err != nil {
		return "", 0, fmt.Errorf("failed to export snapshot and get LSN: %w", err)
	}
	return snapshotID, startLSN, nil
}

func (dr *DefaultDirectReplicator) copyTableWithSnapshot(ctx context.Context, tableName, snapshotID string) error {
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("invalid table name format: %s", tableName)
	}
	schemaName, tableNameOnly := parts[0], parts[1]

	relPages, err := dr.getRelPages(ctx, tableNameOnly)
	if err != nil {
		return fmt.Errorf("failed to get relPages for table %s: %w", tableName, err)
	}

	dr.logger.Info().
		Str("table", tableName).
		Uint32("relPages", relPages).
		Msg("Retrieved relPages for table")

	ranges := dr.generateRanges(relPages)

	for _, rng := range ranges {
		startPage, endPage := rng[0], rng[1]
		rowsCopied, err := dr.copyTableRange(ctx, schemaName, tableNameOnly, startPage, endPage, snapshotID)
		if err != nil {
			return fmt.Errorf("failed to copy table range [%d-%d]: %w", startPage, endPage, err)
		}

		dr.logger.Info().
			Str("table", tableName).
			Uint32("startPage", startPage).
			Uint32("endPage", endPage).
			Int64("rowsCopied", rowsCopied).
			Msg("Copied table range")
	}

	return nil
}

func (dr *DefaultDirectReplicator) getRelPages(ctx context.Context, tableName string) (uint32, error) {
	var relPages uint32
	err := dr.standardConn.QueryRow(ctx, `
		SELECT COALESCE(relpages, 1)
		FROM pg_class
		WHERE relname = $1
	`, tableName).Scan(&relPages)

	// For tables with data but relpages=0, ensure we still copy by using at least 1 page
	if relPages == 0 {
		relPages = 1
	}
	return relPages, err
}

func (dr *DefaultDirectReplicator) generateRanges(relPages uint32) [][2]uint32 {
	var ranges [][2]uint32
	batchSize := uint32(1000)
	for start := uint32(0); start < relPages; start += batchSize {
		end := start + batchSize
		if end >= relPages {
			end = ^uint32(0)
		}
		ranges = append(ranges, [2]uint32{start, end})
	}
	return ranges
}

func (dr *DefaultDirectReplicator) copyTableRange(ctx context.Context, schema, tableName string, startPage, endPage uint32, snapshotID string) (int64, error) {
	conn, err := dr.standardConn.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := dr.setTransactionSnapshot(tx, snapshotID); err != nil {
		return 0, err
	}

	query := dr.buildCopyQuery(tableName, schema, startPage, endPage)
	return dr.executeCopyQuery(ctx, tx, query, schema, tableName)
}

func (dr *DefaultDirectReplicator) setTransactionSnapshot(tx pgx.Tx, snapshotID string) error {
	_, err := tx.Exec(context.Background(), fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotID))
	if err != nil {
		return fmt.Errorf("failed to set transaction snapshot: %w", err)
	}
	return nil
}

func (dr *DefaultDirectReplicator) buildCopyQuery(tableName, schema string, startPage, endPage uint32) string {
	return fmt.Sprintf(`
		SELECT *
		FROM %s
		WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid`,
		pgx.Identifier{schema, tableName}.Sanitize(), startPage, endPage)
}

func (dr *DefaultDirectReplicator) executeCopyQuery(ctx context.Context, tx pgx.Tx, query, schema, tableName string) (int64, error) {
	dr.logger.Debug().
		Str("copyQuery", query).
		Str("table", fmt.Sprintf("%s.%s", schema, tableName)).
		Msg("Executing copy query")

	rows, err := tx.Query(context.Background(), query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute copy query: %w", err)
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	columns := make([]*pglogrepl.RelationMessageColumn, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		columns[i] = &pglogrepl.RelationMessageColumn{
			Name:     fd.Name,
			DataType: fd.DataTypeOID,
		}
	}

	var copyCount int64
	for rows.Next() {
		rawData := rows.RawValues()

		cdcMessage := utils.CDCMessage{
			Type:      utils.OperationInsert,
			Schema:    schema,
			Table:     tableName,
			Columns:   columns,
			CopyData:  rawData,
			EmittedAt: time.Now(),
		}

		key := fmt.Sprintf("%s.%s:%s", schema, tableName, string(rawData[0]))
		if err := dr.transactionStore.Store(key, &cdcMessage); err != nil {
			return 0, fmt.Errorf("failed to store copy operation: %w", err)
		}

		copyCount++

		select {
		case <-ctx.Done():
			return copyCount, ctx.Err()
		default:
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error during row iteration: %w", err)
	}

	operations, err := dr.transactionStore.GetAllMessages()
	if err != nil {
		return 0, fmt.Errorf("failed to get operations: %w", err)
	}

	consolidatedOps := dr.consolidator.ConsolidateTransaction(operations)

	if err := dr.parquetWriter.WriteTransaction(consolidatedOps); err != nil {
		return 0, fmt.Errorf("failed to write to parquet: %w", err)
	}

	if err := dr.transactionStore.Clear(); err != nil {
		return 0, fmt.Errorf("failed to clear transaction store: %w", err)
	}

	return copyCount, nil
}
