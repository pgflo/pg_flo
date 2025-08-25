package direct

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
)

const (
	// CheckpointBatchSize defines how many commits to process before saving a checkpoint
	CheckpointBatchSize = 100 // Update LSN every 100 commits
	// CheckpointInterval defines the maximum time between checkpoints
	CheckpointInterval = 5 * time.Second // Or every 5 seconds
)

// StreamReplicator handles WAL streaming with transaction boundaries
// Includes: commit lock, transaction store, consolidation, CDC metadata
type StreamReplicator struct {
	config           *Config
	replicationConn  replicator.ReplicationConnection
	standardConn     *pgx.Conn
	transactionStore TransactionStore
	consolidator     OperationConsolidator
	cdcWriter        ParquetWriter // Will be CDCParquetWriter
	metadataStore    MetadataStore
	logger           utils.Logger

	// WAL streaming state
	relations    map[uint32]*pglogrepl.RelationMessage
	lastLSN      pglogrepl.LSN
	commitLock   *pglogrepl.BeginMessage // Transaction boundary enforcement
	currentTxLSN pglogrepl.LSN

	// Batching state for anti-thrashing
	commitCount      int64
	currentBatchID   int64
	lastCheckpoint   time.Time
	lastProcessedLSN pglogrepl.LSN

	// State management
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex
}

// NewStreamReplicator creates a new stream replicator
func NewStreamReplicator(
	config *Config,
	replicationConn replicator.ReplicationConnection,
	standardConn *pgx.Conn,
	transactionStore TransactionStore,
	consolidator OperationConsolidator,
	cdcWriter ParquetWriter,
	metadataStore MetadataStore,
	logger utils.Logger,
) *StreamReplicator {
	return &StreamReplicator{
		config:           config,
		replicationConn:  replicationConn,
		standardConn:     standardConn,
		transactionStore: transactionStore,
		consolidator:     consolidator,
		cdcWriter:        cdcWriter,
		metadataStore:    metadataStore,
		logger:           logger,
		relations:        make(map[uint32]*pglogrepl.RelationMessage),
		stopChan:         make(chan struct{}),
		lastCheckpoint:   time.Now(),
		currentBatchID:   1,
	}
}

// StartFromLSN begins WAL streaming from the specified LSN
func (sr *StreamReplicator) StartFromLSN(ctx context.Context, startLSN pglogrepl.LSN) error {
	sr.mu.Lock()
	sr.lastLSN = startLSN
	sr.mu.Unlock()

	sr.logger.Info().
		Str("start_lsn", startLSN.String()).
		Msg("Starting stream replicator")

	// Connect to replication stream
	if err := sr.replicationConn.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to replication: %w", err)
	}

	// Create publication and replication slot if needed
	if err := sr.createPublicationAndSlot(ctx); err != nil {
		return fmt.Errorf("failed to create publication and slot: %w", err)
	}

	return sr.startReplicationFromLSN(ctx, startLSN)
}

// Stop gracefully stops the stream replicator
func (sr *StreamReplicator) Stop(ctx context.Context) error {
	close(sr.stopChan)
	sr.wg.Wait()

	if err := sr.cdcWriter.Close(); err != nil {
		sr.logger.Error().Err(err).Msg("Failed to close CDC writer")
	}

	// Only close if replication connection was established
	if sr.replicationConn != nil {
		// Additional safety check to avoid nil pointer dereference
		if err := func() error {
			defer func() {
				if r := recover(); r != nil {
					sr.logger.Warn().Interface("panic", r).Msg("Recovered from panic while closing replication connection")
				}
			}()
			return sr.replicationConn.Close(ctx)
		}(); err != nil {
			sr.logger.Warn().Err(err).Msg("Error closing replication connection")
		}
	}

	return nil
}

// startReplicationFromLSN starts WAL streaming
func (sr *StreamReplicator) startReplicationFromLSN(ctx context.Context, lsn pglogrepl.LSN) error {
	slotName := fmt.Sprintf("pg_flo_%s", sr.config.Group)
	publicationName := generatePublicationName(sr.config.Group)

	opts := pglogrepl.StartReplicationOptions{
		Timeline: -1,
		Mode:     pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publicationName),
		},
	}

	if err := sr.replicationConn.StartReplication(ctx, slotName, lsn, opts); err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	sr.wg.Add(1)
	go sr.streamingLoop(ctx)

	return nil
}

// streamingLoop handles incoming WAL messages
func (sr *StreamReplicator) streamingLoop(ctx context.Context) {
	defer sr.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-sr.stopChan:
			return
		default:
			if err := sr.receiveMessage(ctx); err != nil {
				sr.logger.Error().Err(err).Msg("Error receiving WAL message")
				continue
			}
		}
	}
}

// receiveMessage receives and processes a single WAL message
func (sr *StreamReplicator) receiveMessage(ctx context.Context) error {
	message, err := sr.replicationConn.ReceiveMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	switch msg := message.(type) {
	case *pgproto3.CopyData:
		return sr.handleCopyData(msg.Data)
	default:
		// Handle keepalive, etc.
		return nil
	}
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// handleCopyData processes WAL copy data messages
func (sr *StreamReplicator) handleCopyData(data []byte) error {
	// Check if this is a keepalive message first
	if len(data) == 0 {
		return nil
	}

	// Handle primary keepalive messages
	if data[0] == 'k' {
		sr.logger.Debug().Msg("Received keepalive message")
		return nil
	}

	// Handle WAL messages starting with 'w'
	if data[0] != 'w' {
		sr.logger.Debug().
			Str("message_type", string(data[0])).
			Msg("Received non-WAL message - ignored")
		return nil
	}

	// Parse logical replication message (skip the 'w' prefix and WAL position)
	if len(data) < 25 {
		sr.logger.Debug().Msg("Received short WAL message - ignored")
		return nil
	}

	logicalMsg, err := pglogrepl.Parse(data[25:]) // Skip WAL header
	if err != nil {
		// Log the error but don't fail - some messages might not be supported
		sr.logger.Debug().
			Err(err).
			Str("data_preview", fmt.Sprintf("%x", data[:min(len(data), 50)])).
			Msg("Failed to parse logical message - ignoring")
		return nil
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		return sr.handleBeginMessage(msg)
	case *pglogrepl.CommitMessage:
		return sr.handleCommitMessage(msg)
	case *pglogrepl.InsertMessage:
		return sr.handleInsertMessage(msg)
	case *pglogrepl.UpdateMessage:
		return sr.handleUpdateMessage(msg)
	case *pglogrepl.DeleteMessage:
		return sr.handleDeleteMessage(msg)
	case *pglogrepl.RelationMessage:
		return sr.handleRelationMessage(msg)
	case *pglogrepl.TypeMessage:
		// Type messages provide data type information - we can safely ignore these
		sr.logger.Debug().Msg("Received type message - ignored")
		return nil
	case *pglogrepl.OriginMessage:
		// Origin messages for replication origins - safe to ignore
		sr.logger.Debug().Msg("Received origin message - ignored")
		return nil
	case *pglogrepl.TruncateMessage:
		// Truncate operations - log but don't fail
		sr.logger.Info().Msg("Received truncate message - ignored")
		return nil
	default:
		// Log unknown message types but continue processing
		sr.logger.Warn().
			Str("message_type", fmt.Sprintf("%T", msg)).
			Msg("Received unknown message type - ignored")
		return nil
	}
}

// handleBeginMessage implements commit lock pattern
func (sr *StreamReplicator) handleBeginMessage(msg *pglogrepl.BeginMessage) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.commitLock = msg // Lock prevents mid-transaction writes
	sr.currentTxLSN = msg.FinalLSN

	sr.logger.Debug().
		Uint32("xid", msg.Xid).
		Str("final_lsn", msg.FinalLSN.String()).
		Msg("Transaction BEGIN - commit locked")

	return sr.transactionStore.Clear()
}

// handleCommitMessage processes transaction commit with consolidation
func (sr *StreamReplicator) handleCommitMessage(msg *pglogrepl.CommitMessage) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	if sr.commitLock == nil {
		sr.logger.Warn().Msg("Received COMMIT without corresponding BEGIN - skipping")
		return nil
	}

	// Get all operations from transaction store
	allMessages, err := sr.transactionStore.GetAllMessages()
	if err != nil {
		return fmt.Errorf("failed to get transaction messages: %w", err)
	}

	if len(allMessages) == 0 {
		sr.commitLock = nil
		sr.lastProcessedLSN = msg.CommitLSN
		sr.commitCount++

		// Batched checkpoint for empty transactions too
		if sr.shouldSaveCheckpoint() {
			return sr.saveStreamingCheckpoint(msg.CommitLSN)
		}
		return nil
	}

	sr.logger.Info().
		Uint32("xid", sr.commitLock.Xid).
		Str("commit_lsn", msg.CommitLSN.String()).
		Int("operations", len(allMessages)).
		Msg("Processing transaction commit with consolidation")

	// Consolidate operations (key-based deduplication)
	consolidated := sr.consolidator.ConsolidateTransaction(allMessages)

	sr.logger.Info().
		Int("original_ops", len(allMessages)).
		Int("consolidated_ops", len(consolidated)).
		Float64("reduction_ratio", float64(len(allMessages))/float64(len(consolidated))).
		Msg("Transaction consolidation completed")

	// Write consolidated operations to CDC parquet (includes metadata)
	if err := sr.cdcWriter.WriteTransaction(consolidated); err != nil {
		return fmt.Errorf("failed to write transaction to CDC parquet: %w", err)
	}

	// Update streaming state
	sr.lastLSN = msg.CommitLSN
	sr.lastProcessedLSN = msg.CommitLSN
	sr.commitCount++

	// Batched checkpoint updates (anti-thrashing)
	if sr.shouldSaveCheckpoint() {
		if err := sr.saveStreamingCheckpoint(msg.CommitLSN); err != nil {
			return fmt.Errorf("failed to save streaming checkpoint: %w", err)
		}
	}

	// Clear transaction state
	sr.commitLock = nil
	if err := sr.transactionStore.Clear(); err != nil {
		return fmt.Errorf("failed to clear transaction store: %w", err)
	}

	return nil
}

// handleInsertMessage stores INSERT operations in transaction store
func (sr *StreamReplicator) handleInsertMessage(msg *pglogrepl.InsertMessage) error {
	relation := sr.relations[msg.RelationID]
	if relation == nil {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:      utils.OperationInsert,
		Schema:    relation.Namespace,
		Table:     relation.RelationName,
		Columns:   relation.Columns,
		NewTuple:  msg.Tuple,
		LSN:       sr.currentTxLSN.String(),
		EmittedAt: sr.commitLock.CommitTime,
	}

	// Store in transaction store with key for consolidation
	key := fmt.Sprintf("%s.%s.%s", relation.Namespace, relation.RelationName, sr.extractPrimaryKey(&cdcMessage))
	return sr.transactionStore.Store(key, &cdcMessage)
}

// handleUpdateMessage stores UPDATE operations in transaction store
func (sr *StreamReplicator) handleUpdateMessage(msg *pglogrepl.UpdateMessage) error {
	relation := sr.relations[msg.RelationID]
	if relation == nil {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:      utils.OperationUpdate,
		Schema:    relation.Namespace,
		Table:     relation.RelationName,
		Columns:   relation.Columns,
		NewTuple:  msg.NewTuple,
		OldTuple:  msg.OldTuple,
		LSN:       sr.currentTxLSN.String(),
		EmittedAt: sr.commitLock.CommitTime,
	}

	// Store in transaction store with key for consolidation
	key := fmt.Sprintf("%s.%s.%s", relation.Namespace, relation.RelationName, sr.extractPrimaryKey(&cdcMessage))
	return sr.transactionStore.Store(key, &cdcMessage)
}

// handleDeleteMessage stores DELETE operations in transaction store
func (sr *StreamReplicator) handleDeleteMessage(msg *pglogrepl.DeleteMessage) error {
	relation := sr.relations[msg.RelationID]
	if relation == nil {
		return fmt.Errorf("unknown relation ID: %d", msg.RelationID)
	}

	cdcMessage := utils.CDCMessage{
		Type:      utils.OperationDelete,
		Schema:    relation.Namespace,
		Table:     relation.RelationName,
		Columns:   relation.Columns,
		OldTuple:  msg.OldTuple,
		LSN:       sr.currentTxLSN.String(),
		EmittedAt: sr.commitLock.CommitTime,
	}

	// Store in transaction store with key for consolidation
	key := fmt.Sprintf("%s.%s.%s", relation.Namespace, relation.RelationName, sr.extractPrimaryKey(&cdcMessage))
	return sr.transactionStore.Store(key, &cdcMessage)
}

// handleRelationMessage stores relation metadata
func (sr *StreamReplicator) handleRelationMessage(msg *pglogrepl.RelationMessage) error {
	sr.relations[msg.RelationID] = msg
	sr.logger.Debug().
		Uint32("relation_id", msg.RelationID).
		Str("namespace", msg.Namespace).
		Str("relation_name", msg.RelationName).
		Msg("Stored relation metadata")
	return nil
}

// extractPrimaryKey extracts primary key for consolidation
func (sr *StreamReplicator) extractPrimaryKey(msg *utils.CDCMessage) string {
	// Simplified primary key extraction - can be enhanced
	if msg.NewTuple != nil && len(msg.NewTuple.Columns) > 0 {
		if msg.NewTuple.Columns[0] != nil {
			return string(msg.NewTuple.Columns[0].Data)
		}
	}
	if msg.OldTuple != nil && len(msg.OldTuple.Columns) > 0 {
		if msg.OldTuple.Columns[0] != nil {
			return string(msg.OldTuple.Columns[0].Data)
		}
	}
	return "unknown"
}

// shouldSaveCheckpoint determines if we should save a checkpoint based on batching rules
func (sr *StreamReplicator) shouldSaveCheckpoint() bool {
	return sr.commitCount%CheckpointBatchSize == 0 ||
		time.Since(sr.lastCheckpoint) > CheckpointInterval
}

// saveStreamingCheckpoint saves LSN and batch ID with batching
func (sr *StreamReplicator) saveStreamingCheckpoint(lsn pglogrepl.LSN) error {
	ctx := context.Background()

	sr.currentBatchID++
	sr.lastCheckpoint = time.Now()

	err := sr.metadataStore.UpdateStreamingLSNWithBatch(ctx, sr.config.Group, lsn, sr.currentBatchID)
	if err != nil {
		return err
	}

	sr.logger.Info().
		Str("lsn", lsn.String()).
		Int64("batch_id", sr.currentBatchID).
		Int64("commits_processed", sr.commitCount).
		Msg("Checkpoint saved with batching")

	return nil
}

// createPublicationAndSlot creates the PostgreSQL publication and replication slot
func (sr *StreamReplicator) createPublicationAndSlot(ctx context.Context) error {
	groupName := sr.config.Group

	// Create publication first
	if err := sr.createPublication(ctx, groupName); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	// Create replication slot
	if err := sr.createReplicationSlot(ctx, groupName); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

// createPublication creates the PostgreSQL publication for the tables
func (sr *StreamReplicator) createPublication(ctx context.Context, groupName string) error {
	publicationName := generatePublicationName(groupName)

	// Check if publication already exists
	var exists bool
	query := "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)"
	if err := sr.standardConn.QueryRow(ctx, query, publicationName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	if exists {
		sr.logger.Info().Str("publication", publicationName).Msg("Publication already exists")
		return nil
	}

	// Create publication for all tables
	createQuery := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", publicationName)
	if _, err := sr.standardConn.Exec(ctx, createQuery); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	sr.logger.Info().Str("publication", publicationName).Msg("Publication created successfully")
	return nil
}

// createReplicationSlot creates the replication slot if it doesn't exist
func (sr *StreamReplicator) createReplicationSlot(ctx context.Context, groupName string) error {
	slotName := generatePublicationName(groupName)

	// Check if slot already exists
	var exists bool
	query := "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)"
	if err := sr.standardConn.QueryRow(ctx, query, slotName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check replication slot existence: %w", err)
	}

	if exists {
		sr.logger.Info().Str("slot", slotName).Msg("Replication slot already exists")
		return nil
	}

	// Create replication slot using the replication connection
	result, err := sr.replicationConn.CreateReplicationSlot(ctx, slotName)
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	sr.logger.Info().
		Str("slot", slotName).
		Str("consistent_point", result.ConsistentPoint).
		Str("snapshot_name", result.SnapshotName).
		Msg("Replication slot created successfully")

	return nil
}

// generatePublicationName generates a publication name from the group name
func generatePublicationName(groupName string) string {
	return fmt.Sprintf("pg_flo_%s", groupName)
}
