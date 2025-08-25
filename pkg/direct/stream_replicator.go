package direct

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// StreamReplicator handles WAL streaming with transaction boundaries
// Includes: commit lock, transaction store, consolidation, CDC metadata
type StreamReplicator struct {
	config           *Config
	replicationConn  replicator.ReplicationConnection
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

	// State management
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex
}

// NewStreamReplicator creates a new stream replicator
func NewStreamReplicator(
	config *Config,
	replicationConn replicator.ReplicationConnection,
	transactionStore TransactionStore,
	consolidator OperationConsolidator,
	cdcWriter ParquetWriter,
	metadataStore MetadataStore,
	logger utils.Logger,
) *StreamReplicator {
	return &StreamReplicator{
		config:           config,
		replicationConn:  replicationConn,
		transactionStore: transactionStore,
		consolidator:     consolidator,
		cdcWriter:        cdcWriter,
		metadataStore:    metadataStore,
		logger:           logger,
		relations:        make(map[uint32]*pglogrepl.RelationMessage),
		stopChan:         make(chan struct{}),
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

	return sr.startReplicationFromLSN(ctx, startLSN)
}

// Stop gracefully stops the stream replicator
func (sr *StreamReplicator) Stop(ctx context.Context) error {
	close(sr.stopChan)
	sr.wg.Wait()

	if err := sr.cdcWriter.Close(); err != nil {
		sr.logger.Error().Err(err).Msg("Failed to close CDC writer")
	}

	return sr.replicationConn.Close(ctx)
}

// startReplicationFromLSN starts WAL streaming
func (sr *StreamReplicator) startReplicationFromLSN(ctx context.Context, lsn pglogrepl.LSN) error {
	slotName := fmt.Sprintf("pg_flo_%s", sr.config.Group)

	opts := pglogrepl.StartReplicationOptions{
		Timeline:   -1,
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: []string{},
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

// handleCopyData processes WAL copy data messages
func (sr *StreamReplicator) handleCopyData(data []byte) error {
	logicalMsg, err := pglogrepl.Parse(data)
	if err != nil {
		return fmt.Errorf("failed to parse logical message: %w", err)
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
	default:
		// Other message types
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
		return sr.saveStreamingState(msg.CommitLSN)
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
	if err := sr.saveStreamingState(msg.CommitLSN); err != nil {
		return fmt.Errorf("failed to save streaming state: %w", err)
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

// saveStreamingState saves current LSN to metadata store
func (sr *StreamReplicator) saveStreamingState(lsn pglogrepl.LSN) error {
	ctx := context.Background()
	return sr.metadataStore.UpdateStreamingLSN(ctx, sr.config.Group, lsn)
}
