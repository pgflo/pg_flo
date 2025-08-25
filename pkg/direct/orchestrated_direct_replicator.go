package direct

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// OrchestratedDirectReplicator orchestrates the copy → stream phases cleanly
type OrchestratedDirectReplicator struct {
	config   Config
	logger   utils.Logger
	workerID string

	// Phase-specific components
	copyReplicator   *CopyReplicator
	streamReplicator *StreamReplicator

	// Shared infrastructure
	metadataStore   MetadataStore
	standardConn    *pgx.Conn
	replicationConn replicator.ReplicationConnection

	// Writers (phase-specific)
	bulkWriter *BulkParquetWriter // Copy phase
	cdcWriter  *CDCParquetWriter  // Stream phase

	// Transaction store and consolidator (stream only)
	transactionStore TransactionStore
	consolidator     OperationConsolidator
}

// NewOrchestratedDirectReplicator creates a new orchestrated direct replicator
func NewOrchestratedDirectReplicator(config Config, logger utils.Logger) (*OrchestratedDirectReplicator, error) {
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

	// Create standard connection for copy operations
	connConfig, err := pgx.ParseConfig(config.Source.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse source connection string: %w", err)
	}

	standardConn, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create standard connection: %w", err)
	}

	// Create replication connection for streaming
	var port uint16
	if config.Source.Port >= 0 && config.Source.Port <= 65535 {
		port = uint16(config.Source.Port) //nolint:gosec // Already validated bounds
	} else {
		port = 5432 // Default PostgreSQL port
	}

	replicatorConfig := replicator.Config{
		Host:     config.Source.Host,
		Port:     port,
		Database: config.Source.Database,
		User:     config.Source.User,
		Password: config.Source.Password,
		Group:    config.Group,
		Schema:   "public", // Default for now
	}
	replicationConn := replicator.NewReplicationConnection(replicatorConfig)

	// Create parquet writers
	bulkWriter, err := NewBulkParquetWriter(config.S3.LocalPath, config.MaxParquetFileSize, config.IncludePgFloMetadata, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create bulk parquet writer: %w", err)
	}

	cdcWriter, err := NewCDCParquetWriter(config.S3.LocalPath, config.MaxParquetFileSize, config.IncludePgFloMetadata, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create CDC parquet writer: %w", err)
	}

	// Create transaction store for streaming phase
	diskPath := filepath.Join(os.TempDir(), fmt.Sprintf("pg_flo_stream_%s", workerID))
	transactionStore, err := NewTransactionStore(diskPath, config.MaxMemoryBytes, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction store: %w", err)
	}

	// Create consolidator for streaming phase
	consolidator := NewDefaultOperationConsolidator()

	// Create copy replicator
	copyReplicator := NewCopyReplicator(&config, standardConn, bulkWriter, metadataStore, logger)

	// Create stream replicator
	streamReplicator := NewStreamReplicator(&config, replicationConn, standardConn, transactionStore, consolidator, cdcWriter, metadataStore, logger)

	return &OrchestratedDirectReplicator{
		config:           config,
		logger:           logger,
		workerID:         workerID,
		copyReplicator:   copyReplicator,
		streamReplicator: streamReplicator,
		metadataStore:    metadataStore,
		standardConn:     standardConn,
		replicationConn:  replicationConn,
		bulkWriter:       bulkWriter,
		cdcWriter:        cdcWriter,
		transactionStore: transactionStore,
		consolidator:     consolidator,
	}, nil
}

// Bootstrap implements DirectReplicator interface
func (odr *OrchestratedDirectReplicator) Bootstrap(ctx context.Context) error {
	odr.logger.Info().Str("worker_id", odr.workerID).Msg("Bootstrapping orchestrated direct replicator")

	// Ensure metadata schema exists
	if err := odr.metadataStore.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to metadata store: %w", err)
	}

	if err := odr.metadataStore.EnsureSchema(ctx); err != nil {
		return fmt.Errorf("failed to ensure metadata schema: %w", err)
	}

	// Discover tables from config (for logging/validation)
	tables := odr.discoverTablesFromConfig()

	odr.logger.Info().
		Int("tables_discovered", len(tables)).
		Str("group", odr.config.Group).
		Msg("Bootstrap completed successfully")

	return nil
}

// Start implements DirectReplicator interface
func (odr *OrchestratedDirectReplicator) Start(ctx context.Context) error {
	odr.logger.Info().Msg("Starting orchestrated direct replicator")

	// Get tables that need copy-and-stream mode
	tables := odr.getTablesForCopyAndStream()

	if len(tables) > 0 {
		// Phase 1: Copy phase
		odr.logger.Info().Int("tables", len(tables)).Msg("Starting copy phase")
		snapshotInfo, err := odr.copyReplicator.StartCopy(ctx, tables)
		if err != nil {
			return fmt.Errorf("copy phase failed: %w", err)
		}

		odr.logger.Info().
			Str("snapshot_lsn", snapshotInfo.LSN.String()).
			Msg("Copy phase completed - beginning stream handoff")

		// Phase 2: Stream from snapshot LSN
		odr.logger.Info().
			Str("start_lsn", snapshotInfo.LSN.String()).
			Msg("Starting stream phase from snapshot LSN")

		return odr.streamReplicator.StartFromLSN(ctx, snapshotInfo.LSN)
	}

	// Stream-only mode - start from last saved LSN
	lastLSN, err := odr.metadataStore.GetStreamingLSN(ctx, odr.config.Group)
	if err != nil {
		return fmt.Errorf("failed to get last streaming LSN: %w", err)
	}

	// If LSN is 0, this is a fresh start - get current LSN from PostgreSQL
	if lastLSN == 0 {
		odr.logger.Info().Msg("No previous LSN found, getting current LSN from PostgreSQL")
		// Get current LSN to start streaming from now
		currentLSN, err := odr.getCurrentLSN(ctx)
		if err != nil {
			return fmt.Errorf("failed to get current LSN: %w", err)
		}
		lastLSN = currentLSN
		odr.logger.Info().Str("current_lsn", lastLSN.String()).Msg("Starting from current LSN for fresh group")
	}

	odr.logger.Info().
		Str("start_lsn", lastLSN.String()).
		Msg("Starting stream-only mode from last LSN")

	return odr.streamReplicator.StartFromLSN(ctx, lastLSN)
}

// Stop implements DirectReplicator interface
func (odr *OrchestratedDirectReplicator) Stop(ctx context.Context) error {
	odr.logger.Info().Msg("Stopping orchestrated direct replicator")

	// Stop stream replicator
	if err := odr.streamReplicator.Stop(ctx); err != nil {
		odr.logger.Error().Err(err).Msg("Failed to stop stream replicator")
	}

	// Close writers
	if err := odr.bulkWriter.Close(); err != nil {
		odr.logger.Error().Err(err).Msg("Failed to close bulk writer")
	}

	if err := odr.cdcWriter.Close(); err != nil {
		odr.logger.Error().Err(err).Msg("Failed to close CDC writer")
	}

	// Close transaction store
	if err := odr.transactionStore.Close(); err != nil {
		odr.logger.Error().Err(err).Msg("Failed to close transaction store")
	}

	// Close connections
	if odr.standardConn != nil {
		if err := odr.standardConn.Close(ctx); err != nil {
			odr.logger.Error().Err(err).Msg("Failed to close standard connection")
		}
	}

	// Only close if replication connection was established
	if odr.replicationConn != nil {
		// Additional safety check to avoid nil pointer dereference
		if err := func() error {
			defer func() {
				if r := recover(); r != nil {
					odr.logger.Warn().Interface("panic", r).Msg("Recovered from panic while closing replication connection")
				}
			}()
			return odr.replicationConn.Close(ctx)
		}(); err != nil {
			odr.logger.Warn().Err(err).Msg("Error closing replication connection")
		}
	}

	// Close metadata store
	if err := odr.metadataStore.Close(); err != nil {
		odr.logger.Error().Err(err).Msg("Failed to close metadata store")
	}

	odr.logger.Info().Msg("Orchestrated direct replicator stopped successfully")
	return nil
}

// discoverTablesFromConfig discovers tables from configuration
func (odr *OrchestratedDirectReplicator) discoverTablesFromConfig() []TableInfo {
	var tables []TableInfo

	odr.logger.Debug().Interface("schemas_config", odr.config.Schemas).Msg("Discovering tables from config")

	for schemaName, schemaConfig := range odr.config.Schemas {
		odr.logger.Debug().Str("schema", schemaName).Interface("config", schemaConfig).Msg("Processing schema")

		// Convert map[interface{}]interface{} to map[string]interface{} if needed
		schemaConfigMap := odr.convertToStringMap(schemaConfig)
		if schemaConfigMap != nil {
			if tablesConfig, exists := schemaConfigMap["tables"]; exists {
				tablesMap := odr.convertToStringMap(tablesConfig)
				if tablesMap != nil {
					for tableName, tableConfig := range tablesMap {
						syncMode := "copy-and-stream" // Default
						tableConfigMap := odr.convertToStringMap(tableConfig)
						if tableConfigMap != nil {
							if mode, exists := tableConfigMap["sync_mode"]; exists {
								if modeStr, ok := mode.(string); ok {
									syncMode = modeStr
								}
							}
						}

						tables = append(tables, TableInfo{
							Schema:   schemaName,
							Table:    tableName,
							SyncMode: syncMode,
						})

						odr.logger.Debug().Str("schema", schemaName).Str("table", tableName).Str("sync_mode", syncMode).Msg("Discovered table")
					}
				} else {
					odr.logger.Warn().Str("schema", schemaName).Msg("tables config is not a map")
				}
			} else {
				odr.logger.Warn().Str("schema", schemaName).Msg("no tables config found")
			}
		} else {
			odr.logger.Warn().Str("schema", schemaName).Msg("schema config is not a map")
		}
	}

	odr.logger.Info().Int("total_tables", len(tables)).Msg("Table discovery completed")
	return tables
}

// getTablesForCopyAndStream returns tables that need copy-and-stream mode
func (odr *OrchestratedDirectReplicator) getTablesForCopyAndStream() []TableInfo {
	allTables := odr.discoverTablesFromConfig()
	var copyStreamTables []TableInfo

	for _, table := range allTables {
		if table.SyncMode == "copy-and-stream" {
			copyStreamTables = append(copyStreamTables, table)
		}
	}

	return copyStreamTables
}

// getCurrentLSN gets the current LSN from PostgreSQL
func (odr *OrchestratedDirectReplicator) getCurrentLSN(ctx context.Context) (pglogrepl.LSN, error) {
	var lsnStr string
	err := odr.standardConn.QueryRow(ctx, "SELECT pg_current_wal_lsn()").Scan(&lsnStr)
	if err != nil {
		return pglogrepl.LSN(0), fmt.Errorf("failed to get current LSN: %w", err)
	}

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return pglogrepl.LSN(0), fmt.Errorf("failed to parse current LSN: %w", err)
	}

	return lsn, nil
}

// convertToStringMap converts map[interface{}]interface{} to map[string]interface{}
func (odr *OrchestratedDirectReplicator) convertToStringMap(input interface{}) map[string]interface{} {
	switch v := input.(type) {
	case map[string]interface{}:
		return v
	case map[interface{}]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			if strKey, ok := key.(string); ok {
				result[strKey] = value
			}
		}
		return result
	default:
		return nil
	}
}
