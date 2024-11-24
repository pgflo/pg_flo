package sinks

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgflo/pg_flo/pkg/utils"

	"github.com/rs/zerolog/log"
	sf "github.com/snowflakedb/gosnowflake"
)

type SnowflakeSink struct {
	conn          *sql.DB
	sourceConn    *pgx.Conn
	connConfig    *sf.Config
	sourceConfig  *pgx.ConnConfig
	warehouse     string
	database      string
	schema        string
	logger        utils.Logger
	retryConfig   utils.RetryConfig
	tableMetadata map[string]TableMetadata
	syncedTables  sync.Map
}

type TableMetadata struct {
	PrimaryKeys []string
	UniqueKeys  [][]string
	NotNullCols []string
}

// Constants for SQL templates
const (
	insertTemplate = "INSERT INTO %s.%s.%s (%s) SELECT %s"
	updateTemplate = "UPDATE %s.%s.%s SET %s WHERE %s"
	deleteTemplate = "DELETE FROM %s.%s.%s WHERE %s"
)

// columnExpr represents a column expression with its value
type columnExpr struct {
	name      string
	value     interface{}
	dataType  uint32
	isJSONCol bool
}

// NewSnowflakeSink creates a new Snowflake sink instance with the provided configuration
func NewSnowflakeSink(account, user, password, role, warehouse, database, schema string, sourceHost string, sourcePort int, sourceDBName, sourceUser, sourcePassword string) (*SnowflakeSink, error) {
	log.Debug().
		Str("snowflake_account", account).
		Str("database", database).
		Str("role", role).
		Msg("Initializing Snowflake sink")

	// Initialize source config first
	sourceConfig, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		sourceHost, sourcePort, sourceDBName, sourceUser, sourcePassword))
	if err != nil {
		return nil, fmt.Errorf("failed to parse source connection config: %w", err)
	}

	cfg := &sf.Config{
		Account:        account,
		User:           user,
		Password:       password,
		Role:           role,
		Database:       database,
		Schema:         schema,
		Warehouse:      warehouse,
		LoginTimeout:   1 * time.Second,
		RequestTimeout: 5 * time.Second,
	}

	sink := &SnowflakeSink{
		connConfig:   cfg,
		sourceConfig: sourceConfig,
		warehouse:    warehouse,
		database:     database,
		schema:       schema,
		logger:       utils.NewZerologLogger(log.With().Str("component", "snowflake_sink").Logger()),
		retryConfig: utils.RetryConfig{
			MaxAttempts: 5,
			InitialWait: 1 * time.Second,
			MaxWait:     30 * time.Second,
		},
		tableMetadata: make(map[string]TableMetadata),
	}

	if err := sink.connectSource(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %w", err)
	}

	if err := sink.loadTableConstraints(); err != nil {
		return nil, fmt.Errorf("failed to load table constraints: %w", err)
	}

	if err := sink.connect(context.Background()); err != nil {
		return nil, err
	}

	return sink, nil
}

// connectSource establishes a connection to the source PostgreSQL database
func (s *SnowflakeSink) connectSource(ctx context.Context) error {
	var connMutex sync.Mutex

	return utils.WithRetry(ctx, s.retryConfig, func() error {
		conn, err := pgx.ConnectConfig(ctx, s.sourceConfig)
		if err != nil {
			s.logger.Error().Err(err).Msg("Failed to connect to source database, will retry")
			return err
		}
		connMutex.Lock()
		s.sourceConn = conn
		connMutex.Unlock()
		return nil
	})
}

// connect establishes a connection to the Snowflake database
func (s *SnowflakeSink) connect(ctx context.Context) error {
	var connMutex sync.Mutex

	return utils.WithRetry(ctx, s.retryConfig, func() error {
		dsn, err := sf.DSN(s.connConfig)
		if err != nil {
			return fmt.Errorf("failed to create DSN: %w", err)
		}

		db, err := sql.Open("snowflake", dsn)
		if err != nil {
			s.logger.Error().Err(err).Msg("Failed to connect to Snowflake, will retry")
			return err
		}

		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return err
		}

		// Set the BINARY_INPUT_FORMAT session parameter
		if _, err := db.ExecContext(ctx, "ALTER SESSION SET BINARY_INPUT_FORMAT='HEX'"); err != nil {
			db.Close()
			return fmt.Errorf("failed to set binary input format: %w", err)
		}

		connMutex.Lock()
		s.conn = db
		connMutex.Unlock()
		return nil
	})
}

// isSnowflakeConnectionError checks if an error is related to connection issues
func isSnowflakeConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToUpper(err.Error())
	return strings.Contains(errStr, "CONNECTION") ||
		strings.Contains(errStr, "NETWORK") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "CLOSED") ||
		strings.Contains(errStr, "TIMEOUT")
}

// Close closes the database connections
func (s *SnowflakeSink) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

// syncSchema ensures the target table schema exists in Snowflake.
// It uses syncedTables to track which tables have already been synced.
func (s *SnowflakeSink) syncSchema(ctx context.Context, message *utils.CDCMessage) error {
	tableKey := fmt.Sprintf("%s.%s", s.schema, message.Table)
	if _, exists := s.syncedTables.Load(tableKey); exists {
		return nil
	}

	if _, loaded := s.syncedTables.LoadOrStore(tableKey, true); loaded {
		return nil
	}

	exists, err := s.tableExists(ctx, s.schema, message.Table)
	if err != nil {
		s.syncedTables.Delete(tableKey)
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		key := fmt.Sprintf("%s.%s", s.schema, message.Table)
		metadata := s.tableMetadata[key]

		columns := make([]string, 0, len(message.Columns))
		for _, col := range message.Columns {
			snowflakeType, err := mapPostgresToSnowflakeType(col.DataType)
			if err != nil {
				s.syncedTables.Delete(tableKey)
				return fmt.Errorf("failed to map type for column %s: %w", col.Name, err)
			}

			columnDef := fmt.Sprintf(`%s %s`, quoteIdentifier(col.Name), snowflakeType)
			if contains(metadata.NotNullCols, col.Name) {
				columnDef += " NOT NULL"
			}
			columns = append(columns, columnDef)
		}

		if len(metadata.PrimaryKeys) > 0 {
			pkConstraint := fmt.Sprintf("PRIMARY KEY (%s)", quotedColumns(metadata.PrimaryKeys))
			columns = append(columns, pkConstraint)
		}

		for _, uniqueKey := range metadata.UniqueKeys {
			uniqueConstraint := fmt.Sprintf("UNIQUE (%s)", quotedColumns(uniqueKey))
			columns = append(columns, uniqueConstraint)
		}

		createTableSQL := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s.%s.%s (%s)`,
			quoteIdentifier(s.database),
			quoteIdentifier(s.schema),
			quoteIdentifier(message.Table),
			strings.Join(columns, ", "),
		)

		s.logger.Debug().
			Str("sql", createTableSQL).
			Msg("Creating table")

		if _, err := s.conn.ExecContext(ctx, createTableSQL); err != nil {
			s.syncedTables.Delete(tableKey)
			s.logger.Error().
				Err(err).
				Str("sql", createTableSQL).
				Str("schema", s.schema).
				Str("table", message.Table).
				Msg("Failed to create table")
			return fmt.Errorf("failed to create table: %w", err)
		}

		s.logger.Info().
			Str("schema", s.schema).
			Str("table", message.Table).
			Msg("Successfully created table")
	}

	return nil
}

// Add a method to reset synced tables (useful for testing or manual resets)
func (s *SnowflakeSink) ResetSyncedTables() {
	s.syncedTables.Range(func(key, _ interface{}) bool {
		s.syncedTables.Delete(key)
		return true
	})
}

// Add a method to check if a table is synced (useful for testing)
func (s *SnowflakeSink) IsTableSynced(schema, table string) bool {
	tableKey := fmt.Sprintf("%s.%s", schema, table)
	_, exists := s.syncedTables.Load(tableKey)
	return exists
}

// contains checks if a string exists in a slice.
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// tableExists checks if a table exists in Snowflake.
func (s *SnowflakeSink) tableExists(ctx context.Context, schema, table string) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = ?
		AND table_name = ?
		AND table_type = 'BASE TABLE'
	`

	var count int
	err := s.conn.QueryRowContext(ctx, query, schema, table).Scan(&count)
	if err != nil {
		s.logger.Error().
			Err(err).
			Str("schema", schema).
			Str("table", table).
			Str("query", query).
			Msg("Error checking table existence")
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	exists := count > 0

	return exists, nil
}

// mapPostgresToSnowflakeType maps PostgreSQL data types to Snowflake equivalents.
func mapPostgresToSnowflakeType(pgType uint32) (string, error) {
	switch pgType {
	case pgtype.Int2OID:
		return "SMALLINT", nil
	case pgtype.Int4OID:
		return "INTEGER", nil
	case pgtype.Int8OID:
		return "BIGINT", nil
	case pgtype.Float4OID:
		return "FLOAT", nil
	case pgtype.Float8OID:
		return "DOUBLE", nil
	case pgtype.NumericOID:
		return "NUMBER", nil
	case pgtype.BoolOID:
		return "BOOLEAN", nil
	case pgtype.DateOID:
		return "DATE", nil
	case pgtype.TimestampOID:
		return "TIMESTAMP_NTZ", nil
	case pgtype.TimestamptzOID:
		return "TIMESTAMP_TZ", nil
	case pgtype.TimeOID:
		return "TIME", nil
	case pgtype.JSONOID, pgtype.JSONBOID:
		return "VARIANT", nil
	case pgtype.ByteaOID:
		return "BINARY", nil
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return "VARCHAR", nil
	default:
		return "VARCHAR", nil
	}
}

// loadTableConstraints loads table constraints from the source database.
func (s *SnowflakeSink) loadTableConstraints() error {
	ctx := context.Background()
	query := `
		SELECT
			 tc.table_schema,
			 tc.table_name,
			 kcu.column_name,
			 tc.constraint_type,
			 c.is_nullable
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
		ON tc.constraint_name = kcu.constraint_name
		AND tc.table_schema = kcu.table_schema
		AND tc.table_name = kcu.table_name
		JOIN information_schema.columns AS c
		ON c.table_schema = tc.table_schema
		AND c.table_name = tc.table_name
		AND c.column_name = kcu.column_name
		WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
	`

	rows, err := s.sourceConn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table, column, constraintType, isNullable string
		if err := rows.Scan(&schema, &table, &column, &constraintType, &isNullable); err != nil {
			return fmt.Errorf("failed to scan constraint row: %w", err)
		}

		key := fmt.Sprintf("%s.%s", schema, table)
		metadata := s.tableMetadata[key]

		switch constraintType {
		case "PRIMARY KEY":
			metadata.PrimaryKeys = append(metadata.PrimaryKeys, column)
		case "UNIQUE":
			metadata.UniqueKeys = append(metadata.UniqueKeys, []string{column})
		}

		if isNullable == "NO" {
			metadata.NotNullCols = append(metadata.NotNullCols, column)
		}

		s.tableMetadata[key] = metadata
	}

	return nil
}

// quotedColumns returns a comma-separated list of quoted column names.
func quotedColumns(columns []string) string {
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = quoteIdentifier(col)
	}
	return strings.Join(quoted, ", ")
}

// quoteIdentifier properly quotes an identifier for Snowflake.
func quoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(identifier, `"`, `""`))
}

// WriteBatch writes a batch of CDC messages to Snowflake
func (s *SnowflakeSink) WriteBatch(messages []*utils.CDCMessage) error {
	ctx := context.Background()

	if s.conn == nil {
		if err := s.connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to Snowflake: %w", err)
		}
	}

	return s.writeBatchInternal(ctx, messages)
}

// writeBatchInternal handles the internal batch writing logic
func (s *SnowflakeSink) writeBatchInternal(ctx context.Context, messages []*utils.CDCMessage) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			if err := tx.Rollback(); err != nil {
				s.logger.Error().Err(err).Msg("failed to rollback transaction")
			}
		}
	}()

	for _, message := range messages {
		var operationErr error
		err := utils.WithRetry(ctx, s.retryConfig, func() error {
			if s.conn == nil {
				if err := s.connect(ctx); err != nil {
					return fmt.Errorf("failed to reconnect to Snowflake: %w", err)
				}
				newTx, err := s.conn.BeginTx(ctx, nil)
				if err != nil {
					return fmt.Errorf("failed to begin new transaction: %w", err)
				}
				tx = newTx
			}

			// Ensure schema/table exists
			if err := s.syncSchema(ctx, message); err != nil {
				return fmt.Errorf("failed to sync schema: %w", err)
			}

			s.logger.Debug().Msgf("Handling %s for table %s.%s", message.Type, message.Schema, message.Table)

			switch message.Type {
			case utils.OperationInsert:
				operationErr = s.handleInsert(ctx, tx, message)
			case utils.OperationUpdate:
				operationErr = s.handleUpdate(ctx, tx, message)
			case utils.OperationDelete:
				operationErr = s.handleDelete(ctx, tx, message)
			case utils.OperationDDL:
				operationErr = s.handleDDL(ctx, tx, message)
			default:
				operationErr = fmt.Errorf("unknown operation type: %s", message.Type)
			}

			if operationErr != nil && isSnowflakeConnectionError(operationErr) {
				return operationErr // Retry on connection errors
			}
			return nil
		})

		if err != nil || operationErr != nil {
			if tx != nil {
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					s.logger.Error().Err(rollbackErr).Msg("failed to rollback transaction")
				}
			}
			tx = nil
			return fmt.Errorf("failed to handle %s for table %s.%s: %w-%w",
				message.Type,
				message.Schema,
				message.Table,
				err,
				operationErr,
			)
		}
	}

	s.logger.Debug().Msg("Committing transaction")
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	tx = nil // Prevent deferred rollback
	return nil
}

// buildColumnExprs creates column expressions for the given message
func (s *SnowflakeSink) buildColumnExprs(message *utils.CDCMessage, useOldValues bool) ([]columnExpr, error) {
	exprs := make([]columnExpr, 0, len(message.Columns))

	for i, col := range message.Columns {
		if message.IsColumnToasted(col.Name) {
			continue
		}

		var data []byte
		if useOldValues && message.OldTuple != nil {
			data = message.OldTuple.Columns[i].Data
		} else if message.NewTuple != nil {
			data = message.NewTuple.Columns[i].Data
		} else {
			continue
		}

		value, err := utils.DecodeValue(data, col.DataType)
		if err != nil {
			return nil, fmt.Errorf("failed to decode column value: %w", err)
		}

		sfValue, err := convertToSnowflakeValue(value, col.DataType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value for column %s: %w", col.Name, err)
		}

		exprs = append(exprs, columnExpr{
			name:      col.Name,
			value:     sfValue,
			dataType:  col.DataType,
			isJSONCol: col.DataType == pgtype.JSONOID || col.DataType == pgtype.JSONBOID,
		})
	}

	return exprs, nil
}

// buildInsertQuery constructs the INSERT query and values
func (s *SnowflakeSink) buildInsertQuery(message *utils.CDCMessage, exprs []columnExpr) (string, []interface{}, error) {
	columns := make([]string, 0, len(exprs))
	valueExprs := make([]string, 0, len(exprs))
	values := make([]interface{}, 0, len(exprs))

	for _, expr := range exprs {
		columns = append(columns, quoteIdentifier(expr.name))
		if expr.isJSONCol {
			valueExprs = append(valueExprs, "TRY_PARSE_JSON(CAST(? AS VARCHAR))")
		} else {
			valueExprs = append(valueExprs, "?")
		}
		values = append(values, expr.value)
	}

	query := fmt.Sprintf(insertTemplate,
		quoteIdentifier(s.database),
		quoteIdentifier(s.schema),
		quoteIdentifier(message.Table),
		strings.Join(columns, ", "),
		strings.Join(valueExprs, ", "),
	)

	return query, values, nil
}

// buildUpdateQuery constructs the UPDATE query and values
func (s *SnowflakeSink) buildUpdateQuery(message *utils.CDCMessage, setExprs, whereExprs []columnExpr) (string, []interface{}, error) {
	setClauses := make([]string, 0, len(setExprs))
	whereConditions := make([]string, 0, len(whereExprs))
	values := make([]interface{}, 0, len(setExprs)+len(whereExprs))

	// Build SET clause
	for _, expr := range setExprs {
		if expr.isJSONCol {
			setClauses = append(setClauses, fmt.Sprintf("%s = TRY_PARSE_JSON(CAST(? AS VARCHAR))", quoteIdentifier(expr.name)))
		} else {
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", quoteIdentifier(expr.name)))
		}
		values = append(values, expr.value)
	}

	// Build WHERE clause
	for _, expr := range whereExprs {
		if expr.value == nil {
			whereConditions = append(whereConditions, fmt.Sprintf("%s IS NULL", quoteIdentifier(expr.name)))
		} else {
			whereConditions = append(whereConditions, fmt.Sprintf("%s = ?", quoteIdentifier(expr.name)))
			values = append(values, expr.value)
		}
	}

	query := fmt.Sprintf(updateTemplate,
		quoteIdentifier(s.database),
		quoteIdentifier(s.schema),
		quoteIdentifier(message.Table),
		strings.Join(setClauses, ", "),
		strings.Join(whereConditions, " AND "),
	)

	return query, values, nil
}

// buildDeleteQuery constructs the DELETE query and values
func (s *SnowflakeSink) buildDeleteQuery(message *utils.CDCMessage, whereExprs []columnExpr) (string, []interface{}, error) {
	whereConditions := make([]string, 0, len(whereExprs))
	values := make([]interface{}, 0, len(whereExprs))

	for _, expr := range whereExprs {
		if expr.value == nil {
			whereConditions = append(whereConditions, fmt.Sprintf("%s IS NULL", quoteIdentifier(expr.name)))
		} else {
			whereConditions = append(whereConditions, fmt.Sprintf("%s = ?", quoteIdentifier(expr.name)))
			values = append(values, expr.value)
		}
	}

	query := fmt.Sprintf(deleteTemplate,
		quoteIdentifier(s.database),
		quoteIdentifier(s.schema),
		quoteIdentifier(message.Table),
		strings.Join(whereConditions, " AND "),
	)

	return query, values, nil
}

// executeQuery executes the query with proper logging and error handling
func (s *SnowflakeSink) executeQuery(ctx context.Context, tx *sql.Tx, query string, values []interface{}) error {
	_, err := tx.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("query failed: %w\nQuery: %s\nValues: %v", err, query, values)
	}
	return nil
}

// handleInsert processes an insert operation
func (s *SnowflakeSink) handleInsert(ctx context.Context, tx *sql.Tx, message *utils.CDCMessage) error {
	exprs, err := s.buildColumnExprs(message, false)
	if err != nil {
		return fmt.Errorf("failed to build column expressions: %w", err)
	}

	query, values, err := s.buildInsertQuery(message, exprs)
	if err != nil {
		return fmt.Errorf("failed to build insert query: %w", err)
	}

	return s.executeQuery(ctx, tx, query, values)
}

// handleUpdate processes an update operation
func (s *SnowflakeSink) handleUpdate(ctx context.Context, tx *sql.Tx, message *utils.CDCMessage) error {
	setExprs := make([]columnExpr, 0)
	for _, col := range message.Columns {
		if message.IsColumnToasted(col.Name) {
			continue
		}

		value, err := message.GetColumnValue(col.Name, false)
		if err != nil {
			return fmt.Errorf("failed to get column value: %w", err)
		}

		sfValue, err := convertToSnowflakeValue(value, col.DataType)
		if err != nil {
			return fmt.Errorf("failed to convert value for column %s: %w", col.Name, err)
		}

		setExprs = append(setExprs, columnExpr{
			name:      col.Name,
			value:     sfValue,
			dataType:  col.DataType,
			isJSONCol: col.DataType == pgtype.JSONOID || col.DataType == pgtype.JSONBOID,
		})
	}

	if len(setExprs) == 0 {
		s.logger.Debug().Msg("No columns to update, skipping")
		return nil
	}

	// For WHERE clause, only use replication key columns
	whereExprs := make([]columnExpr, 0)
	if !message.ReplicationKey.IsValid() {
		return fmt.Errorf("invalid replication key configuration")
	}

	for _, colName := range message.ReplicationKey.Columns {
		colIndex := message.GetColumnIndex(colName)
		if colIndex == -1 {
			return fmt.Errorf("replication key column %s not found", colName)
		}

		value, err := message.GetColumnValue(colName, true)
		if err != nil {
			return fmt.Errorf("failed to get column value: %w", err)
		}

		sfValue, err := convertToSnowflakeValue(value, message.Columns[colIndex].DataType)
		if err != nil {
			return fmt.Errorf("failed to convert value for column %s: %w", colName, err)
		}

		whereExprs = append(whereExprs, columnExpr{
			name:      colName,
			value:     sfValue,
			dataType:  message.Columns[colIndex].DataType,
			isJSONCol: message.Columns[colIndex].DataType == pgtype.JSONOID || message.Columns[colIndex].DataType == pgtype.JSONBOID,
		})
	}

	query, values, err := s.buildUpdateQuery(message, setExprs, whereExprs)
	if err != nil {
		return fmt.Errorf("failed to build update query: %w", err)
	}

	return s.executeQuery(ctx, tx, query, values)
}

// handleDelete processes a delete operation
func (s *SnowflakeSink) handleDelete(ctx context.Context, tx *sql.Tx, message *utils.CDCMessage) error {
	if !message.ReplicationKey.IsValid() {
		return fmt.Errorf("invalid replication key configuration")
	}

	whereExprs := make([]columnExpr, 0)
	for _, colName := range message.ReplicationKey.Columns {
		colIndex := message.GetColumnIndex(colName)
		if colIndex == -1 {
			return fmt.Errorf("replication key column %s not found", colName)
		}

		value, err := message.GetColumnValue(colName, true)
		if err != nil {
			return fmt.Errorf("failed to get column value: %w", err)
		}

		sfValue, err := convertToSnowflakeValue(value, message.Columns[colIndex].DataType)
		if err != nil {
			return fmt.Errorf("failed to convert value for column %s: %w", colName, err)
		}

		whereExprs = append(whereExprs, columnExpr{
			name:      colName,
			value:     sfValue,
			dataType:  message.Columns[colIndex].DataType,
			isJSONCol: message.Columns[colIndex].DataType == pgtype.JSONOID || message.Columns[colIndex].DataType == pgtype.JSONBOID,
		})
	}

	query, values, err := s.buildDeleteQuery(message, whereExprs)
	if err != nil {
		return fmt.Errorf("failed to build delete query: %w", err)
	}

	return s.executeQuery(ctx, tx, query, values)
}

// handleDDL processes a DDL operation
func (s *SnowflakeSink) handleDDL(ctx context.Context, tx *sql.Tx, message *utils.CDCMessage) error {
	ddlCommand, err := message.GetColumnValue("ddl_command", false)
	if err != nil {
		return fmt.Errorf("failed to get DDL command: %w", err)
	}

	ddlString, ok := ddlCommand.(string)
	if !ok {
		return fmt.Errorf("DDL command is not a string")
	}

	// Convert PostgreSQL DDL to Snowflake compatible DDL
	snowflakeDDL, err := convertToSnowflakeDDL(ddlString)
	if err != nil {
		return fmt.Errorf("failed to convert DDL: %w", err)
	}

	if snowflakeDDL == "" {
		s.logger.Debug().Msg("Skipping unsupported DDL operation")
		return nil
	}

	_, err = tx.ExecContext(ctx, snowflakeDDL)
	if err != nil {
		return fmt.Errorf("failed to execute DDL: %w", err)
	}

	return nil
}

// convertToSnowflakeDDL converts PostgreSQL DDL to Snowflake compatible DDL
func convertToSnowflakeDDL(pgDDL string) (string, error) {
	// This is a simplified conversion. You might need to add more cases
	// based on your specific DDL requirements
	pgDDL = strings.TrimSpace(strings.ToUpper(pgDDL))

	// Skip certain PostgreSQL-specific operations
	if strings.Contains(pgDDL, "CREATE INDEX") ||
		strings.Contains(pgDDL, "CREATE TRIGGER") ||
		strings.Contains(pgDDL, "ALTER TABLE ONLY") {
		return "", nil
	}

	// Convert PostgreSQL types to Snowflake types
	ddl := strings.ReplaceAll(pgDDL, "SERIAL", "INTEGER")
	ddl = strings.ReplaceAll(ddl, "BIGSERIAL", "BIGINT")
	ddl = strings.ReplaceAll(ddl, "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP_NTZ")
	ddl = strings.ReplaceAll(ddl, "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_TZ")
	ddl = strings.ReplaceAll(ddl, "CHARACTER VARYING", "VARCHAR")
	ddl = strings.ReplaceAll(ddl, "DOUBLE PRECISION", "DOUBLE")

	return ddl, nil
}

// convertToSnowflakeValue converts PostgreSQL values to Snowflake-compatible values
func convertToSnowflakeValue(value interface{}, pgType uint32) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch pgType {
	case pgtype.JSONOID, pgtype.JSONBOID:
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal JSON value: %w", err)
			}
			return string(jsonBytes), nil
		}
	case pgtype.ByteaOID:
		switch v := value.(type) {
		case []byte:
			// Since BINARY_INPUT_FORMAT is set to 'HEX', we just need to provide the hex string
			return hex.EncodeToString(v), nil
		default:
			return nil, fmt.Errorf("unexpected type for bytea: %T", value)
		}
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		switch v := value.(type) {
		case time.Time:
			return v.Format("2006-01-02 15:04:05.999999"), nil
		default:
			return value, nil
		}
	case pgtype.DateOID:
		switch v := value.(type) {
		case time.Time:
			return v.Format("2006-01-02"), nil
		default:
			return value, nil
		}
	case pgtype.BoolOID:
		switch v := value.(type) {
		case string:
			return v == "t" || v == "true", nil
		default:
			return value, nil
		}
	case pgtype.NumericOID:
		switch v := value.(type) {
		case pgtype.Numeric:
			// Convert the numeric struct to a string value
			val, err := v.Value()
			if err != nil {
				return nil, fmt.Errorf("failed to convert numeric to string: %w", err)
			}
			return val, nil
		case []uint8:
			// Handle raw bytes from Postgres logical replication
			strVal := string(v)
			if strVal == "NULL" {
				return nil, nil
			}
			return strVal, nil
		case string:
			return v, nil
		case float64:
			return strconv.FormatFloat(v, 'f', -1, 64), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		default:
			return nil, fmt.Errorf("unsupported numeric type: %T", value)
		}
	case pgtype.Int2ArrayOID, pgtype.Int4ArrayOID, pgtype.Int8ArrayOID,
		pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.Float4ArrayOID,
		pgtype.Float8ArrayOID:
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal array value: %w", err)
		}
		return string(jsonBytes), nil
	case pgtype.InetOID, pgtype.CIDROID, pgtype.MacaddrOID:
		return fmt.Sprintf("%v", value), nil
	case pgtype.UUIDOID:
		return fmt.Sprintf("%v", value), nil
	// Default case for other types
	default:
		return value, nil
	}
}
