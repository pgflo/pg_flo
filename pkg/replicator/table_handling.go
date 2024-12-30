package replicator

import (
	"context"
	"fmt"

	"github.com/pgflo/pg_flo/pkg/utils"
)

// AddPrimaryKeyInfo adds replication key information to the CDCMessage
func (r *BaseReplicator) AddPrimaryKeyInfo(message *utils.CDCMessage, table string) {
	if key, ok := r.TableReplicationKeys[table]; ok {
		replicationKey := key
		message.ReplicationKey = &replicationKey
	} else {
		r.Logger.Error().
			Str("table", table).
			Any("message", message.Data).
			Any("schema", message.Schema).
			Any("table", message.Table).
			Any("operation", message.Operation).
			Any("lsn", message.LSN).
			Any("emitted_at", message.EmittedAt).
			Msg("No replication key information found for table. This should not happen as validation is done during initialization")
	}
}

// InitializePrimaryKeyInfo initializes primary key information for all tables
func (r *BaseReplicator) InitializePrimaryKeyInfo() error {
	query := `
		WITH table_info AS (
			SELECT
				t.tablename,
				c.relreplident,
				(
					SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
					FROM pg_index i
					JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
					WHERE i.indrelid = c.oid AND i.indisprimary
				) as pk_columns,
				(
					SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
					FROM pg_index i
					JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
					WHERE i.indrelid = c.oid AND i.indisunique AND NOT i.indisprimary
					LIMIT 1
				) as unique_columns
			FROM pg_tables t
			JOIN pg_class c ON t.tablename = c.relname
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE t.schemaname = $1
		)
		SELECT
			tablename,
			relreplident::text,
			COALESCE(pk_columns, ARRAY[]::text[]) as pk_columns,
			COALESCE(unique_columns, ARRAY[]::text[]) as unique_columns
		FROM table_info;
	`

	rows, err := r.StandardConn.Query(context.Background(), query, r.Config.Schema)
	if err != nil {
		return fmt.Errorf("failed to query replication key info: %v", err)
	}
	defer rows.Close()

	r.TableReplicationKeys = make(map[string]utils.ReplicationKey)

	for rows.Next() {
		var (
			tableName       string
			replicaIdentity string
			pkColumns       []string
			uniqueColumns   []string
		)

		if err := rows.Scan(&tableName, &replicaIdentity, &pkColumns, &uniqueColumns); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		key := utils.ReplicationKey{}

		switch {
		case len(pkColumns) > 0:
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyPK,
				Columns: pkColumns,
			}
		case len(uniqueColumns) > 0:
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyUnique,
				Columns: uniqueColumns,
			}
		case replicaIdentity == "f":
			key = utils.ReplicationKey{
				Type:    utils.ReplicationKeyFull,
				Columns: nil,
			}
		}

		if err := r.validateTableReplicationKey(tableName, key); err != nil {
			r.Logger.Warn().
				Str("table", tableName).
				Str("replica_identity", replicaIdentity).
				Str("key_type", string(key.Type)).
				Strs("columns", key.Columns).
				Err(err).
				Msg("Invalid replication key configuration")
			continue
		}

		r.TableReplicationKeys[tableName] = key

		r.Logger.Debug().
			Str("table", tableName).
			Str("key_type", string(key.Type)).
			Strs("columns", key.Columns).
			Str("replica_identity", replicaIdentity).
			Msg("Initialized replication key configuration")
	}

	return rows.Err()
}

// GetConfiguredTables returns all tables based on configuration
// If no specific tables are configured, returns all tables from the configured schema
func (r *BaseReplicator) GetConfiguredTables(ctx context.Context) ([]string, error) {
	if len(r.Config.Tables) > 0 {
		fullyQualifiedTables := make([]string, len(r.Config.Tables))
		for i, table := range r.Config.Tables {
			fullyQualifiedTables[i] = fmt.Sprintf("%s.%s", r.Config.Schema, table)
		}
		return fullyQualifiedTables, nil
	}

	rows, err := r.StandardConn.Query(ctx, `
		SELECT schemaname || '.' || tablename
		FROM pg_tables
		WHERE schemaname = $1
		AND schemaname NOT IN ('pg_catalog', 'information_schema', 'internal_pg_flo')
	`, r.Config.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (r *BaseReplicator) validateTableReplicationKey(tableName string, key utils.ReplicationKey) error {
	if !key.IsValid() {
		return fmt.Errorf(
			"table %q requires one of the following:\n"+
				"\t1. A PRIMARY KEY constraint\n"+
				"\t2. A UNIQUE constraint\n"+
				"\t3. REPLICA IDENTITY FULL (ALTER TABLE %s REPLICA IDENTITY FULL)",
			tableName, tableName)
	}
	return nil
}
