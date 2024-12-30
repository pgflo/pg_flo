package replicator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pgflo/pg_flo/pkg/utils"
)

type DDLReplicator struct {
	DDLConn  StandardConnection
	BaseRepl *BaseReplicator
	Config   Config
}

// NewDDLReplicator creates a new DDLReplicator instance
func NewDDLReplicator(config Config, BaseRepl *BaseReplicator, ddlConn StandardConnection) (*DDLReplicator, error) {
	return &DDLReplicator{
		Config:   config,
		BaseRepl: BaseRepl,
		DDLConn:  ddlConn,
	}, nil
}

// SetupDDLTracking sets up the necessary schema, table, and triggers for DDL tracking
func (d *DDLReplicator) SetupDDLTracking(ctx context.Context) error {
	tables, err := d.BaseRepl.GetConfiguredTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to get configured tables: %w", err)
	}

	tableConditions := make([]string, len(tables))
	for i, table := range tables {
		parts := strings.Split(table, ".")
		if len(parts) != 2 {
			return fmt.Errorf("invalid table name format: %s", table)
		}
		tableConditions[i] = fmt.Sprintf("(nspname = '%s' AND relname = '%s')",
			parts[0], parts[1])
	}
	tableFilter := strings.Join(tableConditions, " OR ")

	_, err = d.DDLConn.Exec(ctx, fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS internal_pg_flo;

		CREATE TABLE IF NOT EXISTS internal_pg_flo.ddl_log (
			id SERIAL PRIMARY KEY,
			event_type TEXT NOT NULL,
			object_type TEXT,
			object_identity TEXT,
			table_name TEXT,
			ddl_command TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE OR REPLACE FUNCTION internal_pg_flo.ddl_trigger() RETURNS event_trigger AS $$
		DECLARE
			obj record;
			ddl_command text;
			table_name text;
			should_track boolean;
		BEGIN
			SELECT current_query() INTO ddl_command;

			IF TG_EVENT = 'ddl_command_end' THEN
				FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
				LOOP
					should_track := false;
					-- Extract table name if object type is table or index
					IF obj.object_type IN ('table', 'table column') THEN
						SELECT nspname || '.' || relname, (%s)
						INTO table_name, should_track
						FROM pg_class c
						JOIN pg_namespace n ON c.relnamespace = n.oid
						WHERE c.oid = obj.objid;
					ELSIF obj.object_type = 'index' THEN
						WITH target_table AS (
							SELECT t.oid as table_oid, n.nspname, t.relname
							FROM pg_index i
							JOIN pg_class t ON t.oid = i.indrelid
							JOIN pg_namespace n ON t.relnamespace = n.oid
							WHERE i.indexrelid = obj.objid
						)
						SELECT nspname || '.' || relname, (%s)
						INTO table_name, should_track
						FROM target_table;
					END IF;

					IF should_track THEN
						INSERT INTO internal_pg_flo.ddl_log (event_type, object_type, object_identity, table_name, ddl_command)
						VALUES (TG_EVENT, obj.object_type, obj.object_identity, table_name, ddl_command);
					END IF;
				END LOOP;
			END IF;
		END;
		$$ LANGUAGE plpgsql;

		DROP EVENT TRIGGER IF EXISTS pg_flo_ddl_trigger;
		CREATE EVENT TRIGGER pg_flo_ddl_trigger ON ddl_command_end
		EXECUTE FUNCTION internal_pg_flo.ddl_trigger();
	`, tableFilter, tableFilter))

	if err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Failed to setup DDL tracking")
		return err
	}
	return nil
}

// StartDDLReplication starts the DDL replication process
func (d *DDLReplicator) StartDDLReplication(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.BaseRepl.Logger.Info().Msg("DDL replication stopping...")
			return
		case <-ticker.C:
			if err := d.ProcessDDLEvents(ctx); err != nil {
				if ctx.Err() != nil {
					// Context canceled, exit gracefully
					return
				}
				d.BaseRepl.Logger.Error().Err(err).Msg("Failed to process DDL events")
			}
		}
	}
}

// ProcessDDLEvents processes DDL events from the log table
func (d *DDLReplicator) ProcessDDLEvents(ctx context.Context) error {
	rows, err := d.DDLConn.Query(ctx, `
			SELECT id, event_type, object_type, object_identity, table_name, ddl_command, created_at
			FROM internal_pg_flo.ddl_log
			ORDER BY created_at ASC
	`)
	if err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Failed to query DDL log")
		return nil
	}
	defer rows.Close()

	var processedIDs []int
	seenCommands := make(map[string]bool)

	for rows.Next() {
		var id int
		var eventType, objectType, objectIdentity, ddlCommand string
		var tableName sql.NullString
		var createdAt time.Time
		if err := rows.Scan(&id, &eventType, &objectType, &objectIdentity, &tableName, &ddlCommand, &createdAt); err != nil {
			d.BaseRepl.Logger.Error().Err(err).Msg("Failed to scan DDL log row")
			return nil
		}

		if d.shouldSkipDDLEvent(ddlCommand) {
			processedIDs = append(processedIDs, id)
			continue
		}

		if seenCommands[ddlCommand] {
			processedIDs = append(processedIDs, id)
			continue
		}
		seenCommands[ddlCommand] = true

		var schema, table string
		if tableName.Valid {
			schema, table = splitSchemaAndTable(tableName.String)
		} else {
			schema, table = "public", ""
		}

		cdcMessage := utils.CDCMessage{
			Operation: utils.OperationDDL,
			Schema:    schema,
			Table:     table,
			EmittedAt: time.Now(),
			Data: map[string]interface{}{
				"event_type":      eventType,
				"object_type":     objectType,
				"object_identity": objectIdentity,
				"ddl_command":     ddlCommand,
				"created_at":      createdAt.Format(time.RFC3339),
			},
		}

		if err := d.BaseRepl.PublishToNATS(cdcMessage); err != nil {
			d.BaseRepl.Logger.Error().Err(err).Msg("Error during publishing DDL event to NATS")
			return nil
		}

		processedIDs = append(processedIDs, id)
	}

	if err := rows.Err(); err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Error during DDL log iteration")
		return nil
	}

	if len(processedIDs) > 0 {
		_, err = d.DDLConn.Exec(ctx, "DELETE FROM internal_pg_flo.ddl_log WHERE id = ANY($1)", processedIDs)
		if err != nil {
			d.BaseRepl.Logger.Error().Err(err).Msg("Failed to clear processed DDL events")
			return nil
		}
	}

	return nil
}

// splitSchemaAndTable splits a full table name into schema and table parts
func splitSchemaAndTable(fullName string) (string, string) {
	parts := strings.SplitN(fullName, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", fullName
}

// Close closes the DDL connection
func (d *DDLReplicator) Close(ctx context.Context) error {
	if d.DDLConn != nil {
		return d.DDLConn.Close(ctx)
	}
	return nil
}

// Shutdown performs a graceful shutdown of the DDL replicator
func (d *DDLReplicator) Shutdown(ctx context.Context) error {
	d.BaseRepl.Logger.Info().Msg("Shutting down DDL replicator")

	// Process remaining events with the provided context
	if err := d.ProcessDDLEvents(ctx); err != nil {
		d.BaseRepl.Logger.Error().Err(err).Msg("Failed to process final DDL events")
		// Continue with shutdown even if processing fails
	}

	// Wait for any pending events with respect to context deadline
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.BaseRepl.Logger.Warn().Msg("Context deadline exceeded while waiting for DDL events")
			return ctx.Err()
		case <-ticker.C:
			hasEvents, err := d.HasPendingDDLEvents(ctx)
			if err != nil {
				d.BaseRepl.Logger.Error().Err(err).Msg("Failed to check pending DDL events")
				return err
			}
			if !hasEvents {
				d.BaseRepl.Logger.Info().Msg("All DDL events processed")
				return d.Close(ctx)
			}
		}
	}
}

// HasPendingDDLEvents checks if there are pending DDL events in the log
func (d *DDLReplicator) HasPendingDDLEvents(ctx context.Context) (bool, error) {
	var count int
	err := d.DDLConn.QueryRow(ctx, `
		SELECT COUNT(*) FROM internal_pg_flo.ddl_log
	`).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// shouldSkipDDLEvent determines if a DDL event should be skipped from processing
func (d *DDLReplicator) shouldSkipDDLEvent(ddlCommand string) bool {
	if strings.Contains(ddlCommand, "internal_pg_flo.") {
		return true
	}

	publicationName := GeneratePublicationName(d.Config.Group)
	if strings.Contains(ddlCommand, fmt.Sprintf("CREATE PUBLICATION %q", publicationName)) ||
		strings.Contains(ddlCommand, fmt.Sprintf("DROP PUBLICATION %q", publicationName)) ||
		strings.Contains(ddlCommand, "CREATE PUBLICATION pg_flo_") ||
		strings.Contains(ddlCommand, "DROP PUBLICATION pg_flo_") {
		return true
	}

	return false
}
