package replicator

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// PostgresReplicationConnection implements the ReplicationConnection interface
// for PostgreSQL databases.
type PostgresReplicationConnection struct {
	Config Config
	Conn   *pgconn.PgConn
}

// NewReplicationConnection creates a new PostgresReplicationConnection instance.
func NewReplicationConnection(config Config) ReplicationConnection {
	return &PostgresReplicationConnection{
		Config: config,
	}
}

// Connect establishes a connection to the PostgreSQL database for replication.
func (rc *PostgresReplicationConnection) Connect(ctx context.Context) error {
	config, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		rc.Config.Host,
		rc.Config.Port,
		rc.Config.Database,
		rc.Config.User,
		rc.Config.Password))
	if err != nil {
		return fmt.Errorf("failed to parse connection config: %v", err)
	}

	config.RuntimeParams["replication"] = "database"

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	rc.Conn = conn.PgConn()
	return nil
}

// Close terminates the connection to the PostgreSQL database.
func (rc *PostgresReplicationConnection) Close(ctx context.Context) error {
	return rc.Conn.Close(ctx)
}

// CreateReplicationSlot creates a new replication slot in the PostgreSQL database.
func (rc *PostgresReplicationConnection) CreateReplicationSlot(ctx context.Context, slotName string) (pglogrepl.CreateReplicationSlotResult, error) {
	return pglogrepl.CreateReplicationSlot(ctx, rc.Conn, slotName, "wal2json", pglogrepl.CreateReplicationSlotOptions{
		Temporary: false,
	})
}

// StartReplication initiates the replication process from the specified LSN.
func (rc *PostgresReplicationConnection) StartReplication(ctx context.Context, slotName string, startLSN pglogrepl.LSN, options pglogrepl.StartReplicationOptions) error {
	return pglogrepl.StartReplication(ctx, rc.Conn, slotName, startLSN, options)
}

// ReceiveMessage receives a message from the PostgreSQL replication stream.
func (rc *PostgresReplicationConnection) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return rc.Conn.ReceiveMessage(ctx)
}

// SendStandbyStatusUpdate sends a status update to the PostgreSQL server during replication.
func (rc *PostgresReplicationConnection) SendStandbyStatusUpdate(ctx context.Context, status pglogrepl.StandbyStatusUpdate) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, rc.Conn, status)
}
