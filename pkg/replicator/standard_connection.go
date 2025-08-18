package replicator

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// StandardConnectionImpl implements the StandardConnection interface for PostgreSQL databases.
type StandardConnectionImpl struct {
	pool *pgxpool.Pool
}

// NewStandardConnection creates a new StandardConnectionImpl instance and establishes a connection.
func NewStandardConnection(config Config) (*StandardConnectionImpl, error) {
	connString := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		config.Host,
		config.Port,
		config.Database,
		config.User,
		config.Password)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %v", err)
	}

	poolConfig.MaxConns = 20

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %v", err)
	}
	return &StandardConnectionImpl{pool: pool}, nil
}

// Connect establishes a connection to the PostgreSQL database.
func (s *StandardConnectionImpl) Connect(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close terminates the connection to the PostgreSQL database.
func (s *StandardConnectionImpl) Close(_ context.Context) error {
	s.pool.Close()
	return nil
}

// Exec executes a SQL query without returning any rows.
func (s *StandardConnectionImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return s.pool.Exec(ctx, sql, arguments...)
}

// BeginTx starts a new transaction with the specified options.
func (s *StandardConnectionImpl) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return s.pool.BeginTx(ctx, txOptions)
}

// QueryRow executes a query that is expected to return at most one row.
func (s *StandardConnectionImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return s.pool.QueryRow(ctx, sql, args...)
}

// Query executes a query that returns rows, typically a SELECT.
func (s *StandardConnectionImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return s.pool.Query(ctx, sql, args...)
}

// Acquire acquires a connection from the pool.
func (s *StandardConnectionImpl) Acquire(ctx context.Context) (PgxPoolConn, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &PgxPoolConnWrapper{Conn: conn}, nil
}

// PgxPoolConnWrapper wraps a pgxpool connection to implement the PgxPoolConn interface
type PgxPoolConnWrapper struct {
	*pgxpool.Conn
}
