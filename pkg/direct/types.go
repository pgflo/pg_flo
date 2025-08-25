package direct

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// TableInfo represents a table configuration from the YAML config
type TableInfo struct {
	Schema         string
	Table          string
	SyncMode       string // "copy-and-stream" or "stream"
	IncludeColumns []string
	ExcludeColumns []string
}

// TableAssignment represents a table assigned to a worker
type TableAssignment struct {
	ID               string
	GroupName        string
	SchemaName       string
	TableName        string
	SyncMode         string
	IncludeColumns   []string
	ExcludeColumns   []string
	AssignedWorkerID string
	AssignedAt       time.Time
	LastHeartbeat    time.Time
	LastCopiedPage   uint32
	TotalPages       uint32
	CopyStartedAt    *time.Time
	CopyCompletedAt  *time.Time
	AddedAtLSN       pglogrepl.LSN
	LastStreamedLSN  pglogrepl.LSN
}

// CopyProgress tracks the progress of copying a table
type CopyProgress struct {
	AssignmentID  string
	LastPage      uint32
	TotalPages    uint32
	StartedAt     time.Time
	LastUpdatedAt time.Time
}

// Transaction represents a consolidated transaction ready for writing
type Transaction struct {
	ID          string
	BeginLSN    pglogrepl.LSN
	CommitLSN   pglogrepl.LSN
	Operations  []utils.CDCMessage
	TableNames  []string
	MemoryBytes int64
	CreatedAt   time.Time
}

// Config holds the configuration for direct sink replication
type Config struct {
	// Source database
	Source DatabaseConfig `yaml:"source"`

	// Metadata database (can be same as source)
	Metadata DatabaseConfig `yaml:"metadata"`

	// S3 configuration for parquet files
	S3 S3Config `yaml:"s3"`

	// Group name for this replicator instance
	Group string `yaml:"group"`

	// Schema-based table configuration
	Schemas map[string]interface{} `yaml:"schemas"`

	// Worker configuration
	MaxTablesPerWorker  int `yaml:"max_tables_per_worker"`
	MaxWorkersPerServer int `yaml:"max_workers_per_server"`

	// Transaction store configuration
	MaxMemoryBytes int64 `yaml:"max_memory_bytes"`

	// Parquet configuration
	MaxParquetFileSize int64 `yaml:"max_parquet_file_size"`
}

// DatabaseConfig represents database connection configuration
type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// S3Config represents S3 configuration for parquet storage
type S3Config struct {
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	Prefix    string `yaml:"prefix"`
	LocalPath string `yaml:"local_path"` // For local development
}

// SchemaConfig represents configuration for tables in a schema
type SchemaConfig struct {
	Tables map[string]TableConfig `yaml:"tables"`
}

// TableConfig represents configuration for a specific table
type TableConfig struct {
	SyncMode       string   `yaml:"sync_mode"` // "copy-and-stream" or "stream"
	IncludeColumns []string `yaml:"include_columns"`
	ExcludeColumns []string `yaml:"exclude_columns"`
}

// ConnectionString generates a PostgreSQL connection string
func (dc DatabaseConfig) ConnectionString() string {
	// URL encode the password to handle special characters
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dc.Host, dc.Port, dc.User, dc.Password, dc.Database)
}
