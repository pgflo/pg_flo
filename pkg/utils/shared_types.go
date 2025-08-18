package utils //nolint:revive // utils is a standard package name

// OperationType represents the type of database operation
type OperationType string

const (
	// OperationInsert represents an INSERT database operation
	OperationInsert OperationType = "INSERT"
	// OperationUpdate represents an UPDATE database operation
	OperationUpdate OperationType = "UPDATE"
	// OperationDelete represents a DELETE database operation
	OperationDelete OperationType = "DELETE"
	// OperationDDL represents a DDL (Data Definition Language) database operation
	OperationDDL OperationType = "DDL"
)

// ReplicationKeyType represents the type of replication key
type ReplicationKeyType string

const (
	// ReplicationKeyPK represents a primary key replication identifier
	ReplicationKeyPK ReplicationKeyType = "PRIMARY KEY"
	// ReplicationKeyUnique represents a unique constraint replication identifier
	ReplicationKeyUnique ReplicationKeyType = "UNIQUE"
	// ReplicationKeyFull represents a full table replication identifier (replica identity full)
	ReplicationKeyFull ReplicationKeyType = "FULL"
)

// ReplicationKey represents a key used for replication (either PK or unique constraint)
type ReplicationKey struct {
	Type    ReplicationKeyType
	Columns []string
}

// Logger defines the interface for logging operations
type Logger interface {
	Debug() LogEvent
	Info() LogEvent
	Warn() LogEvent
	Error() LogEvent
	Err(err error) LogEvent
}

// LogEvent defines the interface for individual log events
type LogEvent interface {
	Str(key, val string) LogEvent
	Int(key string, val int) LogEvent
	Int64(key string, val int64) LogEvent
	Uint8(key string, val uint8) LogEvent
	Uint32(key string, val uint32) LogEvent
	Interface(key string, val interface{}) LogEvent
	Err(err error) LogEvent
	Strs(key string, vals []string) LogEvent
	Any(key string, val interface{}) LogEvent
	Type(key string, val interface{}) LogEvent
	Msg(msg string)
	Msgf(format string, v ...interface{})
}
