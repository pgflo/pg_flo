package utils

import (
	"encoding/json"
	"fmt"
	"time"
)

// Column represents a database column
type Column struct {
	Name     string `json:"name"`
	DataType uint32 `json:"-"`
}

// CDCMessage represents a change data capture message
type CDCMessage struct {
	Operation      OperationType          `json:"operation"`
	Schema         string                 `json:"schema"`
	Table          string                 `json:"table"`
	Columns        []Column               `json:"columns"`
	Data           map[string]interface{} `json:"data"`
	OldData        map[string]interface{} `json:"old_data,omitempty"`
	PrimaryKey     map[string]interface{} `json:"primary_key,omitempty"`
	LSN            string                 `json:"lsn"`
	EmittedAt      time.Time              `json:"emitted_at"`
	ReplicationKey *ReplicationKey        `json:"replication_key,omitempty"`
	ColumnTypes    map[string]string      `json:"column_types,omitempty"`
}

// Wal2JsonMessage represents the raw message from wal2json
type Wal2JsonMessage struct {
	Action    string        `json:"action"`
	Schema    string        `json:"schema"`
	Table     string        `json:"table"`
	Columns   []wal2JsonCol `json:"columns"`
	Identity  []wal2JsonCol `json:"identity,omitempty"`
	PK        []wal2JsonCol `json:"pk"`
	Timestamp string        `json:"timestamp"`
}

// wal2JsonCol is used only for parsing the raw wal2json output
type wal2JsonCol struct {
	Name     string      `json:"name"`
	Type     string      `json:"type"`
	Value    interface{} `json:"value"`
	Position int         `json:"position,omitempty"`
}

// Wal2JsonChange represents a single change from wal2json
type Wal2JsonChange struct {
	Kind         string                 `json:"kind"`
	Schema       string                 `json:"schema"`
	Table        string                 `json:"table"`
	ColumnNames  []string               `json:"columnnames"`
	ColumnTypes  []string               `json:"columntypes"`
	ColumnValues []interface{}          `json:"columnvalues"`
	OldKeys      map[string]interface{} `json:"oldkeys,omitempty"`
}

func (m *CDCMessage) GetColumnType(columnName string) string {
	if m.ColumnTypes != nil {
		return m.ColumnTypes[columnName]
	}
	return ""
}

// Binary marshaling for NATS compatibility
func (m CDCMessage) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m *CDCMessage) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

// GetColumnValue retrieves a column value from either current or old data
func (m *CDCMessage) GetColumnValue(columnName string, useOldValues bool) (interface{}, error) {
	if useOldValues {
		if value, ok := m.OldData[columnName]; ok {
			return value, nil
		}
		return nil, fmt.Errorf("column %s not found in old data", columnName)
	}

	if value, ok := m.Data[columnName]; ok {
		return value, nil
	}
	return nil, fmt.Errorf("column %s not found in data", columnName)
}

// SetColumnValue sets a column value in the Data map
func (m *CDCMessage) SetColumnValue(columnName string, value interface{}) error {
	if m.Data == nil {
		m.Data = make(map[string]interface{})
	}
	m.Data[columnName] = value
	return nil
}
