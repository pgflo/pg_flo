// Package utils provides common utilities and data structures for pg_flo.
package utils //nolint:revive // utils is a standard package name

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

// init registers types with the gob package for encoding/decoding
func init() {
	gob.Register(json.RawMessage{})
	gob.Register(time.Time{})
	gob.Register(map[string]interface{}{})
	gob.Register(pglogrepl.RelationMessageColumn{})
	gob.Register(pglogrepl.LSN(0))

	gob.Register(CDCMessage{})
	gob.Register(pglogrepl.TupleData{})
	gob.Register(pglogrepl.TupleDataColumn{})
	gob.Register([][]byte{})
}

// ColumnNotFoundError is returned when a requested column is not found in the CDC message
type ColumnNotFoundError struct {
	ColumnName string
}

// CDCMessage represents a full message for Change Data Capture
type CDCMessage struct {
	Type           OperationType
	Schema         string
	Table          string
	Columns        []*pglogrepl.RelationMessageColumn
	NewTuple       *pglogrepl.TupleData // For WAL messages
	OldTuple       *pglogrepl.TupleData // For WAL messages
	CopyData       [][]byte             // For COPY messages
	ReplicationKey ReplicationKey
	LSN            string
	EmittedAt      time.Time
	ToastedColumns map[string]bool
}

// GetPrimaryKeyString returns a fast string representation of the primary key for consolidation
func (m *CDCMessage) GetPrimaryKeyString() string {
	// Use replication key columns if available, otherwise use first column as fallback
	keyColumns := m.ReplicationKey.Columns
	if len(keyColumns) == 0 && len(m.Columns) > 0 {
		// Fallback to first column (often id/pk)
		keyColumns = []string{m.Columns[0].Name}
	}

	if len(keyColumns) == 0 {
		return "no-key" // Should not happen in practice
	}

	// Fast path: single column key (most common case)
	if len(keyColumns) == 1 {
		return m.getColumnDataAsString(keyColumns[0])
	}

	// Multi-column key: concatenate with separator
	var parts []string
	for _, colName := range keyColumns {
		parts = append(parts, m.getColumnDataAsString(colName))
	}
	return fmt.Sprintf("%v", parts) // Simple array representation
}

// getColumnDataAsString returns raw column data as string - fast path, no encoding
func (m *CDCMessage) getColumnDataAsString(columnName string) string {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return ""
	}

	// For CopyData (bulk copy) - direct byte access
	if m.CopyData != nil && colIndex < len(m.CopyData) {
		if m.CopyData[colIndex] == nil {
			return ""
		}
		return string(m.CopyData[colIndex]) // Direct conversion, no parsing
	}

	// For WAL messages - prefer NewTuple, fallback to OldTuple
	if m.NewTuple != nil && colIndex < len(m.NewTuple.Columns) {
		tupleCol := m.NewTuple.Columns[colIndex]
		if tupleCol == nil {
			return ""
		}
		return string(tupleCol.Data) // Direct conversion, no parsing
	}

	if m.OldTuple != nil && colIndex < len(m.OldTuple.Columns) {
		tupleCol := m.OldTuple.Columns[colIndex]
		if tupleCol == nil {
			return ""
		}
		return string(tupleCol.Data) // Direct conversion, no parsing
	}

	return ""
}

// GetColumnIndex returns the index of a column by name, or -1 if not found
func (m *CDCMessage) GetColumnIndex(columnName string) int {
	for i, col := range m.Columns {
		if col.Name == columnName {
			return i
		}
	}
	return -1
}

// GetColumnValue gets a column value, optionally using old values for DELETE/UPDATE
func (m *CDCMessage) GetColumnValue(columnName string, useOldValues bool) (interface{}, error) {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return nil, fmt.Errorf("column %s not found", columnName)
	}

	if !useOldValues && m.CopyData != nil {
		if colIndex >= len(m.CopyData) {
			return nil, fmt.Errorf("column index %d out of range for copy data", colIndex)
		}

		rawBytes := m.CopyData[colIndex]
		if rawBytes == nil {
			return nil, nil
		}

		return GlobalPostgreSQLTypeConverter.DecodePostgreSQLValue(rawBytes, m.Columns[colIndex].DataType, 1)
	}

	var data []byte
	if useOldValues && m.OldTuple != nil {
		data = m.OldTuple.Columns[colIndex].Data
	} else if m.NewTuple != nil {
		data = m.NewTuple.Columns[colIndex].Data
	} else {
		return nil, fmt.Errorf("no data available for column %s", columnName)
	}

	return GlobalPostgreSQLTypeConverter.DecodePostgreSQLValue(data, m.Columns[colIndex].DataType, 0)
}

// SetColumnValue sets the value of a column (only used by transform rules)
func (m *CDCMessage) SetColumnValue(columnName string, value interface{}) error {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return fmt.Errorf("column %s not found", columnName)
	}

	data, err := GlobalPostgreSQLTypeConverter.EncodePostgreSQLValue(value, m.Columns[colIndex].DataType)
	if err != nil {
		return fmt.Errorf("failed to encode value for column %s: %w", columnName, err)
	}

	if m.Type == OperationDelete {
		m.OldTuple.Columns[colIndex] = &pglogrepl.TupleDataColumn{Data: data}
	} else {
		m.NewTuple.Columns[colIndex] = &pglogrepl.TupleDataColumn{Data: data}
	}

	return nil
}

func (e ColumnNotFoundError) Error() string {
	return fmt.Sprintf("column %s not found", e.ColumnName)
}

// RemoveColumn removes a column from the message
func (m *CDCMessage) RemoveColumn(columnName string) error {
	colIndex := m.GetColumnIndex(columnName)
	if colIndex == -1 {
		return ColumnNotFoundError{columnName}
	}

	newColumns := make([]*pglogrepl.RelationMessageColumn, len(m.Columns))
	copy(newColumns, m.Columns)
	m.Columns = append(newColumns[:colIndex], newColumns[colIndex+1:]...)

	if m.NewTuple != nil {
		m.NewTuple.ColumnNum--
		m.NewTuple.Columns = append(m.NewTuple.Columns[:colIndex], m.NewTuple.Columns[colIndex+1:]...)
	}
	if m.OldTuple != nil {
		m.OldTuple.ColumnNum--
		m.OldTuple.Columns = append(m.OldTuple.Columns[:colIndex], m.OldTuple.Columns[colIndex+1:]...)
	}
	return nil
}

// IsColumnToasted checks if a column was TOASTed
func (m *CDCMessage) IsColumnToasted(columnName string) bool {
	return m.ToastedColumns[columnName]
}
