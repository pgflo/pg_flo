// Package direct implements high-performance PostgreSQL CDC with direct sink architecture
package direct

import (
	"fmt"

	"github.com/pgflo/pg_flo/pkg/utils"
)

// DefaultOperationConsolidator implements OperationConsolidator with key-based deduplication
type DefaultOperationConsolidator struct{}

// NewDefaultOperationConsolidator creates a new operation consolidator
func NewDefaultOperationConsolidator() *DefaultOperationConsolidator {
	return &DefaultOperationConsolidator{}
}

// ConsolidateTransaction performs key-based deduplication on CDC operations within a transaction
func (c *DefaultOperationConsolidator) ConsolidateTransaction(operations []*utils.CDCMessage) []*utils.CDCMessage {
	if len(operations) == 0 {
		return operations
	}

	// Group operations by table and primary key
	opMap := make(map[string]*utils.CDCMessage)

	for _, op := range operations {
		// Generate unique key for this record: table.schema.primarykey
		recordKey := c.generateRecordKey(op)

		// Apply consolidation logic
		if existing, exists := opMap[recordKey]; exists {
			consolidated := c.consolidateTwo(existing, op)
			if consolidated != nil {
				opMap[recordKey] = consolidated
			} else {
				// Operations canceled each other out (INSERT -> DELETE)
				delete(opMap, recordKey)
			}
		} else {
			// First operation for this key
			opMap[recordKey] = op
		}
	}

	// Convert map back to slice
	result := make([]*utils.CDCMessage, 0, len(opMap))
	for _, op := range opMap {
		result = append(result, op)
	}

	return result
}

// ConsolidateOperations performs key-based deduplication on CDC operations (legacy interface)
func (c *DefaultOperationConsolidator) ConsolidateOperations(operations []utils.CDCMessage) []utils.CDCMessage {
	if len(operations) == 0 {
		return operations
	}

	// Group operations by table and primary key
	opMap := make(map[string]*utils.CDCMessage)

	for i := range operations {
		op := &operations[i]

		// Generate unique key for this record: table.schema.primarykey
		recordKey := c.generateRecordKey(op)

		// Apply consolidation logic
		if existing, exists := opMap[recordKey]; exists {
			consolidated := c.consolidateTwo(existing, op)
			if consolidated != nil {
				opMap[recordKey] = consolidated
			} else {
				// Operations canceled each other out (INSERT -> DELETE)
				delete(opMap, recordKey)
			}
		} else {
			// First operation for this key
			opMap[recordKey] = op
		}
	}

	// Convert map back to slice, preserving some order
	result := make([]utils.CDCMessage, 0, len(opMap))
	for _, op := range opMap {
		result = append(result, *op)
	}

	return result
}

// generateRecordKey creates a unique key for a database record
func (c *DefaultOperationConsolidator) generateRecordKey(op *utils.CDCMessage) string {
	// Use schema.table.primarykey as the unique identifier
	return fmt.Sprintf("%s.%s.%s", op.Schema, op.Table, op.GetPrimaryKeyString())
}

// extractPrimaryKey extracts the primary key value(s) from a CDC message
func (c *DefaultOperationConsolidator) extractPrimaryKey(op *utils.CDCMessage) string {
	// Use the replication key to find primary key columns
	if len(op.ReplicationKey.Columns) > 0 {
		keyParts := make([]string, 0, len(op.ReplicationKey.Columns))

		for _, pkCol := range op.ReplicationKey.Columns {
			value, err := c.getColumnValue(op, pkCol)
			if err == nil && value != nil {
				keyParts = append(keyParts, fmt.Sprintf("%v", value))
			} else {
				keyParts = append(keyParts, "NULL")
			}
		}

		if len(keyParts) > 0 {
			return fmt.Sprintf("%s", keyParts)
		}
	}

	// Fallback: try common primary key column names
	primaryKeyColumns := []string{"id", "uuid", "pk"}

	for _, pkCol := range primaryKeyColumns {
		value, err := c.getColumnValue(op, pkCol)
		if err == nil && value != nil {
			return fmt.Sprintf("%v", value)
		}
	}

	// Last resort: use all available columns to create a composite key
	composite := ""
	for i, col := range op.Columns {
		if col != nil {
			value, err := c.getColumnValueByIndex(op, i)
			if err == nil && value != nil {
				composite += fmt.Sprintf("%s=%v|", col.Name, value)
			}
		}
	}

	if composite == "" {
		return "unknown"
	}

	return composite
}

// getColumnValue retrieves a column value from a CDC message
func (c *DefaultOperationConsolidator) getColumnValue(op *utils.CDCMessage, columnName string) (interface{}, error) {
	useOldValues := op.Type == utils.OperationDelete
	return op.GetColumnValue(columnName, useOldValues)
}

// getColumnValueByIndex retrieves a column value by index from a CDC message
func (c *DefaultOperationConsolidator) getColumnValueByIndex(op *utils.CDCMessage, index int) (interface{}, error) {
	if index >= len(op.Columns) {
		return nil, fmt.Errorf("column index %d out of range", index)
	}

	columnName := op.Columns[index].Name
	useOldValues := op.Type == utils.OperationDelete
	return op.GetColumnValue(columnName, useOldValues)
}

// consolidateTwo consolidates two operations on the same record
func (c *DefaultOperationConsolidator) consolidateTwo(existing *utils.CDCMessage, newOp *utils.CDCMessage) *utils.CDCMessage {
	switch {
	case existing.Type == utils.OperationInsert && newOp.Type == utils.OperationUpdate:

		result := *existing
		result.NewTuple = newOp.NewTuple
		result.LSN = newOp.LSN
		result.EmittedAt = newOp.EmittedAt
		return &result

	case existing.Type == utils.OperationInsert && newOp.Type == utils.OperationDelete:

		return nil

	case existing.Type == utils.OperationUpdate && newOp.Type == utils.OperationUpdate:

		result := *newOp
		if existing.OldTuple != nil && result.OldTuple == nil {
			result.OldTuple = existing.OldTuple
		}
		return &result

	case existing.Type == utils.OperationUpdate && newOp.Type == utils.OperationDelete:

		result := *newOp
		if existing.OldTuple != nil {
			result.OldTuple = existing.OldTuple
		}
		return &result

	case existing.Type == utils.OperationDelete && newOp.Type == utils.OperationInsert:

		result := *newOp
		result.Type = utils.OperationUpdate
		result.OldTuple = existing.OldTuple
		return &result

	default:

		return newOp
	}
}
