// Package direct implements high-performance PostgreSQL CDC with direct sink architecture
package direct

import (
	"fmt"

	"github.com/pgflo/pg_flo/pkg/utils"
)

const (
	// UnknownOperationType represents an unknown CDC operation type
	UnknownOperationType = "unknown"
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
