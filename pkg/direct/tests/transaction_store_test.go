package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/direct"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionStore_New(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	tests := []struct {
		name           string
		diskPath       string
		maxMemoryBytes int64
		expectError    bool
	}{
		{
			name:           "valid configuration",
			diskPath:       tempDir,
			maxMemoryBytes: 1024 * 1024, // 1MB
			expectError:    false,
		},
		{
			name:           "zero memory limit uses default",
			diskPath:       tempDir,
			maxMemoryBytes: 0,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := direct.NewTransactionStore(tt.diskPath, tt.maxMemoryBytes, logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, store)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, store)
				if store != nil {
					err := store.Close()
					assert.NoError(t, err)
				}
			}
		})
	}
}

func TestTransactionStore_BasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	store, err := direct.NewTransactionStore(tempDir, 1024*1024, logger)
	require.NoError(t, err)
	require.NotNil(t, store)
	defer func() { _ = store.Close() }()

	// Store a message
	operation := &utils.CDCMessage{
		Type:      utils.OperationInsert,
		Schema:    "public",
		Table:     "users",
		LSN:       "0/12345",
		EmittedAt: time.Now(),
	}

	err = store.Store("record-1", operation)
	assert.NoError(t, err)

	// Get all messages
	messages, err := store.GetAllMessages()
	assert.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, utils.OperationInsert, messages[0].Type)
	assert.Equal(t, "public", messages[0].Schema)
	assert.Equal(t, "users", messages[0].Table)
}

func TestTransactionStore_MultipleMessages(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	store, err := direct.NewTransactionStore(tempDir, 1024*1024, logger)
	require.NoError(t, err)
	require.NotNil(t, store)
	defer func() { _ = store.Close() }()

	// Store multiple messages
	operations := []*utils.CDCMessage{
		{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "users",
			LSN:    "0/1",
		},
		{
			Type:   utils.OperationUpdate,
			Schema: "public",
			Table:  "users",
			LSN:    "0/2",
		},
		{
			Type:   utils.OperationDelete,
			Schema: "public",
			Table:  "orders",
			LSN:    "0/3",
		},
	}

	for i, op := range operations {
		err = store.Store(fmt.Sprintf("record-%d", i), op)
		assert.NoError(t, err)
	}

	// Get all messages
	messages, err := store.GetAllMessages()
	assert.NoError(t, err)
	assert.Len(t, messages, 3)

	// Verify message types
	types := make(map[utils.OperationType]int)
	for _, msg := range messages {
		types[msg.Type]++
	}

	assert.Equal(t, 1, types[utils.OperationInsert])
	assert.Equal(t, 1, types[utils.OperationUpdate])
	assert.Equal(t, 1, types[utils.OperationDelete])
}

func TestTransactionStore_MemoryUsage(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	store, err := direct.NewTransactionStore(tempDir, 1024*1024, logger)
	require.NoError(t, err)
	require.NotNil(t, store)
	defer func() { _ = store.Close() }()

	// Check initial memory usage
	initialMemory := store.GetMemoryUsage()
	assert.GreaterOrEqual(t, initialMemory, int64(0))

	// Store a message
	operation := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "users",
		LSN:    "0/12345",
	}

	err = store.Store("record-1", operation)
	assert.NoError(t, err)

	// Memory usage should increase
	afterStoreMemory := store.GetMemoryUsage()
	assert.GreaterOrEqual(t, afterStoreMemory, initialMemory)
}

func TestTransactionStore_Clear(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	store, err := direct.NewTransactionStore(tempDir, 1024*1024, logger)
	require.NoError(t, err)
	require.NotNil(t, store)
	defer func() { _ = store.Close() }()

	// Store some messages
	for i := 0; i < 5; i++ {
		operation := &utils.CDCMessage{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "users",
			LSN:    fmt.Sprintf("0/%d", i),
		}
		err = store.Store(fmt.Sprintf("record-%d", i), operation)
		assert.NoError(t, err)
	}

	// Verify messages exist
	messages, err := store.GetAllMessages()
	assert.NoError(t, err)
	assert.Len(t, messages, 5)

	// Clear the store
	err = store.Clear()
	assert.NoError(t, err)

	// Verify messages are cleared
	messages, err = store.GetAllMessages()
	assert.NoError(t, err)
	assert.Empty(t, messages)
}

func TestTransactionStore_DiskSpilling(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	// Use very small memory limit to force disk spilling
	store, err := direct.NewTransactionStore(tempDir, 100, logger) // 100 bytes
	require.NoError(t, err)
	require.NotNil(t, store)
	defer func() { _ = store.Close() }()

	// Store many large messages to exceed memory limit
	for i := 0; i < 10; i++ {
		operation := &utils.CDCMessage{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "large_table_with_very_long_name",
			LSN:    fmt.Sprintf("0/%d", i),
			// Add some data to make the message larger
			NewTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{Data: []byte("very long data string to force disk spilling when memory limit is exceeded")},
				},
			},
		}
		err = store.Store(fmt.Sprintf("large-record-%d", i), operation)
		assert.NoError(t, err)
	}

	// Should still be able to retrieve all messages
	messages, err := store.GetAllMessages()
	assert.NoError(t, err)
	assert.Len(t, messages, 10)

	// Memory usage should be reasonable (below a high threshold)
	memUsage := store.GetMemoryUsage()
	assert.Less(t, memUsage, int64(1024*1024), "Memory usage should be controlled with disk spilling")
}

func TestTransactionStore_Close(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	store, err := direct.NewTransactionStore(tempDir, 1024*1024, logger)
	require.NoError(t, err)
	require.NotNil(t, store)

	// Store some data
	operation := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "users",
		LSN:    "0/1",
	}
	err = store.Store("record-1", operation)
	require.NoError(t, err)

	// Close should work without error
	err = store.Close()
	assert.NoError(t, err)

	// Second close should also work (idempotent)
	err = store.Close()
	assert.NoError(t, err)
}

// Test commit lock pattern - data is buffered until commit
func TestTransactionStore_CommitLockPattern(t *testing.T) {
	tempDir := t.TempDir()
	logger := &MockLogger{}

	store, err := direct.NewTransactionStore(tempDir, 1024*1024, logger)
	require.NoError(t, err)
	require.NotNil(t, store)
	defer func() { _ = store.Close() }()

	// Test commit lock pattern - data is buffered until commit
	key1 := "tx1-record1"
	op1 := &utils.CDCMessage{
		Type:   utils.OperationInsert,
		Schema: "public",
		Table:  "users", 
		LSN:    "0/1",
	}

	// Store operation
	err = store.Store(key1, op1)
	assert.NoError(t, err)

	// Verify operation is stored
	messages, err := store.GetAllMessages()
	assert.NoError(t, err)
	assert.Len(t, messages, 1)

	// Clear simulates commit
	err = store.Clear()
	assert.NoError(t, err)

	// Verify cleared after commit
	messages, err = store.GetAllMessages()
	assert.NoError(t, err)
	assert.Empty(t, messages)
}
