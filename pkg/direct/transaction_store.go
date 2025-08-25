package direct

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/pgflo/pg_flo/pkg/utils"
)

// DefaultTransactionStore implements TransactionStore interface
type DefaultTransactionStore struct {
	currentTransaction map[string]*utils.CDCMessage
	memoryBytes        int64
	maxMemoryBytes     int64

	diskDB  *pebble.DB
	spilled bool

	mu sync.RWMutex

	logger utils.Logger
}

// NewTransactionStore creates a new transaction store
func NewTransactionStore(diskPath string, maxMemoryBytes int64, logger utils.Logger) (*DefaultTransactionStore, error) {
	if maxMemoryBytes <= 0 {
		maxMemoryBytes = 128 * 1024 * 1024
	}

	db, err := pebble.Open(diskPath, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open PebbleDB: %w", err)
	}

	return &DefaultTransactionStore{
		currentTransaction: make(map[string]*utils.CDCMessage),
		memoryBytes:        0,
		maxMemoryBytes:     maxMemoryBytes,
		diskDB:             db,
		spilled:            false,
		logger:             logger,
	}, nil
}

// Store adds a CDC message to the current transaction with key-based consolidation
func (ts *DefaultTransactionStore) Store(key string, msg *utils.CDCMessage) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	msgSize := ts.estimateMessageSize(msg)

	if ts.memoryBytes+msgSize > ts.maxMemoryBytes && !ts.spilled {
		if err := ts.spillToDisk(); err != nil {
			return fmt.Errorf("failed to spill to disk: %w", err)
		}
	}

	if ts.spilled {
		data, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		err = ts.diskDB.Set([]byte(key), data, pebble.Sync)
		if err != nil {
			return fmt.Errorf("failed to store in PebbleDB: %w", err)
		}
	} else {
		oldMsg := ts.currentTransaction[key]
		ts.currentTransaction[key] = msg

		if oldMsg != nil {
			oldSize := ts.estimateMessageSize(oldMsg)
			ts.memoryBytes = ts.memoryBytes - oldSize + msgSize
		} else {
			ts.memoryBytes += msgSize
		}
	}

	return nil
}

// GetAllMessages retrieves all messages from current transaction
func (ts *DefaultTransactionStore) GetAllMessages() ([]*utils.CDCMessage, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	var messages []*utils.CDCMessage

	if ts.spilled {
		iter, err := ts.diskDB.NewIter(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create iterator: %w", err)
		}
		defer func() {
			if closeErr := iter.Close(); closeErr != nil {
				ts.logger.Error().Err(closeErr).Msg("Failed to close iterator")
			}
		}()

		for iter.First(); iter.Valid(); iter.Next() {
			var msg utils.CDCMessage
			if err := json.Unmarshal(iter.Value(), &msg); err != nil {
				return nil, fmt.Errorf("failed to unmarshal message: %w", err)
			}
			messages = append(messages, &msg)
		}

		if err := iter.Error(); err != nil {
			return nil, fmt.Errorf("iterator error: %w", err)
		}
	} else {
		messages = make([]*utils.CDCMessage, 0, len(ts.currentTransaction))
		for _, msg := range ts.currentTransaction {
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// Clear clears the current transaction
func (ts *DefaultTransactionStore) Clear() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.spilled {
		iter, err := ts.diskDB.NewIter(nil)
		if err != nil {
			return fmt.Errorf("failed to create iterator: %w", err)
		}
		defer func() {
			if closeErr := iter.Close(); closeErr != nil {
				ts.logger.Error().Err(closeErr).Msg("Failed to close iterator")
			}
		}()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ts.diskDB.Delete(iter.Key(), pebble.Sync); err != nil {
				return fmt.Errorf("failed to delete key from PebbleDB: %w", err)
			}
		}

		if err := iter.Error(); err != nil {
			return fmt.Errorf("iterator error during clear: %w", err)
		}

		ts.spilled = false
	}

	ts.currentTransaction = make(map[string]*utils.CDCMessage)
	ts.memoryBytes = 0

	return nil
}

// GetMemoryUsage returns current memory usage in bytes
func (ts *DefaultTransactionStore) GetMemoryUsage() int64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.memoryBytes
}

// Close closes the transaction store
func (ts *DefaultTransactionStore) Close() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.diskDB != nil {
		err := ts.diskDB.Close()
		ts.diskDB = nil
		return err
	}

	return nil
}

func (ts *DefaultTransactionStore) spillToDisk() error {
	ts.logger.Info().
		Int64("memory_bytes", ts.memoryBytes).
		Int64("max_memory_bytes", ts.maxMemoryBytes).
		Msg("Transaction size exceeded memory limit - spilling to PebbleDB")

	for key, msg := range ts.currentTransaction {
		data, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message for spill: %w", err)
		}

		if err := ts.diskDB.Set([]byte(key), data, pebble.Sync); err != nil {
			return fmt.Errorf("failed to spill message to disk: %w", err)
		}
	}

	ts.currentTransaction = make(map[string]*utils.CDCMessage)
	ts.spilled = true

	return nil
}

func (ts *DefaultTransactionStore) estimateMessageSize(msg *utils.CDCMessage) int64 {
	size := int64(100)

	size += int64(len(msg.Schema))
	size += int64(len(msg.Table))
	size += int64(len(msg.LSN))

	if msg.NewTuple != nil {
		for _, col := range msg.NewTuple.Columns {
			if col != nil {
				size += int64(len(col.Data))
			}
		}
	}

	if msg.OldTuple != nil {
		for _, col := range msg.OldTuple.Columns {
			if col != nil {
				size += int64(len(col.Data))
			}
		}
	}

	if msg.CopyData != nil {
		for _, data := range msg.CopyData {
			size += int64(len(data))
		}
	}

	return size
}
