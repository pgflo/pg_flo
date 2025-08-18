package replicator

import (
	"errors"
	"fmt"
)

var (
	// ErrReplicatorAlreadyStarted is returned when attempting to start an already running replicator
	ErrReplicatorAlreadyStarted = errors.New("replicator already started")
	// ErrReplicatorNotStarted is returned when attempting to stop a non-running replicator
	ErrReplicatorNotStarted     = errors.New("replicator not started")
	// ErrReplicatorAlreadyStopped is returned when attempting to stop an already stopped replicator
	ErrReplicatorAlreadyStopped = errors.New("replicator already stopped")
)

// ReplicationError represents an error that occurred during replication.
type ReplicationError struct {
	Op  string // The operation that caused the error
	Err error  // The underlying error
}

// Error returns a formatted error message.
func (e *ReplicationError) Error() string {
	return fmt.Sprintf("replication error during %s: %v", e.Op, e.Err)
}
