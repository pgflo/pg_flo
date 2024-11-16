package replicator

import (
	"context"
	"fmt"
)

// ReplicatorFactory defines the interface for creating replicators
type Factory interface {
	CreateReplicator(config Config, natsClient NATSClient) (Replicator, error)
}

// BaseFactory provides common functionality for factories
type BaseFactory struct{}

// CreateConnections creates replication and standard connections
func (f *BaseFactory) CreateConnections(config Config) (ReplicationConnection, StandardConnection, error) {
	replicationConn := NewReplicationConnection(config)
	if err := replicationConn.Connect(context.Background()); err != nil {
		return nil, nil, fmt.Errorf("failed to connect for replication: %v", err)
	}

	standardConn, err := NewStandardConnection(config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create standard connection: %v", err)
	}

	return replicationConn, standardConn, nil
}

// StreamReplicatorFactory creates `StreamReplicator` instances
type StreamReplicatorFactory struct {
	BaseFactory
}

// CreateReplicator creates a new `StreamReplicator`
func (f *StreamReplicatorFactory) CreateReplicator(config Config, natsClient NATSClient) (Replicator, error) {
	replicationConn, standardConn, err := f.CreateConnections(config)
	if err != nil {
		return nil, err
	}

	baseReplicator := NewBaseReplicator(config, replicationConn, standardConn, natsClient)

	var ddlReplicator *DDLReplicator
	if config.TrackDDL {
		ddlConn, err := NewStandardConnection(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDL connection: %v", err)
		}
		ddlReplicator, err = NewDDLReplicator(config, baseReplicator, ddlConn)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDL replicator: %v", err)
		}
	}

	streamReplicator := &StreamReplicator{
		BaseReplicator: *baseReplicator,
	}

	if ddlReplicator != nil {
		streamReplicator.DDLReplicator = *ddlReplicator
	}

	return streamReplicator, nil
}

// CopyAndStreamReplicatorFactory creates `CopyAndStreamReplicator` instances
type CopyAndStreamReplicatorFactory struct {
	BaseFactory
	MaxCopyWorkersPerTable int
	CopyOnly               bool
}

// CreateReplicator creates a new `CopyAndStreamReplicator`
func (f *CopyAndStreamReplicatorFactory) CreateReplicator(config Config, natsClient NATSClient) (Replicator, error) {
	replicationConn, standardConn, err := f.CreateConnections(config)
	if err != nil {
		return nil, err
	}

	baseReplicator := NewBaseReplicator(config, replicationConn, standardConn, natsClient)

	var ddlReplicator *DDLReplicator
	if config.TrackDDL {
		ddlConn, err := NewStandardConnection(config)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDL connection: %v", err)
		}
		ddlReplicator, err = NewDDLReplicator(config, baseReplicator, ddlConn)
		if err != nil {
			return nil, fmt.Errorf("failed to create DDL replicator: %v", err)
		}
	}

	if f.MaxCopyWorkersPerTable <= 0 {
		f.MaxCopyWorkersPerTable = 4
	}

	copyAndStreamReplicator := &CopyAndStreamReplicator{
		BaseReplicator:         *baseReplicator,
		MaxCopyWorkersPerTable: f.MaxCopyWorkersPerTable,
		CopyOnly:               f.CopyOnly,
	}

	if ddlReplicator != nil {
		copyAndStreamReplicator.DDLReplicator = *ddlReplicator
	}

	return copyAndStreamReplicator, nil
}
