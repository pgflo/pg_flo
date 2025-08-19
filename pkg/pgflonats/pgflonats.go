// Package pgflonats provides NATS integration for PostgreSQL replication state management.
package pgflonats

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/nats-io/nats.go"
)

const (
	defaultNATSURL = "nats://localhost:4222"
	envNATSURL     = "PG_FLO_NATS_URL"
)

// NATSClient represents a client for interacting with NATS
type NATSClient struct {
	conn        *nats.Conn
	js          nats.JetStreamContext
	stream      string
	stateBucket string
}

// State represents the current state of the replication process
type State struct {
	LSN              pglogrepl.LSN `json:"lsn"`
	LastProcessedSeq map[string]uint64
}

// NewNATSClient creates a new NATS client with the specified configuration, setting up the connection, main stream, and state bucket.
func NewNATSClient(url, stream, group string) (*NATSClient, error) {
	if url == "" {
		url = os.Getenv(envNATSURL)
		if url == "" {
			url = defaultNATSURL
		}
	}

	if stream == "" {
		stream = fmt.Sprintf("pgflo_%s_stream", group)
	}

	nc, err := nats.Connect(url,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			fmt.Printf("Disconnected due to: %s, will attempt reconnects\n", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			fmt.Printf("Reconnected [%s]\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			fmt.Printf("Exiting: %v\n", nc.LastError())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Create the main stream
	streamConfig := &nats.StreamConfig{
		Name:      stream,
		Subjects:  []string{fmt.Sprintf("pgflo.%s", group)},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    24 * time.Hour,
	}
	_, err = js.AddStream(streamConfig)
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return nil, fmt.Errorf("failed to create main stream: %w", err)
	}

	// Create the state bucket
	stateBucket := fmt.Sprintf("pg_flo_state_%s", group)
	_, kvErr := js.KeyValue(stateBucket)
	if kvErr != nil {
		if errors.Is(kvErr, nats.ErrBucketNotFound) {
			_, err = js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket: stateBucket,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create state bucket: %w", err)
			}
		} else {
			return nil, fmt.Errorf("failed to access state bucket: %w", kvErr)
		}
	}

	return &NATSClient{
		conn:        nc,
		js:          js,
		stream:      stream,
		stateBucket: stateBucket,
	}, nil
}

// PublishMessage publishes a message to the specified NATS subject.
func (nc *NATSClient) PublishMessage(subject string, data []byte) error {
	_, err := nc.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// Close closes the NATS connection.
func (nc *NATSClient) Close() error {
	nc.conn.Close()
	return nil
}

// SaveState saves the current replication state to NATS.
func (nc *NATSClient) SaveState(state State) error {
	kv, err := nc.js.KeyValue(nc.stateBucket)
	if err != nil {
		return fmt.Errorf("failed to get KV bucket: %v", err)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	_, err = kv.Put("state", data)
	if err != nil {
		return fmt.Errorf("failed to save state: %v", err)
	}

	return nil
}

// GetState retrieves the last saved state from NATS, initializing a new state if none is found.
func (nc *NATSClient) GetState() (State, error) {
	kv, err := nc.js.KeyValue(nc.stateBucket)
	if err != nil {
		return State{}, fmt.Errorf("failed to get KV bucket: %v", err)
	}

	entry, err := kv.Get("state")
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			initialState := State{LastProcessedSeq: make(map[string]uint64)}
			// Try to create initial state
			if err := nc.SaveState(initialState); err != nil {
				// If SaveState fails because the key already exists, fetch it again
				if errors.Is(err, nats.ErrKeyExists) || errors.Is(err, nats.ErrUpdateMetaDeleted) {
					entry, err = kv.Get("state")
					if err != nil {
						return State{}, fmt.Errorf("failed to get state after conflict: %v", err)
					}
					if err := json.Unmarshal(entry.Value(), &initialState); err != nil {
						return State{}, fmt.Errorf("failed to unmarshal state after conflict: %v", err)
					}
					return initialState, nil
				}
				return State{}, fmt.Errorf("failed to save initial state: %v", err)
			}
			return initialState, nil
		}
		return State{}, fmt.Errorf("failed to get state: %v", err)
	}

	var state State
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return State{}, fmt.Errorf("failed to unmarshal state: %v", err)
	}

	if state.LastProcessedSeq == nil {
		state.LastProcessedSeq = make(map[string]uint64)
	}
	return state, nil
}

// JetStream returns the JetStream context.
func (nc *NATSClient) JetStream() nats.JetStreamContext {
	return nc.js
}
