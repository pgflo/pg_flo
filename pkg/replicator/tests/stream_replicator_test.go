package replicator_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgflo/pg_flo/pkg/pgflonats"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestStreamReplicator_Start_GracefulShutdown(t *testing.T) {
	tests := []struct {
		name                  string
		contextTimeout        time.Duration
		startReplicationError error
		expectedStartupError  bool
		expectGoroutineWait   bool
		description           string
	}{
		{
			name:                  "Normal startup and graceful context cancellation",
			contextTimeout:        100 * time.Millisecond,
			startReplicationError: nil,
			expectedStartupError:  true, // context.Canceled
			expectGoroutineWait:   true,
			description:           "Should wait for replication goroutine to finish when context is cancelled",
		},
		{
			name:                  "Fast startup error",
			contextTimeout:        1 * time.Second,
			startReplicationError: assert.AnError,
			expectedStartupError:  true,
			expectGoroutineWait:   false,
			description:           "Should return immediately when StartReplicationFromLSN fails quickly",
		},
		{
			name:                  "Long running replication with context cancellation",
			contextTimeout:        50 * time.Millisecond,
			startReplicationError: nil, // Will be cancelled by context
			expectedStartupError:  true,
			expectGoroutineWait:   true,
			description:           "Should gracefully handle context cancellation during long-running replication",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockReplicationConn := new(MockReplicationConnection)
			mockStandardConn := new(MockStandardConnection)
			mockNATSClient := new(MockNATSClient)

			setupMockBaseReplicatorDependencies(t, mockStandardConn, mockNATSClient)

			config := replicator.Config{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				User:     "testuser",
				Password: "testpass",
				Group:    "testgroup",
				Schema:   "public",
				Tables:   []string{"test_table"},
			}

			baseReplicator := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)
			streamReplicator := replicator.NewStreamReplicator(baseReplicator)

			setupMockBaseReplicatorStart(mockReplicationConn, mockStandardConn)

			mockNATSClient.On("GetState").Return(pgflonats.State{
				LSN:              pglogrepl.LSN(0),
				LastProcessedSeq: make(map[string]uint64),
			}, nil)

			startReplicationCalled := make(chan struct{})
			startReplicationFinished := make(chan struct{})

			if tt.startReplicationError == nil {
				mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
					close(startReplicationCalled)
					ctx := args[0].(context.Context)
					<-ctx.Done()
					close(startReplicationFinished)
				})
			} else {
				mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(tt.startReplicationError).Run(func(_ mock.Arguments) {
					close(startReplicationCalled)
					close(startReplicationFinished)
				})
			}

			ctx, cancel := context.WithTimeout(context.Background(), tt.contextTimeout)
			defer cancel()

			startTime := time.Now()
			err := streamReplicator.Start(ctx)

			if tt.expectedStartupError {
				assert.Error(t, err, "Expected startup error")
			} else {
				assert.NoError(t, err, "Expected no startup error")
			}

			select {
			case <-startReplicationCalled:
			case <-time.After(200 * time.Millisecond):
				if tt.expectedStartupError && tt.startReplicationError != nil {
					t.Log("StartReplication not called, which is expected for base replicator startup errors")
				} else {
					t.Fatal("StartReplicationFromLSN was not called within timeout")
				}
			}

			if tt.expectGoroutineWait {
				select {
				case <-startReplicationFinished:
					elapsed := time.Since(startTime)
					assert.GreaterOrEqual(t, elapsed, tt.contextTimeout-10*time.Millisecond,
						"Should have waited for goroutine to finish")
				case <-time.After(500 * time.Millisecond):
					t.Fatal("Goroutine did not finish within expected time")
				}
			}

			mockReplicationConn.AssertExpectations(t)
			mockStandardConn.AssertExpectations(t)
			mockNATSClient.AssertExpectations(t)
		})
	}
}

func TestStreamReplicator_Start_Stop_Coordination(t *testing.T) {
	t.Run("Stop during Start should coordinate properly", func(t *testing.T) {
		// Create mocks
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)

		// Mock base replicator setup
		setupMockBaseReplicatorDependencies(t, mockStandardConn, mockNATSClient)

		// Create config
		config := replicator.Config{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			Group:    "testgroup",
			Schema:   "public",
			Tables:   []string{"test_table"},
		}

		// Create base replicator
		baseReplicator := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)
		streamReplicator := replicator.NewStreamReplicator(baseReplicator)

		// Set up expectations for base replicator Start()
		setupMockBaseReplicatorStart(mockReplicationConn, mockStandardConn)

		// Mock GetLastState
		mockNATSClient.On("GetState").Return(pgflonats.State{
			LSN:              pglogrepl.LSN(0),
			LastProcessedSeq: make(map[string]uint64),
		}, nil)

		// Mock StartReplication - simulate long-running replication
		replicationStarted := make(chan struct{})
		mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			close(replicationStarted)
			// Wait for context cancellation
			ctx := args[0].(context.Context)
			<-ctx.Done()
		})

		// Mock Stop method dependencies
		mockNATSClient.On("SaveState", mock.Anything).Return(nil)
		mockReplicationConn.On("SendStandbyStatusUpdate", mock.Anything, mock.Anything).Return(nil)
		mockReplicationConn.On("Close", mock.Anything).Return(nil)
		mockStandardConn.On("Close", mock.Anything).Return(nil)

		// Start replication in a goroutine
		ctx := context.Background()
		startErr := make(chan error)
		go func() {
			err := streamReplicator.Start(ctx)
			startErr <- err
		}()

		// Wait for replication to start
		<-replicationStarted

		// Stop the replicator
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stopErr := streamReplicator.Stop(stopCtx)
		assert.NoError(t, stopErr, "Stop should complete without error")

		// Verify Start returns within reasonable time (graceful shutdown should return nil)
		select {
		case err := <-startErr:
			// Graceful shutdown should return nil (no error)
			assert.NoError(t, err, "Graceful shutdown should not return an error")
		case <-time.After(2 * time.Second):
			t.Fatal("Start did not return within reasonable time after Stop")
		}

		// Verify all mock expectations
		mockReplicationConn.AssertExpectations(t)
		mockStandardConn.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
	})
}

func TestStreamReplicator_Multiple_Start_Stop_Calls(t *testing.T) {
	t.Run("Multiple Start calls should be handled safely", func(t *testing.T) {
		// Create mocks
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)

		// Mock base replicator setup
		setupMockBaseReplicatorDependencies(t, mockStandardConn, mockNATSClient)

		// Create config
		config := replicator.Config{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			Group:    "testgroup",
			Schema:   "public",
			Tables:   []string{"test_table"},
		}

		// Create base replicator
		baseReplicator := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)
		streamReplicator := replicator.NewStreamReplicator(baseReplicator)

		// First Start should work
		setupMockBaseReplicatorStart(mockReplicationConn, mockStandardConn)
		mockNATSClient.On("GetState").Return(pgflonats.State{
			LSN:              pglogrepl.LSN(0),
			LastProcessedSeq: make(map[string]uint64),
		}, nil)

		// Mock replication that finishes quickly
		mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err1 := streamReplicator.Start(ctx)
		assert.Error(t, err1, "First start should return error from StartReplication")

		// Second Start should return error (already started)
		err2 := streamReplicator.Start(ctx)
		assert.Error(t, err2, "Second start should return already started error")
		assert.Contains(t, err2.Error(), "already started", "Should indicate already started")

		// Stop should work
		mockNATSClient.On("SaveState", mock.Anything).Return(nil)
		mockReplicationConn.On("SendStandbyStatusUpdate", mock.Anything, mock.Anything).Return(nil)
		mockReplicationConn.On("Close", mock.Anything).Return(nil)
		mockStandardConn.On("Close", mock.Anything).Return(nil)

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer stopCancel()
		err3 := streamReplicator.Stop(stopCtx)
		assert.NoError(t, err3, "Stop should complete without error")

		// Multiple Stop calls should be safe
		err4 := streamReplicator.Stop(stopCtx)
		assert.Error(t, err4, "Second stop should return not started error")

		// Verify all mock expectations
		mockReplicationConn.AssertExpectations(t)
		mockStandardConn.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
	})
}

func TestStreamReplicator_UnhandledCancellation_Prevention(t *testing.T) {
	t.Run("Context cancellation during Start should not leave hanging goroutines", func(t *testing.T) {
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)

		setupMockBaseReplicatorDependencies(t, mockStandardConn, mockNATSClient)
		setupMockBaseReplicatorStart(mockReplicationConn, mockStandardConn)

		config := replicator.Config{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			Group:    "testgroup",
			Schema:   "public",
			Tables:   []string{"test_table"},
		}

		baseReplicator := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)
		streamReplicator := replicator.NewStreamReplicator(baseReplicator)

		mockNATSClient.On("GetState").Return(pgflonats.State{
			LSN:              pglogrepl.LSN(0),
			LastProcessedSeq: make(map[string]uint64),
		}, nil)

		goroutineStarted := make(chan struct{})
		goroutineFinished := make(chan struct{})

		// Mock StartReplication that simulates long-running operation
		mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			close(goroutineStarted)
			ctx := args[0].(context.Context)
			// Wait for cancellation to test proper handling
			<-ctx.Done()
			close(goroutineFinished)
		})

		// Very short timeout to trigger cancellation quickly
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		// Start should handle cancellation gracefully
		err := streamReplicator.Start(ctx)
		assert.Error(t, err, "Should return context cancellation error")
		assert.Contains(t, err.Error(), "context", "Error should indicate context cancellation")

		// Verify goroutine actually started and finished (no leak)
		select {
		case <-goroutineStarted:
			t.Log("✓ Goroutine started correctly")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Goroutine never started - test setup issue")
		}

		select {
		case <-goroutineFinished:
			t.Log("✓ Goroutine finished correctly - no leak")
		case <-time.After(500 * time.Millisecond):
			t.Fatal("Goroutine did not finish - LEAK DETECTED")
		}

		mockReplicationConn.AssertExpectations(t)
		mockStandardConn.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
	})
}

func TestStreamReplicator_ResourceCleanup_(t *testing.T) {
	t.Run("Stop should cleanup all resources even with concurrent operations", func(t *testing.T) {
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)

		setupMockBaseReplicatorDependencies(t, mockStandardConn, mockNATSClient)
		setupMockBaseReplicatorStart(mockReplicationConn, mockStandardConn)

		config := replicator.Config{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			Group:    "testgroup",
			Schema:   "public",
			Tables:   []string{"test_table"},
		}

		baseReplicator := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)
		streamReplicator := replicator.NewStreamReplicator(baseReplicator)

		mockNATSClient.On("GetState").Return(pgflonats.State{
			LSN:              pglogrepl.LSN(0),
			LastProcessedSeq: make(map[string]uint64),
		}, nil)

		replicationActive := make(chan struct{})

		// Mock long-running replication
		mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			close(replicationActive)
			ctx := args[0].(context.Context)
			<-ctx.Done()
		})

		// Mock ALL cleanup operations to verify they're called
		mockNATSClient.On("SaveState", mock.Anything).Return(nil)
		mockReplicationConn.On("SendStandbyStatusUpdate", mock.Anything, mock.Anything).Return(nil)
		mockReplicationConn.On("Close", mock.Anything).Return(nil)
		mockStandardConn.On("Close", mock.Anything).Return(nil)

		ctx := context.Background()
		startErr := make(chan error)
		go func() {
			err := streamReplicator.Start(ctx)
			startErr <- err
		}()

		// Wait for replication to be active
		<-replicationActive

		// Call Stop to trigger cleanup
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		stopErr := streamReplicator.Stop(stopCtx)
		assert.NoError(t, stopErr, "Stop should complete cleanup without error")

		// Verify Start method completes after Stop
		select {
		case err := <-startErr:
			assert.NoError(t, err, "Start should complete gracefully after Stop")
		case <-time.After(3 * time.Second):
			t.Fatal("Start did not complete after Stop - possible hanging goroutine")
		}

		// Verify ALL cleanup methods were called
		mockNATSClient.AssertExpectations(t)
		mockReplicationConn.AssertExpectations(t)
		mockStandardConn.AssertExpectations(t)
	})
}

func TestStreamReplicator_MultipleStop_EdgeCases(t *testing.T) {
	t.Run("Multiple Stop calls should be safe and not cause panics", func(t *testing.T) {
		mockReplicationConn := new(MockReplicationConnection)
		mockStandardConn := new(MockStandardConnection)
		mockNATSClient := new(MockNATSClient)

		setupMockBaseReplicatorDependencies(t, mockStandardConn, mockNATSClient)
		setupMockBaseReplicatorStart(mockReplicationConn, mockStandardConn)

		config := replicator.Config{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
			Group:    "testgroup",
			Schema:   "public",
			Tables:   []string{"test_table"},
		}

		baseReplicator := replicator.NewBaseReplicator(config, mockReplicationConn, mockStandardConn, mockNATSClient)
		streamReplicator := replicator.NewStreamReplicator(baseReplicator)

		mockNATSClient.On("GetState").Return(pgflonats.State{
			LSN:              pglogrepl.LSN(0),
			LastProcessedSeq: make(map[string]uint64),
		}, nil)

		// Mock replication that returns error quickly
		mockReplicationConn.On("StartReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err1 := streamReplicator.Start(ctx)
		assert.Error(t, err1, "First start should return error from StartReplication")

		// Mock cleanup operations for Stop calls
		mockNATSClient.On("SaveState", mock.Anything).Return(nil)
		mockReplicationConn.On("SendStandbyStatusUpdate", mock.Anything, mock.Anything).Return(nil)
		mockReplicationConn.On("Close", mock.Anything).Return(nil)
		mockStandardConn.On("Close", mock.Anything).Return(nil)

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer stopCancel()

		// First Stop should work
		err2 := streamReplicator.Stop(stopCtx)
		assert.NoError(t, err2, "First Stop should complete without error")

		// Multiple Stop calls should be safe and not panic
		err3 := streamReplicator.Stop(stopCtx)
		assert.Error(t, err3, "Second Stop should return not started error")
		assert.Contains(t, err3.Error(), "not started", "Should indicate replicator not started")

		// Third Stop should also be safe
		err4 := streamReplicator.Stop(stopCtx)
		assert.Error(t, err4, "Third Stop should also return not started error")

		mockReplicationConn.AssertExpectations(t)
		mockStandardConn.AssertExpectations(t)
		mockNATSClient.AssertExpectations(t)
	})
}

// Helper function to set up common mock expectations for base replicator dependencies
func setupMockBaseReplicatorDependencies(_ *testing.T, mockStandardConn *MockStandardConnection, _ *MockNATSClient) {
	// Mock for InitializeOIDMap query
	mockOIDRows := new(MockRows)
	mockOIDRows.On("Next").Return(false)
	mockOIDRows.On("Err").Return(nil)
	mockOIDRows.On("Close").Return()

	// Mock for InitializePrimaryKeyInfo query
	mockPKRows := new(MockRows)
	mockPKRows.On("Next").Return(false)
	mockPKRows.On("Err").Return(nil)
	mockPKRows.On("Close").Return()

	// Set up expectations for initialization queries
	mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_type")
	}), mock.Anything).Return(mockOIDRows, nil)

	// Mock for the complex primary key info query
	mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "table_info") && strings.Contains(q, "relreplident")
	}), mock.Anything).Return(mockPKRows, nil)

	// Fallback mock for any other Query calls
	mockStandardConn.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(mockPKRows, nil)
}

// Helper function to set up mock expectations for base replicator Start()
func setupMockBaseReplicatorStart(mockReplicationConn *MockReplicationConnection, mockStandardConn *MockStandardConnection) {
	// Mock publication existence check
	mockPublicationRow := MockRow{
		scanFunc: func(dest ...interface{}) error {
			if b, ok := dest[0].(*bool); ok {
				*b = false // Publication doesn't exist
			}
			return nil
		},
	}
	mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_publication")
	}), mock.Anything).Return(mockPublicationRow)

	// Mock GetConfiguredTables - return the configured tables
	mockTablesRows := new(MockRows)
	mockTablesRows.On("Next").Return(false) // No additional tables beyond config
	mockTablesRows.On("Err").Return(nil)
	mockTablesRows.On("Close").Return()
	mockStandardConn.On("Query", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_tables")
	}), mock.Anything).Return(mockTablesRows, nil)

	// Mock publication creation
	mockStandardConn.On("Exec", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "CREATE PUBLICATION")
	}), mock.Anything).Return(pgconn.CommandTag{}, nil)

	// Mock replication slot existence check
	mockSlotRow := MockRow{
		scanFunc: func(dest ...interface{}) error {
			if b, ok := dest[0].(*bool); ok {
				*b = false // Slot doesn't exist
			}
			return nil
		},
	}
	mockStandardConn.On("QueryRow", mock.Anything, mock.MatchedBy(func(q string) bool {
		return strings.Contains(q, "pg_replication_slots")
	}), mock.Anything).Return(mockSlotRow)

	// Mock replication slot creation
	mockReplicationConn.On("CreateReplicationSlot", mock.Anything, mock.Anything).Return(
		pglogrepl.CreateReplicationSlotResult{
			SlotName:        "test_slot",
			ConsistentPoint: "0/123456",
			SnapshotName:    "test_snapshot",
			OutputPlugin:    "pgoutput",
		}, nil)
}
