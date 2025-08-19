package pgflonats

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNATSClient_Close_GracefulShutdown(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "Normal close should drain and close connection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if testing.Short() {
				t.Skip("Skipping NATS integration test in short mode")
			}

			client, err := NewNATSClient("nats://localhost:4222", "test_stream", "test_group")
			if err != nil {
				t.Skip("NATS server not available, skipping test")
				return
			}

			for i := 0; i < 5; i++ {
				err := client.PublishMessage("pgflo.test_group", []byte("test message"))
				assert.NoError(t, err, "Should be able to publish messages")
			}

			testState := State{
				LSN:              pglogrepl.LSN(12345),
				LastProcessedSeq: map[string]uint64{"test_group": 100},
			}
			err = client.SaveState(testState)
			assert.NoError(t, err, "Should be able to save state")

			startTime := time.Now()
			err = client.Close()
			elapsed := time.Since(startTime)

			assert.NoError(t, err, "Close should complete without error")
			assert.Less(t, elapsed, 5*time.Second, "Close should complete within reasonable time")
			// Note: Drain() automatically closes the connection, so we can't check IsClosed() reliably
			// The important thing is that Close() completed without error
		})
	}
}

func TestNATSClient_Close_AlreadyClosed(t *testing.T) {
	t.Run("Close on already closed connection should be safe", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping NATS integration test in short mode")
		}

		client, err := NewNATSClient("nats://localhost:4222", "test_stream", "test_group")
		if err != nil {
			t.Skip("NATS server not available, skipping test")
			return
		}

		err = client.Close()
		require.NoError(t, err, "First close should succeed")

		err = client.Close()
		assert.NoError(t, err, "Second close should be safe and not error")
	})
}

func TestNATSClient_Close_NilConnection(t *testing.T) {
	t.Run("Close on nil connection should be safe", func(t *testing.T) {
		client := &NATSClient{
			conn: nil,
		}

		err := client.Close()
		assert.NoError(t, err, "Close on nil connection should not error")
	})
}

func TestNATSClient_PublishAndClose_Integration(t *testing.T) {
	t.Run("Publish messages then close should drain properly", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping NATS integration test in short mode")
		}

		client, err := NewNATSClient("nats://localhost:4222", "test_stream_integration", "test_group_integration")
		if err != nil {
			t.Skip("NATS server not available, skipping test")
			return
		}

		const numMessages = 100
		for i := 0; i < numMessages; i++ {
			err := client.PublishMessage("pgflo.test_group_integration", []byte("test message"))
			if err != nil {
				t.Logf("Failed to publish message %d: %v", i, err)
			}
		}

		startTime := time.Now()
		err = client.Close()
		elapsed := time.Since(startTime)

		assert.NoError(t, err, "Close should complete successfully after publishing many messages")
		assert.Less(t, elapsed, 10*time.Second, "Close should complete within reasonable time even with many messages")
		// Note: Drain() handles connection closure automatically
	})
}

// Benchmark to ensure Close performance is reasonable
func BenchmarkNATSClient_Close(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping NATS benchmark in short mode")
	}

	client, err := NewNATSClient("nats://localhost:4222", "bench_stream", "bench_group")
	if err != nil {
		b.Skip("NATS server not available, skipping benchmark")
		return
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = client.PublishMessage("pgflo.bench_group", []byte("benchmark message"))

		start := time.Now()
		err := client.Close()
		elapsed := time.Since(start)

		if err != nil {
			b.Errorf("Close failed: %v", err)
		}

		b.ReportMetric(float64(elapsed.Nanoseconds()), "ns/close")

		client, err = NewNATSClient("nats://localhost:4222", "bench_stream", "bench_group")
		if err != nil {
			b.Fatalf("Failed to reconnect: %v", err)
		}
	}
}
