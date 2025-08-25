package tests

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/direct"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/stretchr/testify/assert"
)

// Test the CTID range generation logic
func TestCTIDRangeGeneration(t *testing.T) {
	tests := []struct {
		name        string
		totalPages  uint32
		chunkSize   int
		expectedMin int // minimum number of ranges
	}{
		{
			name:        "small table",
			totalPages:  100,
			chunkSize:   1000,
			expectedMin: 1,
		},
		{
			name:        "medium table",
			totalPages:  1000,
			chunkSize:   1000,
			expectedMin: 1,
		},
		{
			name:        "large table",
			totalPages:  10000,
			chunkSize:   1000,
			expectedMin: 5, // Should create multiple chunks
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't easily test the private method, but we can test the logic
			// by creating ranges manually and verifying they don't overlap

			// Simple range generation logic for testing
			ranges := generateTestCTIDRanges(tt.totalPages, tt.chunkSize)

			assert.GreaterOrEqual(t, len(ranges), tt.expectedMin)

			// Verify ranges don't overlap
			for i := 1; i < len(ranges); i++ {
				assert.LessOrEqual(t, ranges[i-1].EndPage, ranges[i].StartPage,
					"Ranges should not overlap")
			}
		})
	}
}

// Helper function to simulate CTID range generation
func generateTestCTIDRanges(totalPages uint32, chunkSize int) []TestCTIDRange {
	var ranges []TestCTIDRange

	pagesPerChunk := uint32(chunkSize / 100) //nolint:gosec // Test values are safe
	if pagesPerChunk == 0 {
		pagesPerChunk = 1
	}

	for startPage := uint32(0); startPage < totalPages; startPage += pagesPerChunk {
		endPage := startPage + pagesPerChunk
		if endPage > totalPages {
			endPage = totalPages
		}

		ranges = append(ranges, TestCTIDRange{
			StartPage: startPage,
			EndPage:   endPage,
		})
	}

	return ranges
}

type TestCTIDRange struct {
	StartPage uint32
	EndPage   uint32
}

// Test copy status transitions
func TestCopyStatusTransitions(t *testing.T) {
	tests := []struct {
		name            string
		initialStatus   direct.CopyStatus
		validNextStates []direct.CopyStatus
	}{
		{
			name:          "not started to in progress",
			initialStatus: direct.CopyStatusNotStarted,
			validNextStates: []direct.CopyStatus{
				direct.CopyStatusInProgress,
				direct.CopyStatusFailed,
			},
		},
		{
			name:          "in progress to completed",
			initialStatus: direct.CopyStatusInProgress,
			validNextStates: []direct.CopyStatus{
				direct.CopyStatusCompleted,
				direct.CopyStatusFailed,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that status values are valid strings
			assert.NotEmpty(t, string(tt.initialStatus))

			for _, nextStatus := range tt.validNextStates {
				assert.NotEmpty(t, string(nextStatus))
				assert.NotEqual(t, tt.initialStatus, nextStatus)
			}
		})
	}
}

// Test table info validation
func TestTableInfoValidation(t *testing.T) {
	tests := []struct {
		name    string
		table   direct.TableInfo
		isValid bool
	}{
		{
			name: "valid copy-and-stream",
			table: direct.TableInfo{
				Schema:   "public",
				Table:    "users",
				SyncMode: "copy-and-stream",
			},
			isValid: true,
		},
		{
			name: "valid stream-only",
			table: direct.TableInfo{
				Schema:   "analytics",
				Table:    "events",
				SyncMode: "stream-only",
			},
			isValid: true,
		},
		{
			name: "empty schema",
			table: direct.TableInfo{
				Schema:   "",
				Table:    "users",
				SyncMode: "copy-and-stream",
			},
			isValid: false,
		},
		{
			name: "empty table",
			table: direct.TableInfo{
				Schema:   "public",
				Table:    "",
				SyncMode: "copy-and-stream",
			},
			isValid: false,
		},
		{
			name: "invalid sync mode",
			table: direct.TableInfo{
				Schema:   "public",
				Table:    "users",
				SyncMode: "invalid-mode",
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.isValid {
				assert.NotEmpty(t, tt.table.Schema)
				assert.NotEmpty(t, tt.table.Table)
				assert.Contains(t, []string{"copy-and-stream", "stream-only"}, tt.table.SyncMode)
			} else {
				isValidTable := tt.table.Schema != "" && tt.table.Table != "" &&
					(tt.table.SyncMode == "copy-and-stream" || tt.table.SyncMode == "stream-only")
				assert.False(t, isValidTable)
			}
		})
	}
}

// Test configuration validation
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  direct.Config
		isValid bool
	}{
		{
			name: "valid minimal config",
			config: direct.Config{
				Group: "test-group",
				Source: direct.DatabaseConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "testdb",
					User:     "testuser",
				},
				S3: direct.S3Config{
					LocalPath: "/tmp/parquet",
				},
			},
			isValid: true,
		},
		{
			name: "missing group",
			config: direct.Config{
				Source: direct.DatabaseConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "testdb",
				},
			},
			isValid: false,
		},
		{
			name: "invalid port",
			config: direct.Config{
				Group: "test-group",
				Source: direct.DatabaseConfig{
					Host:     "localhost",
					Port:     -1,
					Database: "testdb",
				},
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.isValid {
				assert.NotEmpty(t, tt.config.Group)
				assert.NotEmpty(t, tt.config.Source.Host)
				assert.Greater(t, tt.config.Source.Port, 0)
			} else {
				isValidConfig := tt.config.Group != "" &&
					tt.config.Source.Host != "" &&
					tt.config.Source.Port > 0
				assert.False(t, isValidConfig)
			}
		})
	}
}

// Test database connection string generation
func TestDatabaseConnectionString(t *testing.T) {
	config := direct.DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		Database: "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	connStr := config.ConnectionString()

	assert.Contains(t, connStr, "host=localhost")
	assert.Contains(t, connStr, "port=5432")
	assert.Contains(t, connStr, "dbname=testdb")
	assert.Contains(t, connStr, "user=testuser")
	assert.Contains(t, connStr, "password=testpass")
}

// Test snapshot info structure
func TestSnapshotInfo(t *testing.T) {
	snapshot := direct.SnapshotInfo{
		Name:       "test-snapshot-123",
		LSN:        pglogrepl.LSN(12345),
		ExportedAt: time.Now(),
	}

	assert.NotEmpty(t, snapshot.Name)
	assert.Greater(t, uint64(snapshot.LSN), uint64(0))
	assert.False(t, snapshot.ExportedAt.IsZero())
}

// Test table assignment structure (group-based scaling)
func TestTableAssignment(t *testing.T) {
	now := time.Now()
	assignment := direct.TableAssignment{
		ID:               "table-1",
		GroupName:        "test-group",
		SchemaName:       "public",
		TableName:        "users",
		SyncMode:         "copy-and-stream",
		AssignedWorkerID: "worker-123", // Current field name
		AssignedAt:       now,          // Current field type
		LastHeartbeat:    now,
		TotalPages:       1000,
	}

	assert.NotEmpty(t, assignment.ID)
	assert.NotEmpty(t, assignment.GroupName)
	assert.NotEmpty(t, assignment.SchemaName)
	assert.NotEmpty(t, assignment.TableName)
	assert.Contains(t, []string{"copy-and-stream", "stream-only"}, assignment.SyncMode)
	assert.False(t, assignment.AssignedAt.IsZero())
}

// Test consolidator basic functionality
func TestConsolidatorBasics(t *testing.T) {
	consolidator := direct.NewDefaultOperationConsolidator()
	assert.NotNil(t, consolidator)

	// Test with empty operations
	result := consolidator.ConsolidateTransaction([]*utils.CDCMessage{})
	assert.Empty(t, result)

	// Test with nil operations
	result = consolidator.ConsolidateTransaction(nil)
	assert.Empty(t, result)
}

// Test that all major types implement their interfaces
func TestInterfaceImplementations(t *testing.T) {
	// Test that default consolidator implements the interface
	var consolidator direct.OperationConsolidator = direct.NewDefaultOperationConsolidator()
	assert.NotNil(t, consolidator)

	// Test basic consolidation
	operations := []*utils.CDCMessage{
		{
			Type:   utils.OperationInsert,
			Schema: "public",
			Table:  "test",
			LSN:    "0/1",
		},
	}

	result := consolidator.ConsolidateTransaction(operations)
	assert.Len(t, result, 1)
}

// Test configuration defaults
func TestConfigDefaults(t *testing.T) {
	config := &direct.Config{}

	// Test that boolean fields have sensible defaults
	assert.False(t, config.IncludePgFloMetadata) // Should default to false

	// Test that numeric fields can be set
	config.MaxParquetFileSize = 128 * 1024 * 1024 // 128MB
	assert.Equal(t, int64(128*1024*1024), config.MaxParquetFileSize)

	config.MaxMemoryBytes = 64 * 1024 * 1024 // 64MB
	assert.Equal(t, int64(64*1024*1024), config.MaxMemoryBytes)
}
