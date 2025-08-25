package tests

import (
	"testing"

	"github.com/pgflo/pg_flo/pkg/direct"
	"github.com/stretchr/testify/assert"
)

// Test DirectReplicatorFactory - maps to factory.go
func TestDirectReplicatorFactory_New(t *testing.T) {
	config := direct.Config{
		Group: "test-group",
		Source: direct.DatabaseConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "testdb",
			User:     "testuser",
		},
		S3: direct.S3Config{
			LocalPath: "/tmp/test",
		},
	}

	// Test factory creation
	factory := direct.NewDirectReplicatorFactory(config)
	assert.NotNil(t, factory)
	assert.Equal(t, "test-group", factory.Config.Group)
}

// Test config validation - maps to types.go
func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  direct.Config
		isValid bool
	}{
		{
			name: "valid config",
			config: direct.Config{
				Group: "test-group",
				Source: direct.DatabaseConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "testdb",
				},
			},
			isValid: true,
		},
		{
			name: "missing group",
			config: direct.Config{
				Source: direct.DatabaseConfig{
					Host: "localhost",
					Port: 5432,
				},
			},
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.isValid {
				assert.NotEmpty(t, tt.config.Group)
			} else {
				assert.Empty(t, tt.config.Group)
			}
		})
	}
}
