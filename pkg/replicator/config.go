package replicator

import (
	"fmt"
	"time"

	"github.com/pgflo/pg_flo/pkg/utils"
)

// Config holds the configuration for the replicator
type Config struct {
	Host        string
	Port        uint16
	Database    string
	User        string
	Password    string
	Group       string
	Schema      string
	Tables      []string
	TrackDDL    bool
	RetryConfig utils.RetryConfig
}

// GetRetryConfig returns the retry configuration with defaults if not set
func (c Config) GetRetryConfig() utils.RetryConfig {
	if c.RetryConfig.MaxAttempts == 0 {
		return utils.RetryConfig{
			MaxAttempts: 3,
			InitialWait: time.Second * 1,
			MaxWait:     time.Second * 8,
		}
	}
	return c.RetryConfig
}

// ConnectionString generates and returns a PostgreSQL connection string
func (c Config) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.Database)
}
