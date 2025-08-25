package direct

import (
	"context"
	"fmt"

	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog/log"
)

// DirectReplicatorFactory creates DirectReplicator instances
type DirectReplicatorFactory struct {
	Config Config
}

// NewDirectReplicatorFactory creates a new factory for direct replicators
func NewDirectReplicatorFactory(config Config) *DirectReplicatorFactory {
	return &DirectReplicatorFactory{
		Config: config,
	}
}

// CreateReplicator creates a new DirectReplicator instance
func (f *DirectReplicatorFactory) CreateReplicator(replicatorConfig replicator.Config, _ replicator.NATSClient) (replicator.Replicator, error) {
	logger := utils.NewZerologLogger(log.With().Str("component", "orchestrated-direct-replicator").Logger())

	// Convert replicator config to direct config
	directConfig := f.Config
	if directConfig.Source.Host == "" {
		directConfig.Source.Host = replicatorConfig.Host
	}
	if directConfig.Source.Port == 0 {
		directConfig.Source.Port = int(replicatorConfig.Port)
	}
	if directConfig.Source.Database == "" {
		directConfig.Source.Database = replicatorConfig.Database
	}
	if directConfig.Source.User == "" {
		directConfig.Source.User = replicatorConfig.User
	}
	if directConfig.Source.Password == "" {
		directConfig.Source.Password = replicatorConfig.Password
	}
	if directConfig.Group == "" {
		directConfig.Group = replicatorConfig.Group
	}

	// Create orchestrated direct replicator
	dr, err := NewOrchestratedDirectReplicator(directConfig, logger)
	if err != nil {
		return nil, err
	}

	// Wrap it to implement replicator.Replicator interface
	return WrapDirectReplicator(dr), nil
}

// DirectReplicatorWrapper wraps DirectReplicator to implement replicator.Replicator interface
type DirectReplicatorWrapper struct {
	directReplicator DirectReplicator
}

// Start implements replicator.Replicator interface
func (w *DirectReplicatorWrapper) Start(ctx context.Context) error {
	if err := w.directReplicator.Bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap failed: %w", err)
	}
	return w.directReplicator.Start(ctx)
}

// Stop implements replicator.Replicator interface
func (w *DirectReplicatorWrapper) Stop(ctx context.Context) error {
	return w.directReplicator.Stop(ctx)
}

// WrapDirectReplicator wraps a DirectReplicator to implement replicator.Replicator
func WrapDirectReplicator(dr DirectReplicator) replicator.Replicator {
	return &DirectReplicatorWrapper{directReplicator: dr}
}
