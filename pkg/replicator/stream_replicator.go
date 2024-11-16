package replicator

import (
	"context"

	"github.com/jackc/pglogrepl"
)

type StreamReplicator struct {
	*BaseReplicator
}

func NewStreamReplicator(base *BaseReplicator) *StreamReplicator {
	return &StreamReplicator{
		BaseReplicator: base,
	}
}

func (r *StreamReplicator) Start(ctx context.Context) error {
	if err := r.BaseReplicator.Start(ctx); err != nil {
		return err
	}

	startLSN, err := r.GetLastState()
	if err != nil {
		r.Logger.Warn().Err(err).Msg("Failed to get last LSN, starting from 0")
		startLSN = pglogrepl.LSN(0)
	}

	r.Logger.Info().Str("startLSN", startLSN.String()).Msg("Starting replication")

	errChan := make(chan error, 1)
	go func() {
		errChan <- r.StartReplicationFromLSN(ctx, startLSN, r.stopChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

func (r *StreamReplicator) Stop(ctx context.Context) error {
	return r.BaseReplicator.Stop(ctx)
}
