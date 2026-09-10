// Package store contains pgx-backed persistence for drassi-server: pool
// bootstrap plus per-entity repositories (runners here; runs/jobs/steps/
// job_outputs land in T-M1-01).
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool parses dsn, opens a pgx connection pool and verifies connectivity
// with a Ping before returning. Callers should Close the pool on shutdown.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("store: parse dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping db: %w", err)
	}

	return pool, nil
}
