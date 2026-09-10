package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobOutputStore persists per-job key/value outputs (see job_outputs table,
// migrations/000003_create_workflow_run_tables.up.sql). Written by
// Dispatch.CompleteJob (T-M1-06); read back to hydrate downstream
// needs.*.outputs is T-M3-02's concern, out of scope here.
type JobOutputStore interface {
	// UpsertOutput sets outputs[jobRowID][key] = value, overwriting any
	// existing value for that key. Idempotent: a retried CompleteJob with
	// the same outputs is safe to replay.
	UpsertOutput(ctx context.Context, jobRowID uuid.UUID, key, value string) error
	// ListOutputs returns every output for jobRowID as a map.
	ListOutputs(ctx context.Context, jobRowID uuid.UUID) (map[string]string, error)
}

// pgxJobOutputStore is the pgx/v5 implementation of JobOutputStore.
type pgxJobOutputStore struct {
	pool *pgxpool.Pool
}

// NewJobOutputStore constructs a JobOutputStore backed by pool.
func NewJobOutputStore(pool *pgxpool.Pool) JobOutputStore {
	return &pgxJobOutputStore{pool: pool}
}

func (s *pgxJobOutputStore) UpsertOutput(ctx context.Context, jobRowID uuid.UUID, key, value string) error {
	const q = `
		INSERT INTO job_outputs (job_row_id, key, value)
		VALUES ($1, $2, $3)
		ON CONFLICT (job_row_id, key) DO UPDATE
		SET value = EXCLUDED.value`

	if _, err := s.pool.Exec(ctx, q, jobRowID, key, value); err != nil {
		return fmt.Errorf("store: upsert job output: %w", err)
	}
	return nil
}

func (s *pgxJobOutputStore) ListOutputs(ctx context.Context, jobRowID uuid.UUID) (map[string]string, error) {
	const q = `SELECT key, value FROM job_outputs WHERE job_row_id = $1`

	rows, err := s.pool.Query(ctx, q, jobRowID)
	if err != nil {
		return nil, fmt.Errorf("store: list job outputs: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("store: list job outputs: scan: %w", err)
		}
		out[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list job outputs: %w", err)
	}
	return out, nil
}
