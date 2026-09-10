package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Step is the domain type for a single step within a job (see steps table,
// migrations/000003_create_workflow_run_tables.up.sql).
type Step struct {
	ID         uuid.UUID
	JobRowID   uuid.UUID
	StepIndex  int // 0-based order within job
	Name       string
	Status     string // "queued"|"running"|"success"|"failure"|"skipped"|"cancelled"
	StartedAt  *time.Time
	FinishedAt *time.Time
}

// StepStore persists and queries steps.
type StepStore interface {
	// UpsertStep inserts or updates the step keyed by (job_row_id,
	// step_index). The caller must set s.ID for the insert case; on update
	// the existing row's id is preserved.
	UpsertStep(ctx context.Context, s *Step) error
	// ListStepsForJob returns every step for jobRowID, ordered by step_index.
	ListStepsForJob(ctx context.Context, jobRowID uuid.UUID) ([]*Step, error)
}

// pgxStepStore is the pgx/v5 implementation of StepStore.
type pgxStepStore struct {
	pool *pgxpool.Pool
}

// NewStepStore constructs a StepStore backed by pool.
func NewStepStore(pool *pgxpool.Pool) StepStore {
	return &pgxStepStore{pool: pool}
}

func (s *pgxStepStore) UpsertStep(ctx context.Context, st *Step) error {
	const q = `
		INSERT INTO steps (id, job_row_id, step_index, name, status, started_at, finished_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (job_row_id, step_index) DO UPDATE
		SET name = EXCLUDED.name,
		    status = EXCLUDED.status,
		    started_at = EXCLUDED.started_at,
		    finished_at = EXCLUDED.finished_at
		RETURNING id`

	err := s.pool.QueryRow(ctx, q,
		st.ID, st.JobRowID, st.StepIndex, st.Name, st.Status, st.StartedAt, st.FinishedAt,
	).Scan(&st.ID)
	if err != nil {
		return fmt.Errorf("store: upsert step: %w", err)
	}
	return nil
}

func (s *pgxStepStore) ListStepsForJob(ctx context.Context, jobRowID uuid.UUID) ([]*Step, error) {
	const q = `
		SELECT id, job_row_id, step_index, name, status, started_at, finished_at
		FROM steps
		WHERE job_row_id = $1
		ORDER BY step_index`

	rows, err := s.pool.Query(ctx, q, jobRowID)
	if err != nil {
		return nil, fmt.Errorf("store: list steps for job: %w", err)
	}
	defer rows.Close()

	var out []*Step
	for rows.Next() {
		var st Step
		if err := rows.Scan(
			&st.ID, &st.JobRowID, &st.StepIndex, &st.Name, &st.Status, &st.StartedAt, &st.FinishedAt,
		); err != nil {
			return nil, fmt.Errorf("store: list steps for job: scan: %w", err)
		}
		out = append(out, &st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list steps for job: %w", err)
	}
	return out, nil
}
