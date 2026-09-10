package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Run is the domain type for a workflow run (see workflow_runs table,
// migrations/000003_create_workflow_run_tables.up.sql,
// migrations/000004_add_workflow_yaml.up.sql).
type Run struct {
	ID               uuid.UUID
	Repo             string // owner/name
	Ref              string // branch/tag/ref requested
	SHA              string // resolved commit sha
	Event            string // "push"|"pull_request"|"manual"
	EventPayloadJSON []byte // raw jsonb; {} allowed for manual
	TriggeredBy      string // actor login / user id
	Status           string // "queued"|"running"|"success"|"failure"|"cancelled"
	// WorkflowYAML is the verbatim workflow YAML the planner (T-M1-02)
	// parsed to create this run's jobs, persisted here so the dispatcher
	// (T-M1-04) can build JobLease.workflow_yaml without re-fetching from
	// GitHub at lease time. M1 has exactly one workflow YAML per run, so
	// storing it once on the run (rather than per-job) is the simplest
	// MVP choice. "" for rows written before migration 000004 or by a
	// caller that hasn't been wired to set it yet.
	WorkflowYAML string
	CreatedAt    time.Time
}

// RunStore persists and queries workflow runs.
type RunStore interface {
	// CreateRun inserts r. The caller must set r.ID before calling (may be
	// pre-set or generated with uuid.New()).
	CreateRun(ctx context.Context, r *Run) error
	// GetRun fetches a run by id. Returns a wrapped pgx.ErrNoRows when the
	// run does not exist.
	GetRun(ctx context.Context, id uuid.UUID) (*Run, error)
	// ListRuns returns runs ordered by created_at descending, most recent first.
	ListRuns(ctx context.Context, limit, offset int) ([]*Run, error)
	// SetRunStatus sets status for the run identified by id.
	SetRunStatus(ctx context.Context, id uuid.UUID, status string) error
}

// pgxRunStore is the pgx/v5 implementation of RunStore.
type pgxRunStore struct {
	pool *pgxpool.Pool
}

// NewRunStore constructs a RunStore backed by pool.
func NewRunStore(pool *pgxpool.Pool) RunStore {
	return &pgxRunStore{pool: pool}
}

func (s *pgxRunStore) CreateRun(ctx context.Context, r *Run) error {
	if r.EventPayloadJSON == nil {
		r.EventPayloadJSON = []byte(`{}`)
	}

	const q = `
		INSERT INTO workflow_runs (id, repo, ref, sha, event, event_payload_json, triggered_by, status, workflow_yaml)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING created_at`

	err := s.pool.QueryRow(ctx, q,
		r.ID, r.Repo, r.Ref, r.SHA, r.Event, r.EventPayloadJSON, r.TriggeredBy, r.Status, r.WorkflowYAML,
	).Scan(&r.CreatedAt)
	if err != nil {
		return fmt.Errorf("store: create run: %w", err)
	}
	return nil
}

func (s *pgxRunStore) GetRun(ctx context.Context, id uuid.UUID) (*Run, error) {
	const q = `
		SELECT id, repo, ref, sha, event, event_payload_json, triggered_by, status, workflow_yaml, created_at
		FROM workflow_runs
		WHERE id = $1`

	var r Run
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&r.ID, &r.Repo, &r.Ref, &r.SHA, &r.Event, &r.EventPayloadJSON, &r.TriggeredBy, &r.Status, &r.WorkflowYAML, &r.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("store: get run: %w", err)
	}
	return &r, nil
}

func (s *pgxRunStore) ListRuns(ctx context.Context, limit, offset int) ([]*Run, error) {
	const q = `
		SELECT id, repo, ref, sha, event, event_payload_json, triggered_by, status, workflow_yaml, created_at
		FROM workflow_runs
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2`

	rows, err := s.pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("store: list runs: %w", err)
	}
	defer rows.Close()

	var out []*Run
	for rows.Next() {
		var r Run
		if err := rows.Scan(
			&r.ID, &r.Repo, &r.Ref, &r.SHA, &r.Event, &r.EventPayloadJSON, &r.TriggeredBy, &r.Status, &r.WorkflowYAML, &r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("store: list runs: scan: %w", err)
		}
		out = append(out, &r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list runs: %w", err)
	}
	return out, nil
}

func (s *pgxRunStore) SetRunStatus(ctx context.Context, id uuid.UUID, status string) error {
	const q = `UPDATE workflow_runs SET status = $2 WHERE id = $1`

	tag, err := s.pool.Exec(ctx, q, id, status)
	if err != nil {
		return fmt.Errorf("store: set run status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: set run status: %w", pgx.ErrNoRows)
	}
	return nil
}
