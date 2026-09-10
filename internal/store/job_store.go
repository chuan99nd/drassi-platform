package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job is the domain type for a single job run row (see jobs table,
// migrations/000003_create_workflow_run_tables.up.sql). This is the "jobs
// row id" referenced elsewhere as the proto job_run_id.
//
// Matrix expansion (T-M4-01) will create multiple Job rows sharing one
// JobIDYAML (one per matrix cell) — there is intentionally no uniqueness
// constraint on (RunID, JobIDYAML).
type Job struct {
	ID             uuid.UUID
	RunID          uuid.UUID
	JobIDYAML      string
	Name           string
	MatrixCellJSON []byte // nullable jsonb; null/{} for MVP (matrix deferred to T-M4-01)
	Needs          []string
	IfExpr         *string // nullable raw job-level if: string; empty/null = always
	Status         string  // "queued"|"leased"|"running"|"success"|"failure"|"skipped"|"cancelled"
	RunnerID       *uuid.UUID
	LeaseID        *uuid.UUID
	LeaseDeadline  *time.Time
	Result         *string
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

// jobTransitions encodes the job status state machine enforced by
// TransitionStatus: from -> set of legal to values. Any pair not listed here
// is illegal. Terminal statuses (success, failure, skipped, cancelled) have
// no outgoing transitions and so no entry.
var jobTransitions = map[string]map[string]bool{
	"queued": {"leased": true, "cancelled": true, "skipped": true},
	"leased": {"running": true, "queued": true, "cancelled": true},
	// "skipped" is a legal running->* outcome alongside success/failure/
	// cancelled (T-M3-01/T-M3-02): the runner pre-evaluates a job-level
	// `if:` (T-M3-03) and may report PHASE_SKIPPED via CompleteJob for a job
	// it decided not to actually execute after all steps were skipped.
	"running": {"success": true, "failure": true, "cancelled": true, "skipped": true},
}

func isLegalJobTransition(from, to string) bool {
	tos, ok := jobTransitions[from]
	if !ok {
		return false
	}
	return tos[to]
}

// JobStore persists and queries jobs.
type JobStore interface {
	// CreateJob inserts j. The caller must set j.ID before calling.
	CreateJob(ctx context.Context, j *Job) error
	// GetJob fetches a job by its row id. Returns a wrapped pgx.ErrNoRows
	// when the job does not exist.
	GetJob(ctx context.Context, id uuid.UUID) (*Job, error)
	// GetByLease fetches the job currently holding leaseID. Used by logstore
	// (T-M1-06) to map StreamLogs/ReportStatus/CompleteJob back to a job.
	// Returns a wrapped pgx.ErrNoRows when no job holds that lease.
	GetByLease(ctx context.Context, leaseID uuid.UUID) (*Job, error)
	// ListJobsForRun returns every job row for runID.
	ListJobsForRun(ctx context.Context, runID uuid.UUID) ([]*Job, error)
	// ListQueuedJobs returns up to limit jobs currently in "queued" status,
	// ordered by id for a stable, deterministic candidate order. Used by the
	// dispatcher (T-M1-04) to pick lease candidates: M1 workflows are
	// single-job and gate no `needs` (that's T-M3-01), so any stable order
	// is sufficient -- this intentionally does not order by a creation
	// timestamp, since jobs has none.
	ListQueuedJobs(ctx context.Context, limit int) ([]*Job, error)
	// TransitionStatus enforces the job status state machine (see
	// jobTransitions). It is implemented as a single conditional
	// UPDATE ... WHERE status = $from, so it is safe for concurrent callers:
	// if from->to is not a legal transition, or the row's current status no
	// longer matches from (lost race), it returns ErrIllegalTransition
	// without mutating the row.
	TransitionStatus(ctx context.Context, id uuid.UUID, from, to string) error
	// Lease bookkeeping helpers used by the dispatcher (T-M1-04):

	// Lease performs the queued->leased transition, stamping runner_id,
	// lease_id and lease_deadline. started_at is stamped here (on first
	// queued->leased) if it is not already set, rather than on the later
	// running transition. Returns ErrIllegalTransition if the job is not
	// currently queued (including a lost race).
	Lease(ctx context.Context, id, leaseID, runnerID uuid.UUID, deadline time.Time) error
	// RenewLease extends lease_deadline for the job currently holding
	// leaseID. Returns a wrapped pgx.ErrNoRows if no job holds that lease.
	RenewLease(ctx context.Context, leaseID uuid.UUID, deadline time.Time) error
	// RequeueExpired moves every leased job whose lease_deadline is before
	// now back to queued, clearing runner_id/lease_id/lease_deadline. The
	// dispatcher issues a new lease_id on the next Lease call, keeping this
	// idempotent. Returns the number of rows requeued.
	RequeueExpired(ctx context.Context, now time.Time) (requeued int, err error)
	// SetResult sets the terminal result and finished_at for a job. Callers
	// pair this with a TransitionStatus call into a terminal status.
	SetResult(ctx context.Context, id uuid.UUID, result string, finishedAt time.Time) error
}

// pgxJobStore is the pgx/v5 implementation of JobStore.
type pgxJobStore struct {
	pool *pgxpool.Pool
}

// NewJobStore constructs a JobStore backed by pool.
func NewJobStore(pool *pgxpool.Pool) JobStore {
	return &pgxJobStore{pool: pool}
}

const jobColumns = `id, run_id, job_id_yaml, name, matrix_cell_json, needs, if_expr, status,
		runner_id, lease_id, lease_deadline, result, started_at, finished_at`

func scanJob(row pgx.Row) (*Job, error) {
	var j Job
	err := row.Scan(
		&j.ID, &j.RunID, &j.JobIDYAML, &j.Name, &j.MatrixCellJSON, &j.Needs, &j.IfExpr, &j.Status,
		&j.RunnerID, &j.LeaseID, &j.LeaseDeadline, &j.Result, &j.StartedAt, &j.FinishedAt,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *pgxJobStore) CreateJob(ctx context.Context, j *Job) error {
	if j.Needs == nil {
		j.Needs = []string{}
	}

	q := fmt.Sprintf(`
		INSERT INTO jobs (%s)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`, jobColumns)

	_, err := s.pool.Exec(ctx, q,
		j.ID, j.RunID, j.JobIDYAML, j.Name, j.MatrixCellJSON, j.Needs, j.IfExpr, j.Status,
		j.RunnerID, j.LeaseID, j.LeaseDeadline, j.Result, j.StartedAt, j.FinishedAt,
	)
	if err != nil {
		return fmt.Errorf("store: create job: %w", err)
	}
	return nil
}

func (s *pgxJobStore) GetJob(ctx context.Context, id uuid.UUID) (*Job, error) {
	q := fmt.Sprintf(`SELECT %s FROM jobs WHERE id = $1`, jobColumns)

	j, err := scanJob(s.pool.QueryRow(ctx, q, id))
	if err != nil {
		return nil, fmt.Errorf("store: get job: %w", err)
	}
	return j, nil
}

func (s *pgxJobStore) GetByLease(ctx context.Context, leaseID uuid.UUID) (*Job, error) {
	q := fmt.Sprintf(`SELECT %s FROM jobs WHERE lease_id = $1`, jobColumns)

	j, err := scanJob(s.pool.QueryRow(ctx, q, leaseID))
	if err != nil {
		return nil, fmt.Errorf("store: get job by lease: %w", err)
	}
	return j, nil
}

func (s *pgxJobStore) ListJobsForRun(ctx context.Context, runID uuid.UUID) ([]*Job, error) {
	q := fmt.Sprintf(`SELECT %s FROM jobs WHERE run_id = $1 ORDER BY job_id_yaml`, jobColumns)

	rows, err := s.pool.Query(ctx, q, runID)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs for run: %w", err)
	}
	defer rows.Close()

	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list jobs for run: scan: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list jobs for run: %w", err)
	}
	return out, nil
}

func (s *pgxJobStore) ListQueuedJobs(ctx context.Context, limit int) ([]*Job, error) {
	q := fmt.Sprintf(`SELECT %s FROM jobs WHERE status = 'queued' ORDER BY id LIMIT $1`, jobColumns)

	rows, err := s.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list queued jobs: %w", err)
	}
	defer rows.Close()

	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list queued jobs: scan: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list queued jobs: %w", err)
	}
	return out, nil
}

func (s *pgxJobStore) TransitionStatus(ctx context.Context, id uuid.UUID, from, to string) error {
	if !isLegalJobTransition(from, to) {
		return fmt.Errorf("store: transition job %s from %q to %q: %w", id, from, to, ErrIllegalTransition)
	}

	const q = `UPDATE jobs SET status = $3 WHERE id = $1 AND status = $2`

	tag, err := s.pool.Exec(ctx, q, id, from, to)
	if err != nil {
		return fmt.Errorf("store: transition job status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: transition job %s from %q to %q: %w", id, from, to, ErrIllegalTransition)
	}
	return nil
}

func (s *pgxJobStore) Lease(ctx context.Context, id, leaseID, runnerID uuid.UUID, deadline time.Time) error {
	const q = `
		UPDATE jobs
		SET status = 'leased',
		    runner_id = $2,
		    lease_id = $3,
		    lease_deadline = $4,
		    started_at = COALESCE(started_at, now())
		WHERE id = $1 AND status = 'queued'`

	tag, err := s.pool.Exec(ctx, q, id, runnerID, leaseID, deadline.UTC())
	if err != nil {
		return fmt.Errorf("store: lease job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: lease job %s: %w", id, ErrIllegalTransition)
	}
	return nil
}

func (s *pgxJobStore) RenewLease(ctx context.Context, leaseID uuid.UUID, deadline time.Time) error {
	const q = `UPDATE jobs SET lease_deadline = $2 WHERE lease_id = $1`

	tag, err := s.pool.Exec(ctx, q, leaseID, deadline.UTC())
	if err != nil {
		return fmt.Errorf("store: renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: renew lease %s: %w", leaseID, pgx.ErrNoRows)
	}
	return nil
}

func (s *pgxJobStore) RequeueExpired(ctx context.Context, now time.Time) (int, error) {
	const q = `
		UPDATE jobs
		SET status = 'queued', runner_id = NULL, lease_id = NULL, lease_deadline = NULL
		WHERE status = 'leased' AND lease_deadline < $1`

	tag, err := s.pool.Exec(ctx, q, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: requeue expired: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *pgxJobStore) SetResult(ctx context.Context, id uuid.UUID, result string, finishedAt time.Time) error {
	const q = `UPDATE jobs SET result = $2, finished_at = $3 WHERE id = $1`

	tag, err := s.pool.Exec(ctx, q, id, result, finishedAt.UTC())
	if err != nil {
		return fmt.Errorf("store: set result: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: set result: %w", pgx.ErrNoRows)
	}
	return nil
}
