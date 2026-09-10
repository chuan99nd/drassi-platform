package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestRunJobStepLogStore_RoundTrip exercises RunStore, JobStore and
// StepStore against a real Postgres instance, plus a direct check that
// log_chunks appends are idempotent on duplicate (lease_id, seq). It is
// skipped unless DATABASE_URL is set (see migrations/ + `make dev-up`).
func TestRunJobStepLogStore_RoundTrip(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping store integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	runs := NewRunStore(pool)
	jobs := NewJobStore(pool)
	steps := NewStepStore(pool)

	// --- workflow_runs ---

	runID := uuid.New()
	run := &Run{
		ID:               runID,
		Repo:             "octo/example",
		Ref:              "refs/heads/main",
		SHA:              "deadbeef",
		Event:            "push",
		EventPayloadJSON: []byte(`{"ref":"refs/heads/main"}`),
		TriggeredBy:      "octocat",
		Status:           "queued",
	}

	// Deleting workflow_runs cascades to jobs -> job_outputs/steps. log_chunks
	// has no FK to jobs, so it is cleaned up separately below.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM workflow_runs WHERE id = $1`, runID); err != nil {
			t.Logf("cleanup: failed to delete test run %s: %v", runID, err)
		}
	})

	if err := runs.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.CreatedAt.IsZero() {
		t.Fatalf("CreateRun: expected CreatedAt to be populated")
	}

	gotRun, err := runs.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Repo != run.Repo || gotRun.Ref != run.Ref || gotRun.SHA != run.SHA ||
		gotRun.Event != run.Event || gotRun.TriggeredBy != run.TriggeredBy || gotRun.Status != run.Status {
		t.Fatalf("GetRun: unexpected fields: %+v", gotRun)
	}

	listed, err := runs.ListRuns(ctx, 100, 0)
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if !containsRun(listed, runID) {
		t.Fatalf("ListRuns: expected to find run %s", runID)
	}

	if err := runs.SetRunStatus(ctx, runID, "running"); err != nil {
		t.Fatalf("SetRunStatus: %v", err)
	}
	gotRun, err = runs.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun (after SetRunStatus): %v", err)
	}
	if gotRun.Status != "running" {
		t.Fatalf("expected run status 'running', got %q", gotRun.Status)
	}

	// --- jobs ---

	job1ID := uuid.New()
	job1 := &Job{
		ID:        job1ID,
		RunID:     runID,
		JobIDYAML: "build",
		Name:      "build",
		Needs:     []string{},
		Status:    "queued",
	}
	if err := jobs.CreateJob(ctx, job1); err != nil {
		t.Fatalf("CreateJob (job1): %v", err)
	}

	ifExpr := "github.event_name == 'push'"
	job2ID := uuid.New()
	job2 := &Job{
		ID:        job2ID,
		RunID:     runID,
		JobIDYAML: "deploy",
		Name:      "deploy",
		Needs:     []string{"build", "test"},
		IfExpr:    &ifExpr,
		Status:    "queued",
	}
	if err := jobs.CreateJob(ctx, job2); err != nil {
		t.Fatalf("CreateJob (job2): %v", err)
	}

	gotJob2, err := jobs.GetJob(ctx, job2ID)
	if err != nil {
		t.Fatalf("GetJob (job2): %v", err)
	}
	if gotJob2.JobIDYAML != "deploy" || len(gotJob2.Needs) != 2 || gotJob2.Needs[0] != "build" || gotJob2.Needs[1] != "test" {
		t.Fatalf("GetJob (job2): unexpected needs: %+v", gotJob2.Needs)
	}
	if gotJob2.IfExpr == nil || *gotJob2.IfExpr != ifExpr {
		t.Fatalf("GetJob (job2): expected IfExpr %q, got %+v", ifExpr, gotJob2.IfExpr)
	}

	jobList, err := jobs.ListJobsForRun(ctx, runID)
	if err != nil {
		t.Fatalf("ListJobsForRun: %v", err)
	}
	if len(jobList) != 2 {
		t.Fatalf("ListJobsForRun: expected 2 jobs, got %d", len(jobList))
	}

	// --- TransitionStatus: legal transition ---
	if err := jobs.TransitionStatus(ctx, job1ID, "queued", "leased"); err != nil {
		t.Fatalf("TransitionStatus (queued->leased): %v", err)
	}
	gotJob1, err := jobs.GetJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("GetJob (job1 after transition): %v", err)
	}
	if gotJob1.Status != "leased" {
		t.Fatalf("expected job1 status 'leased', got %q", gotJob1.Status)
	}

	// --- TransitionStatus: illegal transition (not in the state machine) ---
	if err := jobs.TransitionStatus(ctx, job1ID, "success", "running"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("TransitionStatus (success->running): expected ErrIllegalTransition, got %v", err)
	}
	gotJob1, err = jobs.GetJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("GetJob (job1 after illegal transition): %v", err)
	}
	if gotJob1.Status != "leased" {
		t.Fatalf("illegal transition must not mutate row: expected 'leased', got %q", gotJob1.Status)
	}

	// --- TransitionStatus: lost race (from no longer current) ---
	if err := jobs.TransitionStatus(ctx, job1ID, "queued", "leased"); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("TransitionStatus (lost race, job1 already leased): expected ErrIllegalTransition, got %v", err)
	}

	// Move job1 through the rest of a normal lifecycle for SetResult/GetByLease below.
	if err := jobs.TransitionStatus(ctx, job1ID, "leased", "running"); err != nil {
		t.Fatalf("TransitionStatus (leased->running): %v", err)
	}

	// --- Lease / RenewLease / RequeueExpired on job2 ---

	leaseID := uuid.New()
	runnerID := uuid.New()
	deadline := time.Now().UTC().Add(1 * time.Minute).Truncate(time.Microsecond)
	if err := jobs.Lease(ctx, job2ID, leaseID, runnerID, deadline); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	gotJob2, err = jobs.GetJob(ctx, job2ID)
	if err != nil {
		t.Fatalf("GetJob (job2 after Lease): %v", err)
	}
	if gotJob2.Status != "leased" || gotJob2.LeaseID == nil || *gotJob2.LeaseID != leaseID ||
		gotJob2.RunnerID == nil || *gotJob2.RunnerID != runnerID || gotJob2.StartedAt == nil {
		t.Fatalf("Lease: unexpected job2 state: %+v", gotJob2)
	}

	// Leasing an already-leased job must fail (not currently queued).
	if err := jobs.Lease(ctx, job2ID, uuid.New(), runnerID, deadline); !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("Lease (already leased): expected ErrIllegalTransition, got %v", err)
	}

	byLease, err := jobs.GetByLease(ctx, leaseID)
	if err != nil {
		t.Fatalf("GetByLease: %v", err)
	}
	if byLease.ID != job2ID {
		t.Fatalf("GetByLease: expected job %s, got %s", job2ID, byLease.ID)
	}

	renewedDeadline := deadline.Add(1 * time.Minute)
	if err := jobs.RenewLease(ctx, leaseID, renewedDeadline); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	gotJob2, err = jobs.GetJob(ctx, job2ID)
	if err != nil {
		t.Fatalf("GetJob (job2 after RenewLease): %v", err)
	}
	if gotJob2.LeaseDeadline == nil || !gotJob2.LeaseDeadline.Equal(renewedDeadline) {
		t.Fatalf("RenewLease: expected lease_deadline %v, got %+v", renewedDeadline, gotJob2.LeaseDeadline)
	}

	// Force job2's lease into the past, then requeue it.
	pastDeadline := time.Now().UTC().Add(-1 * time.Minute)
	if err := jobs.RenewLease(ctx, leaseID, pastDeadline); err != nil {
		t.Fatalf("RenewLease (expire): %v", err)
	}

	// A non-expired leased job (job1 is now 'running', not 'leased', so it's
	// a good control: RequeueExpired must never touch running jobs).
	requeued, err := jobs.RequeueExpired(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("RequeueExpired: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("RequeueExpired: expected 1 row requeued, got %d", requeued)
	}
	gotJob2, err = jobs.GetJob(ctx, job2ID)
	if err != nil {
		t.Fatalf("GetJob (job2 after RequeueExpired): %v", err)
	}
	if gotJob2.Status != "queued" || gotJob2.RunnerID != nil || gotJob2.LeaseID != nil || gotJob2.LeaseDeadline != nil {
		t.Fatalf("RequeueExpired: expected cleared lease fields, got %+v", gotJob2)
	}
	gotJob1, err = jobs.GetJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("GetJob (job1, control for RequeueExpired): %v", err)
	}
	if gotJob1.Status != "running" {
		t.Fatalf("RequeueExpired must not touch non-expired/non-leased jobs; job1 status = %q", gotJob1.Status)
	}

	// A second RequeueExpired call is a no-op now.
	requeued, err = jobs.RequeueExpired(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("RequeueExpired (second call): %v", err)
	}
	if requeued != 0 {
		t.Fatalf("RequeueExpired (second call): expected 0 rows requeued, got %d", requeued)
	}

	// --- SetResult ---
	if err := jobs.TransitionStatus(ctx, job1ID, "running", "success"); err != nil {
		t.Fatalf("TransitionStatus (running->success): %v", err)
	}
	finishedAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := jobs.SetResult(ctx, job1ID, "success", finishedAt); err != nil {
		t.Fatalf("SetResult: %v", err)
	}
	gotJob1, err = jobs.GetJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("GetJob (job1 after SetResult): %v", err)
	}
	if gotJob1.Result == nil || *gotJob1.Result != "success" || gotJob1.FinishedAt == nil || !gotJob1.FinishedAt.Equal(finishedAt) {
		t.Fatalf("SetResult: unexpected job1 state: %+v", gotJob1)
	}

	// --- steps ---

	step0ID := uuid.New()
	step0 := &Step{
		ID:        step0ID,
		JobRowID:  job1ID,
		StepIndex: 0,
		Name:      "checkout",
		Status:    "queued",
	}
	if err := steps.UpsertStep(ctx, step0); err != nil {
		t.Fatalf("UpsertStep (insert): %v", err)
	}

	step1ID := uuid.New()
	step1 := &Step{
		ID:        step1ID,
		JobRowID:  job1ID,
		StepIndex: 1,
		Name:      "run tests",
		Status:    "queued",
	}
	if err := steps.UpsertStep(ctx, step1); err != nil {
		t.Fatalf("UpsertStep (insert step1): %v", err)
	}

	// Upsert: update step0 in place (same job_row_id, step_index).
	startedAt := time.Now().UTC().Truncate(time.Microsecond)
	step0Update := &Step{
		ID:        uuid.New(), // must be ignored: existing row id wins via ON CONFLICT
		JobRowID:  job1ID,
		StepIndex: 0,
		Name:      "checkout",
		Status:    "running",
		StartedAt: &startedAt,
	}
	if err := steps.UpsertStep(ctx, step0Update); err != nil {
		t.Fatalf("UpsertStep (update): %v", err)
	}

	stepList, err := steps.ListStepsForJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("ListStepsForJob: %v", err)
	}
	if len(stepList) != 2 {
		t.Fatalf("ListStepsForJob: expected 2 steps, got %d", len(stepList))
	}
	if stepList[0].ID != step0ID {
		t.Fatalf("UpsertStep: expected original step0 id %s to be preserved, got %s", step0ID, stepList[0].ID)
	}
	if stepList[0].Status != "running" || stepList[0].StartedAt == nil {
		t.Fatalf("UpsertStep: expected step0 updated in place, got %+v", stepList[0])
	}
	if stepList[1].ID != step1ID || stepList[1].Status != "queued" {
		t.Fatalf("UpsertStep: unexpected step1 state: %+v", stepList[1])
	}

	// --- log_chunks: append idempotency on duplicate (lease_id, seq) ---
	// The log_chunks append/pub-sub API belongs to internal/logstore
	// (T-M1-06); this task only creates the table DDL, so exercise the
	// idempotency guarantee directly against the schema here.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM log_chunks WHERE lease_id = $1`, leaseID); err != nil {
			t.Logf("cleanup: failed to delete test log_chunks for lease %s: %v", leaseID, err)
		}
	})

	const appendChunk = `
		INSERT INTO log_chunks (lease_id, step_id, seq, ts, data)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (lease_id, seq) DO NOTHING`

	ts := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx, appendChunk, leaseID, "", int64(0), ts, []byte("hello ")); err != nil {
		t.Fatalf("append log_chunks (seq 0): %v", err)
	}
	// Duplicate seq with different payload: must be a no-op, not an error and
	// not a second row.
	if _, err := pool.Exec(ctx, appendChunk, leaseID, "", int64(0), ts, []byte("DUPLICATE-SHOULD-BE-IGNORED")); err != nil {
		t.Fatalf("append log_chunks (duplicate seq 0): %v", err)
	}
	if _, err := pool.Exec(ctx, appendChunk, leaseID, "", int64(1), ts, []byte("world")); err != nil {
		t.Fatalf("append log_chunks (seq 1): %v", err)
	}

	var chunkCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM log_chunks WHERE lease_id = $1`, leaseID).Scan(&chunkCount); err != nil {
		t.Fatalf("count log_chunks: %v", err)
	}
	if chunkCount != 2 {
		t.Fatalf("expected 2 log_chunks rows (duplicate seq ignored), got %d", chunkCount)
	}

	var seq0Data []byte
	if err := pool.QueryRow(ctx, `SELECT data FROM log_chunks WHERE lease_id = $1 AND seq = 0`, leaseID).Scan(&seq0Data); err != nil {
		t.Fatalf("read log_chunks seq 0: %v", err)
	}
	if string(seq0Data) != "hello " {
		t.Fatalf("expected seq 0 data to be unchanged by the duplicate insert, got %q", seq0Data)
	}
}

func containsRun(runs []*Run, id uuid.UUID) bool {
	for _, r := range runs {
		if r.ID == id {
			return true
		}
	}
	return false
}
