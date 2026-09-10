package dispatch

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	"github.com/chuan99nd/drassi-platform/internal/config"
	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/store"
	"github.com/chuan99nd/drassi-platform/pkg/dispatchpb"
)

// TestDispatchService_AcquireJob exercises T-M1-04's queue/leasing engine
// against a real Postgres instance and a real (bufconn) gRPC AcquireJob
// stream:
//   - a single queued job connected to one runner is leased exactly once
//     (job row queued->leased, one JobLease received, its fields correctly
//     assembled from the job + run rows);
//   - two runners racing for two queued jobs never both get the same job
//     (the conditional-UPDATE handoff in JobStore.Lease is what guarantees
//     this -- ListQueuedJobs is only an advisory candidate list);
//   - an expired lease (deadline already passed, no heartbeat renewal) is
//     requeued by StartJobReaper and can be leased again under a fresh
//     lease_id.
//
// Skipped unless DATABASE_URL is set.
func TestDispatchService_AcquireJob(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping dispatch integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	runners := store.NewRunnerStore(pool)
	jobs := store.NewJobStore(pool)
	steps := store.NewStepStore(pool)
	runs := store.NewRunStore(pool)
	outputs := store.NewJobOutputStore(pool)
	logs := logstore.New(pool)

	// Short heartbeat config so leaseTTL() (interval * miss-limit) is small
	// enough to exercise the reaper without a long test.
	cfg := config.Config{
		HTTPAddr:                 ":0",
		GRPCAddr:                 ":0",
		DBDSN:                    dsn,
		HeartbeatIntervalSeconds: 1,
		HeartbeatMissLimit:       1,
	}
	svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)

	// --- fixtures: two runners + a run with three queued jobs -----------
	// job1/job2: the two-runners-race scenario. job3: the reaper scenario.

	newRunner := func(name string) *store.Runner {
		id := uuid.New()
		r := &store.Runner{
			ID:        id,
			Name:      name + "-" + id.String()[:8],
			Labels:    []string{"self-hosted"},
			Mode:      "host",
			Capacity:  2,
			Version:   "test",
			Status:    "online",
			AuthToken: "test-token-" + id.String(),
		}
		if err := runners.Create(ctx, r); err != nil {
			t.Fatalf("runners.Create(%s): %v", name, err)
		}
		t.Cleanup(func() {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cCancel()
			if _, err := pool.Exec(cCtx, `DELETE FROM runners WHERE id = $1`, id); err != nil {
				t.Logf("cleanup runner %s: %v", name, err)
			}
		})
		return r
	}
	runnerA := newRunner("runner-a")
	runnerB := newRunner("runner-b")

	const workflowYAML = "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n"

	runID := uuid.New()
	run := &store.Run{
		ID:           runID,
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "deadbeef",
		Event:        "push",
		TriggeredBy:  "octocat",
		Status:       "running",
		WorkflowYAML: workflowYAML,
	}
	if err := runs.CreateRun(ctx, run); err != nil {
		t.Fatalf("runs.CreateRun: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		if _, err := pool.Exec(cCtx, `DELETE FROM workflow_runs WHERE id = $1`, runID); err != nil {
			t.Logf("cleanup run: %v", err)
		}
	})

	newJob := func(jobIDYAML string) *store.Job {
		id := uuid.New()
		j := &store.Job{ID: id, RunID: runID, JobIDYAML: jobIDYAML, Name: jobIDYAML, Status: "queued"}
		if err := jobs.CreateJob(ctx, j); err != nil {
			t.Fatalf("CreateJob(%s): %v", jobIDYAML, err)
		}
		return j
	}
	job1 := newJob("build")
	job2 := newJob("test")
	job3 := newJob("deploy")

	grpcServer, client, closeClient := startBufconnServer(t, svc)
	defer grpcServer.Stop()
	defer closeClient()

	authCtx := func(runner *store.Runner) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+runner.AuthToken)
	}

	// acquireOne returns (nil, err) instead of calling t.Fatal directly so
	// it is safe to invoke from a non-test goroutine (below, for the
	// concurrent-race scenario) -- callers on the main test goroutine check
	// the returned error themselves.
	acquireOne := func(runner *store.Runner) (*dispatchpb.JobLease, error) {
		stream, err := client.AcquireJob(authCtx(runner), &dispatchpb.AcquireJobRequest{
			RunnerId:  runner.ID.String(),
			FreeSlots: 1,
		})
		if err != nil {
			return nil, err
		}
		return stream.Recv()
	}

	// --- single queued job leased exactly once, JobLease fields correct --

	lease1, err := acquireOne(runnerA)
	if err != nil {
		t.Fatalf("AcquireJob (runnerA): %v", err)
	}
	if lease1.GetJobRunId() != job1.ID.String() && lease1.GetJobRunId() != job2.ID.String() {
		// job1/job2 are both queued at this point; either is a valid first
		// pick (M1 does no needs-gating/ordering beyond "some queued job").
		t.Fatalf("expected lease for job1 or job2, got job_run_id=%s", lease1.GetJobRunId())
	}
	if lease1.GetWorkflowYaml() != workflowYAML {
		t.Fatalf("expected workflow_yaml round-trip, got %q", lease1.GetWorkflowYaml())
	}
	if lease1.GetEventName() != "push" || lease1.GetRef() != "refs/heads/main" || lease1.GetSha() != "deadbeef" || lease1.GetActor() != "octocat" {
		t.Fatalf("unexpected lease event/ref/sha/actor fields: %+v", lease1)
	}
	if lease1.GetRepoUrl() == "" {
		t.Fatalf("expected non-empty repo_url")
	}
	if lease1.GetLeaseDeadlineUnix() <= time.Now().Unix() {
		t.Fatalf("expected lease_deadline_unix in the future, got %d", lease1.GetLeaseDeadlineUnix())
	}
	leasedJobID1, err := uuid.Parse(lease1.GetJobRunId())
	if err != nil {
		t.Fatalf("parse job_run_id: %v", err)
	}
	gotJob1, err := jobs.GetJob(ctx, leasedJobID1)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if gotJob1.Status != "leased" || gotJob1.RunnerID == nil || *gotJob1.RunnerID != runnerA.ID || gotJob1.LeaseID == nil || gotJob1.LeaseID.String() != lease1.GetLeaseId() {
		t.Fatalf("unexpected job state after lease: %+v", gotJob1)
	}

	// --- two runners racing for the one remaining queued job never both
	// get it: only one of the two concurrent AcquireJob calls can succeed
	// against the single job still queued (the other of job1/job2). -------

	remainingJobID := job1.ID
	if remainingJobID == leasedJobID1 {
		remainingJobID = job2.ID
	}

	type result struct {
		runner string
		lease  *dispatchpb.JobLease
		err    error
	}
	results := make(chan result, 2)
	go func() { l, err := acquireOne(runnerA); results <- result{"A", l, err} }()
	go func() { l, err := acquireOne(runnerB); results <- result{"B", l, err} }()

	// Exactly one of these two concurrent calls should win the remaining
	// queued job (job3 is still queued too, so both calls do complete, but
	// they must not both report the SAME job -- that would mean the same
	// job was double-leased).
	r1, r2 := <-results, <-results
	if r1.err != nil {
		t.Fatalf("AcquireJob (%s): %v", r1.runner, r1.err)
	}
	if r2.err != nil {
		t.Fatalf("AcquireJob (%s): %v", r2.runner, r2.err)
	}
	if r1.lease.GetJobRunId() == r2.lease.GetJobRunId() {
		t.Fatalf("two concurrent AcquireJob calls received the SAME job (double-lease!): %s", r1.lease.GetJobRunId())
	}
	seen := map[string]bool{r1.lease.GetJobRunId(): true, r2.lease.GetJobRunId(): true}
	if !seen[remainingJobID.String()] || !seen[job3.ID.String()] {
		t.Fatalf("expected the two remaining queued jobs (%s, %s) to be leased, got %+v", remainingJobID, job3.ID, seen)
	}
	if r1.lease.GetLeaseId() == r2.lease.GetLeaseId() {
		t.Fatalf("two concurrent AcquireJob calls received the SAME lease_id: %s", r1.lease.GetLeaseId())
	}

	// All three jobs are now leased with distinct lease_ids and no job
	// double-booked.
	for _, j := range []*store.Job{job1, job2, job3} {
		got, err := jobs.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatalf("GetJob(%s): %v", j.JobIDYAML, err)
		}
		if got.Status != "leased" {
			t.Fatalf("expected job %s leased, got %q", j.JobIDYAML, got.Status)
		}
	}

	// --- expired lease requeue via StartJobReaper -------------------------
	// Simulate a lease whose deadline already passed (no heartbeat renewal
	// in time) directly via JobStore.Lease with a past deadline, run the
	// service's reaper, and confirm it is requeued and re-leasable under a
	// fresh lease_id.

	oldLeaseID := uuid.New()
	if err := jobs.Lease(ctx, job3.ID, oldLeaseID, runnerA.ID, time.Now().UTC().Add(-time.Minute)); err != nil {
		// job3 may already be "leased" (from the race above) rather than
		// "queued" -- RequeueExpired doesn't care about lease_id, only
		// status='leased' AND lease_deadline < now, so overwrite its
		// deadline into the past directly instead of re-Leasing (Lease
		// requires status='queued').
		if _, execErr := pool.Exec(ctx, `UPDATE jobs SET lease_deadline = $2, lease_id = $3 WHERE id = $1`,
			job3.ID, time.Now().UTC().Add(-time.Minute), oldLeaseID); execErr != nil {
			t.Fatalf("force-expire job3 lease: %v (Lease also failed: %v)", execErr, err)
		}
	}

	reaperCtx, reaperCancel := context.WithCancel(ctx)
	defer reaperCancel()
	go svc.StartJobReaper(reaperCtx)

	deadlineWait := time.Now().Add(jobReaperInterval + 5*time.Second)
	var requeued *store.Job
	for time.Now().Before(deadlineWait) {
		got, err := jobs.GetJob(ctx, job3.ID)
		if err != nil {
			t.Fatalf("GetJob(job3) while waiting for reaper: %v", err)
		}
		if got.Status == "queued" {
			requeued = got
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	reaperCancel()
	if requeued == nil {
		t.Fatalf("job3 was not requeued by StartJobReaper within %s", jobReaperInterval+5*time.Second)
	}
	if requeued.LeaseID != nil || requeued.RunnerID != nil || requeued.LeaseDeadline != nil {
		t.Fatalf("expected lease fields cleared after requeue, got %+v", requeued)
	}

	newLeaseID := uuid.New()
	if err := jobs.Lease(ctx, job3.ID, newLeaseID, runnerB.ID, time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatalf("re-lease job3 after requeue: %v", err)
	}
	if newLeaseID == oldLeaseID {
		t.Fatalf("new lease_id must differ from the expired one")
	}
}
