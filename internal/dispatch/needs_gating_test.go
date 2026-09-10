// This file implements the T-M3-01 (needs-gating + skip propagation) and
// T-M3-02 (job_outputs store + assemble needs_outputs) test coverage.
//
// Pure-function unit tests (needsSatisfied, hasDefaultIfExpr, phaseToResult)
// need no DB and always run. The integration tests exercise gateRun/
// AcquireJob/CompleteJob against a real Postgres instance via bufconn, same
// pattern as queue_test.go/service_test.go, and are skipped unless
// DATABASE_URL is set.
package dispatch

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/metadata"

	"github.com/chuan99nd/drassi-platform/internal/config"
	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/store"
	"github.com/chuan99nd/drassi-platform/pkg/dispatchpb"
)

func strPtr(s string) *string { return &s }

// --- pure-function unit tests (no DB) --------------------------------------

func TestNeedsSatisfied(t *testing.T) {
	mk := func(idYAML, status string) *store.Job {
		return &store.Job{JobIDYAML: idYAML, Status: status}
	}

	cases := []struct {
		name            string
		job             *store.Job
		byJobID         map[string][]*store.Job
		wantAllTerminal bool
		wantAnyBlocking bool
	}{
		{
			name:            "no needs is trivially satisfied",
			job:             &store.Job{JobIDYAML: "b", Needs: nil},
			byJobID:         map[string][]*store.Job{},
			wantAllTerminal: true,
			wantAnyBlocking: false,
		},
		{
			name:            "single need still queued: not terminal",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "queued")}},
			wantAllTerminal: false,
			wantAnyBlocking: false,
		},
		{
			name:            "single need leased/running: not terminal",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "running")}},
			wantAllTerminal: false,
			wantAnyBlocking: false,
		},
		{
			name:            "single need success: terminal, not blocking",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "success")}},
			wantAllTerminal: true,
			wantAnyBlocking: false,
		},
		{
			name:            "single need failure: terminal, blocking",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "failure")}},
			wantAllTerminal: true,
			wantAnyBlocking: true,
		},
		{
			name:            "single need cancelled: terminal, blocking",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "cancelled")}},
			wantAllTerminal: true,
			wantAnyBlocking: true,
		},
		{
			name:            "single need skipped: terminal, blocking (propagation)",
			job:             &store.Job{JobIDYAML: "c", Needs: []string{"b"}},
			byJobID:         map[string][]*store.Job{"b": {mk("b", "skipped")}},
			wantAllTerminal: true,
			wantAnyBlocking: true,
		},
		{
			name: "mixed success + skipped needs: terminal, blocking",
			job:  &store.Job{JobIDYAML: "d", Needs: []string{"a", "b"}},
			byJobID: map[string][]*store.Job{
				"a": {mk("a", "success")},
				"b": {mk("b", "skipped")},
			},
			wantAllTerminal: true,
			wantAnyBlocking: true,
		},
		{
			name: "mixed success needs, one still running: not terminal",
			job:  &store.Job{JobIDYAML: "d", Needs: []string{"a", "b"}},
			byJobID: map[string][]*store.Job{
				"a": {mk("a", "success")},
				"b": {mk("b", "running")},
			},
			wantAllTerminal: false,
			wantAnyBlocking: false,
		},
		{
			name:            "need id absent from byJobID: conservatively not terminal",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{},
			wantAllTerminal: false,
			wantAnyBlocking: false,
		},
		{
			name: "all needs success: terminal, not blocking",
			job:  &store.Job{JobIDYAML: "c", Needs: []string{"a", "b"}},
			byJobID: map[string][]*store.Job{
				"a": {mk("a", "success")},
				"b": {mk("b", "success")},
			},
			wantAllTerminal: true,
			wantAnyBlocking: false,
		},
		{
			name:            "matrixed need, 2 cells both success: terminal, not blocking",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "success"), mk("a", "success")}},
			wantAllTerminal: true,
			wantAnyBlocking: false,
		},
		{
			name:            "matrixed need, one cell still running: not terminal",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "success"), mk("a", "running")}},
			wantAllTerminal: false,
			wantAnyBlocking: false,
		},
		{
			name:            "matrixed need, one cell failed: terminal, blocking",
			job:             &store.Job{JobIDYAML: "b", Needs: []string{"a"}},
			byJobID:         map[string][]*store.Job{"a": {mk("a", "success"), mk("a", "failure")}},
			wantAllTerminal: true,
			wantAnyBlocking: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotAllTerminal, gotAnyBlocking := needsSatisfied(tc.job, tc.byJobID)
			if gotAllTerminal != tc.wantAllTerminal || gotAnyBlocking != tc.wantAnyBlocking {
				t.Fatalf("needsSatisfied() = (%v, %v), want (%v, %v)",
					gotAllTerminal, gotAnyBlocking, tc.wantAllTerminal, tc.wantAnyBlocking)
			}
		})
	}
}

func TestHasDefaultIfExpr(t *testing.T) {
	cases := []struct {
		name    string
		ifExpr  *string
		wantDef bool
	}{
		{"nil if_expr", nil, true},
		{"empty if_expr", strPtr(""), true},
		{"success() (act's persisted default)", strPtr("success()"), true},
		{"always() is an explicit override", strPtr("always()"), false},
		{"failure() is an explicit override", strPtr("failure()"), false},
		{"custom expression is an explicit override", strPtr("github.event_name == 'push'"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := &store.Job{IfExpr: tc.ifExpr}
			if got := hasDefaultIfExpr(job); got != tc.wantDef {
				t.Fatalf("hasDefaultIfExpr(if_expr=%v) = %v, want %v", tc.ifExpr, got, tc.wantDef)
			}
		})
	}
}

func TestPhaseToResult_RoundTrips(t *testing.T) {
	cases := []struct {
		phase dispatchpb.Phase
		want  string
	}{
		{dispatchpb.Phase_PHASE_SUCCESS, "success"},
		{dispatchpb.Phase_PHASE_FAILURE, "failure"},
		{dispatchpb.Phase_PHASE_SKIPPED, "skipped"},
		{dispatchpb.Phase_PHASE_CANCELLED, "cancelled"},
	}
	for _, tc := range cases {
		got, err := phaseToResult(tc.phase)
		if err != nil {
			t.Fatalf("phaseToResult(%v): %v", tc.phase, err)
		}
		if got != tc.want {
			t.Fatalf("phaseToResult(%v) = %q, want %q", tc.phase, got, tc.want)
		}
		if !isTerminalStatus(got) {
			t.Fatalf("phaseToResult(%v) = %q, expected a terminal status", tc.phase, got)
		}
	}
}

// --- integration tests (real Postgres + bufconn) ----------------------------

// needsGatingFixture wires a Service against a real Postgres pool plus the
// small set of store handles the tests below need directly, mirroring the
// fixture setup already used by queue_test.go / service_test.go.
type needsGatingFixture struct {
	t       *testing.T
	ctx     context.Context
	pool    *pgxpool.Pool
	jobs    store.JobStore
	runs    store.RunStore
	outputs store.JobOutputStore
	svc     *Service
	runner  *store.Runner
}

func newNeedsGatingFixture(t *testing.T) *needsGatingFixture {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping needs-gating integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

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

	cfg := config.Config{
		HTTPAddr:                 ":0",
		GRPCAddr:                 ":0",
		DBDSN:                    dsn,
		HeartbeatIntervalSeconds: 10,
		HeartbeatMissLimit:       3,
	}
	svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)

	runnerID := uuid.New()
	runner := &store.Runner{
		ID:        runnerID,
		Name:      "needs-gating-runner-" + runnerID.String()[:8],
		Labels:    []string{"self-hosted"},
		Mode:      "host",
		Capacity:  4,
		Version:   "test",
		Status:    "online",
		AuthToken: "test-token-" + runnerID.String(),
	}
	if err := runners.Create(ctx, runner); err != nil {
		t.Fatalf("runners.Create: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		if _, err := pool.Exec(cCtx, `DELETE FROM runners WHERE id = $1`, runnerID); err != nil {
			t.Logf("cleanup runner: %v", err)
		}
	})

	return &needsGatingFixture{t: t, ctx: ctx, pool: pool, jobs: jobs, runs: runs, outputs: outputs, svc: svc, runner: runner}
}

func (f *needsGatingFixture) newRun() uuid.UUID {
	f.t.Helper()
	runID := uuid.New()
	run := &store.Run{
		ID:          runID,
		Repo:        "octo/example",
		Ref:         "refs/heads/main",
		SHA:         "deadbeef",
		Event:       "push",
		TriggeredBy: "octocat",
		Status:      "running",
	}
	if err := f.runs.CreateRun(f.ctx, run); err != nil {
		f.t.Fatalf("runs.CreateRun: %v", err)
	}
	f.t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		if _, err := f.pool.Exec(cCtx, `DELETE FROM workflow_runs WHERE id = $1`, runID); err != nil {
			f.t.Logf("cleanup run %s: %v", runID, err)
		}
	})
	return runID
}

func (f *needsGatingFixture) newJob(runID uuid.UUID, jobIDYAML string, needs []string, ifExpr *string) *store.Job {
	f.t.Helper()
	j := &store.Job{
		ID:        uuid.New(),
		RunID:     runID,
		JobIDYAML: jobIDYAML,
		Name:      jobIDYAML,
		Needs:     needs,
		IfExpr:    ifExpr,
		Status:    "queued",
	}
	if err := f.jobs.CreateJob(f.ctx, j); err != nil {
		f.t.Fatalf("CreateJob(%s): %v", jobIDYAML, err)
	}
	return j
}

// newMatrixCellJob is newJob plus an explicit display name and
// matrix_cell_json, so a test can create several rows sharing one
// jobIDYAML (T-M4-01's "one jobs row per matrix cell") to exercise the
// group-by-job_id_yaml gating semantics in needsSatisfied/gateRun/
// acquireOneQueuedJob.
func (f *needsGatingFixture) newMatrixCellJob(runID uuid.UUID, jobIDYAML, name string, cellJSON []byte, needs []string, ifExpr *string) *store.Job {
	f.t.Helper()
	j := &store.Job{
		ID:             uuid.New(),
		RunID:          runID,
		JobIDYAML:      jobIDYAML,
		Name:           name,
		MatrixCellJSON: cellJSON,
		Needs:          needs,
		IfExpr:         ifExpr,
		Status:         "queued",
	}
	if err := f.jobs.CreateJob(f.ctx, j); err != nil {
		f.t.Fatalf("CreateJob(%s/%s): %v", jobIDYAML, name, err)
	}
	return j
}

func (f *needsGatingFixture) authCtx() context.Context {
	return metadata.NewIncomingContext(f.ctx, metadata.Pairs("authorization", "Bearer "+f.runner.AuthToken))
}

// completeJob leases jobID directly (bypassing AcquireJob, since some
// scenarios below deliberately want a job leased before its needs are even
// relevant to this test) and drives it straight to CompleteJob with the
// given phase/outputs -- this is the same "Lease then ReportStatus/
// CompleteJob" pattern service_test.go uses.
func (f *needsGatingFixture) completeJob(jobID uuid.UUID, phase dispatchpb.Phase, outputs map[string]string) {
	f.t.Helper()
	leaseID := uuid.New()
	if err := f.jobs.Lease(f.ctx, jobID, leaseID, f.runner.ID, time.Now().UTC().Add(time.Minute)); err != nil {
		f.t.Fatalf("Lease(%s): %v", jobID, err)
	}
	if _, err := f.svc.CompleteJob(f.authCtx(), &dispatchpb.CompleteJobRequest{
		LeaseId: leaseID.String(),
		Result:  phase,
		Outputs: outputs,
	}); err != nil {
		f.t.Fatalf("CompleteJob(%s): %v", jobID, err)
	}
}

// TestNeedsGating_BlocksUntilNeedTerminal_ThenAssemblesNeedsOutputs covers
// T-M3-01's core gating predicate plus T-M3-02's needs_outputs assembly: job
// B (needs: [A]) must never be leased while A is non-terminal, and once A
// completes with result=success and outputs, B becomes leasable with
// needs_outputs["A"] carrying A's {result, outputs}.
func TestNeedsGating_BlocksUntilNeedTerminal_ThenAssemblesNeedsOutputs(t *testing.T) {
	f := newNeedsGatingFixture(t)

	runID := f.newRun()
	jobA := f.newJob(runID, "A", nil, nil)
	jobB := f.newJob(runID, "B", []string{"A"}, nil)

	grpcServer, client, closeClient := startBufconnServer(t, f.svc)
	defer grpcServer.Stop()
	defer closeClient()

	authCtx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer "+f.runner.AuthToken)
	acquireOne := func(ctx context.Context) (*dispatchpb.JobLease, error) {
		stream, err := client.AcquireJob(ctx, &dispatchpb.AcquireJobRequest{
			RunnerId:  f.runner.ID.String(),
			FreeSlots: 1,
		})
		if err != nil {
			return nil, err
		}
		return stream.Recv()
	}

	// --- A is queued: the only leasable candidate is A itself (B's need is
	// not yet terminal) -------------------------------------------------
	lease, err := acquireOne(authCtx)
	if err != nil {
		t.Fatalf("AcquireJob (expect A): %v", err)
	}
	if lease.GetJobRunId() != jobA.ID.String() {
		t.Fatalf("expected job A to be leased first (B's need not terminal), got job_run_id=%s", lease.GetJobRunId())
	}

	gotB, err := f.jobs.GetJob(f.ctx, jobB.ID)
	if err != nil {
		t.Fatalf("GetJob(B): %v", err)
	}
	if gotB.Status != "queued" {
		t.Fatalf("expected B to remain queued while A is non-terminal, got %q", gotB.Status)
	}

	// --- while A is leased (not yet terminal), an AcquireJob call has
	// nothing to lease and must block rather than return B early. ---------
	shortCtx, shortCancel := context.WithTimeout(f.ctx, 700*time.Millisecond)
	defer shortCancel()
	shortAuthCtx := metadata.AppendToOutgoingContext(shortCtx, "authorization", "Bearer "+f.runner.AuthToken)
	if _, err := acquireOne(shortAuthCtx); err == nil {
		t.Fatalf("expected AcquireJob to block with nothing leasable while A is non-terminal, but it returned a lease")
	}

	// --- drive A to running, then complete it as success with outputs ----
	if _, err := f.svc.ReportStatus(f.authCtx(), &dispatchpb.StatusUpdate{
		LeaseId: lease.GetLeaseId(),
		Phase:   dispatchpb.Phase_PHASE_RUNNING,
		TsUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("ReportStatus (A running): %v", err)
	}

	// A is running (non-terminal): B must still not be leasable.
	shortCtx2, shortCancel2 := context.WithTimeout(f.ctx, 700*time.Millisecond)
	defer shortCancel2()
	shortAuthCtx2 := metadata.AppendToOutgoingContext(shortCtx2, "authorization", "Bearer "+f.runner.AuthToken)
	if _, err := acquireOne(shortAuthCtx2); err == nil {
		t.Fatalf("expected AcquireJob to still block while A is running, but it returned a lease")
	}

	if _, err := f.svc.CompleteJob(f.authCtx(), &dispatchpb.CompleteJobRequest{
		LeaseId: lease.GetLeaseId(),
		Result:  dispatchpb.Phase_PHASE_SUCCESS,
		Outputs: map[string]string{"x": "42"},
	}); err != nil {
		t.Fatalf("CompleteJob (A success): %v", err)
	}

	// --- A is now terminal (success): B must become leasable, and its
	// lease's needs_outputs["A"] must carry A's result + outputs. ---------
	leaseB, err := acquireOne(authCtx)
	if err != nil {
		t.Fatalf("AcquireJob (expect B, after A success): %v", err)
	}
	if leaseB.GetJobRunId() != jobB.ID.String() {
		t.Fatalf("expected job B to be leased once A succeeded, got job_run_id=%s", leaseB.GetJobRunId())
	}
	needA := leaseB.GetNeedsOutputs()["A"]
	if needA == nil {
		t.Fatalf("expected needs_outputs[%q] to be populated, got %+v", "A", leaseB.GetNeedsOutputs())
	}
	if needA.GetResult() != "success" {
		t.Fatalf("expected needs_outputs[A].result = success, got %q", needA.GetResult())
	}
	if needA.GetOutputs()["x"] != "42" {
		t.Fatalf("expected needs_outputs[A].outputs[x] = 42, got %+v", needA.GetOutputs())
	}

	// Cross-check directly against the job_outputs store (T-M3-02 DoD).
	gotOutputs, err := f.outputs.ListOutputs(f.ctx, jobA.ID)
	if err != nil {
		t.Fatalf("ListOutputs(A): %v", err)
	}
	if gotOutputs["x"] != "42" {
		t.Fatalf("expected job_outputs[A][x] = 42, got %+v", gotOutputs)
	}
}

// TestNeedsGating_SkipPropagation_RunFailure covers T-M3-01's skip-by-rule +
// transitive propagation branch: A fails, B (needs:[A], no if: override) is
// marked skipped without ever being dispatched, and C (needs:[B]) cascades
// to skipped too. The run rolls up to failure (any job failure), per point 3
// of the task ("skipped counts as terminal/non-failing" does not mean the
// run itself can't still fail because of the real upstream failure).
func TestNeedsGating_SkipPropagation_RunFailure(t *testing.T) {
	f := newNeedsGatingFixture(t)

	runID := f.newRun()
	jobA := f.newJob(runID, "A", nil, nil)
	jobB := f.newJob(runID, "B", []string{"A"}, nil)                 // no if: override
	jobC := f.newJob(runID, "C", []string{"B"}, strPtr("success()")) // explicit default, same as nil

	// Complete A as failure -- CompleteJob hooks gateRun, which must
	// transitively skip B then C in the same call (fixpoint).
	f.completeJob(jobA.ID, dispatchpb.Phase_PHASE_FAILURE, nil)

	gotB, err := f.jobs.GetJob(f.ctx, jobB.ID)
	if err != nil {
		t.Fatalf("GetJob(B): %v", err)
	}
	if gotB.Status != "skipped" {
		t.Fatalf("expected B skipped (A failed, no if: override), got %q", gotB.Status)
	}
	if gotB.Result == nil || *gotB.Result != "skipped" {
		t.Fatalf("expected B.result = skipped, got %+v", gotB.Result)
	}

	gotC, err := f.jobs.GetJob(f.ctx, jobC.ID)
	if err != nil {
		t.Fatalf("GetJob(C): %v", err)
	}
	if gotC.Status != "skipped" {
		t.Fatalf("expected C transitively skipped (B skipped, no if: override), got %q", gotC.Status)
	}
	if gotC.Result == nil || *gotC.Result != "skipped" {
		t.Fatalf("expected C.result = skipped, got %+v", gotC.Result)
	}

	// Neither B nor C was ever dispatched: both must still show no
	// runner_id/lease_id (skipJob never leases).
	if gotB.RunnerID != nil || gotB.LeaseID != nil {
		t.Fatalf("expected B to never be leased, got runner_id=%v lease_id=%v", gotB.RunnerID, gotB.LeaseID)
	}
	if gotC.RunnerID != nil || gotC.LeaseID != nil {
		t.Fatalf("expected C to never be leased, got runner_id=%v lease_id=%v", gotC.RunnerID, gotC.LeaseID)
	}

	// AcquireJob must have nothing to lease at all now (A terminal, B/C
	// skipped): confirm it blocks rather than handing out B or C.
	grpcServer, client, closeClient := startBufconnServer(t, f.svc)
	defer grpcServer.Stop()
	defer closeClient()

	shortCtx, shortCancel := context.WithTimeout(f.ctx, 700*time.Millisecond)
	defer shortCancel()
	stream, err := client.AcquireJob(
		metadata.AppendToOutgoingContext(shortCtx, "authorization", "Bearer "+f.runner.AuthToken),
		&dispatchpb.AcquireJobRequest{RunnerId: f.runner.ID.String(), FreeSlots: 1},
	)
	if err != nil {
		t.Fatalf("AcquireJob: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatalf("expected AcquireJob to find nothing leasable (B/C skipped, not dispatched), but it returned a lease")
	}

	// Run rolls up to failure (A's real failure), even though B/C are a
	// clean "skipped" outcome.
	gotRun, err := f.runs.GetRun(f.ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != "failure" {
		t.Fatalf("expected run status failure (A failed), got %q", gotRun.Status)
	}
}

// TestNeedsGating_IfOverride_NotAutoSkipped covers the T-M3-01 escape hatch:
// a job whose needs failed but which carries a non-default if: expression
// (e.g. always()) must NOT be auto-skipped by gateRun/AcquireJob -- it stays
// queued and becomes a normal leasable candidate once its needs are
// terminal, routed to the runner for T-M3-03's pre-evaluation instead.
func TestNeedsGating_IfOverride_NotAutoSkipped(t *testing.T) {
	f := newNeedsGatingFixture(t)

	runID := f.newRun()
	jobA := f.newJob(runID, "A", nil, nil)
	jobB := f.newJob(runID, "B", []string{"A"}, strPtr("always()"))

	f.completeJob(jobA.ID, dispatchpb.Phase_PHASE_FAILURE, nil)

	gotB, err := f.jobs.GetJob(f.ctx, jobB.ID)
	if err != nil {
		t.Fatalf("GetJob(B): %v", err)
	}
	if gotB.Status != "queued" {
		t.Fatalf("expected B (if: always()) to remain queued/leasable despite A's failure, got %q", gotB.Status)
	}

	// B must now be a normal leasable candidate (routed to the runner path).
	grpcServer, client, closeClient := startBufconnServer(t, f.svc)
	defer grpcServer.Stop()
	defer closeClient()

	stream, err := client.AcquireJob(
		metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer "+f.runner.AuthToken),
		&dispatchpb.AcquireJobRequest{RunnerId: f.runner.ID.String(), FreeSlots: 1},
	)
	if err != nil {
		t.Fatalf("AcquireJob: %v", err)
	}
	lease, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected B to be leased (if: override routes to runner), got error: %v", err)
	}
	if lease.GetJobRunId() != jobB.ID.String() {
		t.Fatalf("expected leased job to be B, got job_run_id=%s", lease.GetJobRunId())
	}
	// Its needs_outputs["A"] must still be populated (failure result) so the
	// runner can evaluate always()/failure() against it.
	needA := lease.GetNeedsOutputs()["A"]
	if needA == nil || needA.GetResult() != "failure" {
		t.Fatalf("expected needs_outputs[A].result = failure, got %+v", lease.GetNeedsOutputs())
	}
}

// TestNeedsGating_RunRollup_SuccessPlusSkipped covers point 3 of the task:
// CompleteJob must accept result=skipped as terminal (a runner reporting a
// job it decided not to run after its own if: pre-eval), and the run
// roll-up must treat an all-success-or-skipped run as success, not failure
// or cancelled.
func TestNeedsGating_RunRollup_SuccessPlusSkipped(t *testing.T) {
	f := newNeedsGatingFixture(t)

	runID := f.newRun()
	jobD := f.newJob(runID, "D", nil, nil)
	jobE := f.newJob(runID, "E", nil, strPtr("github.event_name == 'nightly'")) // if: override; runner decides

	f.completeJob(jobD.ID, dispatchpb.Phase_PHASE_SUCCESS, nil)

	gotD, err := f.jobs.GetJob(f.ctx, jobD.ID)
	if err != nil {
		t.Fatalf("GetJob(D): %v", err)
	}
	if gotD.Status != "success" {
		t.Fatalf("expected D success, got %q", gotD.Status)
	}

	// Run not finalized yet: E still outstanding.
	gotRun, err := f.runs.GetRun(f.ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != "running" {
		t.Fatalf("expected run to remain running with E outstanding, got %q", gotRun.Status)
	}

	// Runner reports E as skipped (its own if: pre-eval decided not to run
	// it) -- CompleteJob must accept PHASE_SKIPPED as a terminal result.
	f.completeJob(jobE.ID, dispatchpb.Phase_PHASE_SKIPPED, nil)

	gotE, err := f.jobs.GetJob(f.ctx, jobE.ID)
	if err != nil {
		t.Fatalf("GetJob(E): %v", err)
	}
	if gotE.Status != "skipped" || gotE.Result == nil || *gotE.Result != "skipped" {
		t.Fatalf("expected E status/result = skipped, got status=%q result=%+v", gotE.Status, gotE.Result)
	}

	gotRun, err = f.runs.GetRun(f.ctx, runID)
	if err != nil {
		t.Fatalf("GetRun (after both terminal): %v", err)
	}
	if gotRun.Status != "success" {
		t.Fatalf("expected run status success (success + skipped, no failure), got %q", gotRun.Status)
	}
}

// TestNeedsGating_MatrixNeed_BlocksUntilAllCellsTerminal covers T-M4-01's
// matrix-aware gating (point 2 of the task): a matrixed job `build` expanded
// into two jobs rows sharing job_id_yaml "build" (T-M4-01's one-row-per-cell
// planner output, simulated directly here via newMatrixCellJob since this
// package's scope is the dispatcher, not the planner) gates a `deploy` job
// (needs: [build]) only once BOTH cells are terminal -- completing just one
// cell must not unblock deploy. It also exercises T-M4-01 point 3's
// canonical-cell rule: once both cells succeed, needs_outputs["build"] must
// carry the LAST-COMPLETED cell's outputs.
func TestNeedsGating_MatrixNeed_BlocksUntilAllCellsTerminal(t *testing.T) {
	f := newNeedsGatingFixture(t)

	runID := f.newRun()
	buildOS1 := f.newMatrixCellJob(runID, "build", "build (ubuntu-latest)", []byte(`{"os":"ubuntu-latest"}`), nil, nil)
	buildOS2 := f.newMatrixCellJob(runID, "build", "build (macos-latest)", []byte(`{"os":"macos-latest"}`), nil, nil)
	deploy := f.newJob(runID, "deploy", []string{"build"}, nil)

	// Pure-function sanity check: with only one of the two build cells
	// terminal, needsSatisfied must report the "build" group as not yet
	// allTerminal (this is the exact assumption the dispatcher's
	// acquireOneQueuedJob/gateRun rely on for the live assertions below).
	oneCellTerminal := map[string][]*store.Job{
		"build": {{JobIDYAML: "build", Status: "success"}, {JobIDYAML: "build", Status: "queued"}},
	}
	if allTerminal, _ := needsSatisfied(deploy, oneCellTerminal); allTerminal {
		t.Fatalf("expected needsSatisfied to report the build group as not allTerminal with one cell still queued")
	}

	// Complete only the first build cell as success -- deploy must remain
	// queued because the second cell is still outstanding.
	f.completeJob(buildOS1.ID, dispatchpb.Phase_PHASE_SUCCESS, map[string]string{"artifact": "a1"})

	gotDeploy, err := f.jobs.GetJob(f.ctx, deploy.ID)
	if err != nil {
		t.Fatalf("GetJob(deploy): %v", err)
	}
	if gotDeploy.Status != "queued" {
		t.Fatalf("expected deploy to remain queued with one build cell still non-terminal, got %q", gotDeploy.Status)
	}

	// A short, deliberate gap so the two cells' finished_at values are
	// unambiguously ordered (avoids a same-instant tie in
	// canonicalNeedRow's finished_at comparison making this test flaky).
	time.Sleep(10 * time.Millisecond)

	// Complete the second build cell as success too -- deploy's needs group
	// (both "build" rows) is now entirely terminal and all-success.
	f.completeJob(buildOS2.ID, dispatchpb.Phase_PHASE_SUCCESS, map[string]string{"artifact": "a2"})

	grpcServer, client, closeClient := startBufconnServer(t, f.svc)
	defer grpcServer.Stop()
	defer closeClient()

	stream, err := client.AcquireJob(
		metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer "+f.runner.AuthToken),
		&dispatchpb.AcquireJobRequest{RunnerId: f.runner.ID.String(), FreeSlots: 1},
	)
	if err != nil {
		t.Fatalf("AcquireJob: %v", err)
	}
	lease, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected deploy to be leased once both build cells succeeded: %v", err)
	}
	if lease.GetJobRunId() != deploy.ID.String() {
		t.Fatalf("expected leased job to be deploy, got job_run_id=%s", lease.GetJobRunId())
	}

	needBuild := lease.GetNeedsOutputs()["build"]
	if needBuild == nil || needBuild.GetResult() != "success" {
		t.Fatalf("expected needs_outputs[build].result = success, got %+v", lease.GetNeedsOutputs())
	}
	if needBuild.GetOutputs()["artifact"] != "a2" {
		t.Fatalf("expected needs_outputs[build] to carry the last-completed cell's outputs (a2), got %+v", needBuild.GetOutputs())
	}
}

// TestNeedsGating_MatrixNeed_OneCellFails_DependentSkipped covers T-M4-01's
// matrix-aware "blocking" rule: once ANY row in a matrixed need's group ends
// non-success, a dependent with the default if: is skipped by rule --
// generalizing the plain (single-row) skip-propagation behavior in
// TestNeedsGating_SkipPropagation_RunFailure to "any cell in the group",
// not "the one row".
func TestNeedsGating_MatrixNeed_OneCellFails_DependentSkipped(t *testing.T) {
	f := newNeedsGatingFixture(t)

	runID := f.newRun()
	buildOS1 := f.newMatrixCellJob(runID, "build", "build (ubuntu-latest)", []byte(`{"os":"ubuntu-latest"}`), nil, nil)
	buildOS2 := f.newMatrixCellJob(runID, "build", "build (macos-latest)", []byte(`{"os":"macos-latest"}`), nil, nil)
	deploy := f.newJob(runID, "deploy", []string{"build"}, nil) // default if:, no override

	f.completeJob(buildOS1.ID, dispatchpb.Phase_PHASE_SUCCESS, nil)

	// buildOS2 is still non-terminal: deploy must still be queued even
	// though one of its two needed cells already succeeded.
	gotDeploy, err := f.jobs.GetJob(f.ctx, deploy.ID)
	if err != nil {
		t.Fatalf("GetJob(deploy): %v", err)
	}
	if gotDeploy.Status != "queued" {
		t.Fatalf("expected deploy to remain queued with one build cell still non-terminal, got %q", gotDeploy.Status)
	}

	f.completeJob(buildOS2.ID, dispatchpb.Phase_PHASE_FAILURE, nil)

	gotDeploy, err = f.jobs.GetJob(f.ctx, deploy.ID)
	if err != nil {
		t.Fatalf("GetJob(deploy) after build cell failure: %v", err)
	}
	if gotDeploy.Status != "skipped" {
		t.Fatalf("expected deploy skipped (one build cell failed, no if: override), got %q", gotDeploy.Status)
	}
	if gotDeploy.Result == nil || *gotDeploy.Result != "skipped" {
		t.Fatalf("expected deploy.result = skipped, got %+v", gotDeploy.Result)
	}

	// The successful sibling cell must be unaffected by the other cell's
	// failure -- matrix cells fail/succeed independently (MVP: no
	// fail-fast/cancel-siblings, per the task's "Out of scope").
	gotBuild1, err := f.jobs.GetJob(f.ctx, buildOS1.ID)
	if err != nil {
		t.Fatalf("GetJob(buildOS1): %v", err)
	}
	if gotBuild1.Status != "success" {
		t.Fatalf("expected buildOS1 to remain success despite buildOS2's failure, got %q", gotBuild1.Status)
	}

	gotRun, err := f.runs.GetRun(f.ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != "failure" {
		t.Fatalf("expected run status failure (one build cell failed), got %q", gotRun.Status)
	}
}
