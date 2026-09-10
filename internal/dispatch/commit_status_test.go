package dispatch

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	"github.com/chuan99nd/drassi-platform/internal/config"
	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/store"
	"github.com/chuan99nd/drassi-platform/pkg/dispatchpb"
)

// fakeCommitStatusReporter is a test double for the commitStatus field
// (T-M2-03): it satisfies the same structural interface as
// *github.CommitStatusReporter without importing the github package, and
// records every call so tests can assert on how finalizeRunIfComplete used
// it.
type fakeCommitStatusReporter struct {
	mu    sync.Mutex
	calls []fakeCommitStatusCall
	err   error // returned from ReportStatus if non-nil, to exercise the best-effort path
}

type fakeCommitStatusCall struct {
	repo, sha, state, description string
}

func (f *fakeCommitStatusReporter) ReportStatus(ctx context.Context, repo, sha, state, description string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCommitStatusCall{repo: repo, sha: sha, state: state, description: description})
	return f.err
}

func (f *fakeCommitStatusReporter) snapshot() []fakeCommitStatusCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeCommitStatusCall, len(f.calls))
	copy(out, f.calls)
	return out
}

// authContextFor mirrors service_test.go's inline authCtx helper: it wraps
// base with the gRPC incoming-metadata a runner's Bearer auth_token would
// carry, so ReportStatus/CompleteJob's authorizeJobRunner check passes.
func authContextFor(base context.Context, authToken string) context.Context {
	return metadata.NewIncomingContext(base, metadata.Pairs("authorization", "Bearer "+authToken))
}

// TestCommitStatusForRunStatus_Mapping is a pure unit test (no DB) of the
// terminal workflow_runs.status -> GitHub state mapping used by
// reportCommitStatus.
func TestCommitStatusForRunStatus_Mapping(t *testing.T) {
	cases := []struct {
		runStatus string
		wantState string
	}{
		{"success", "success"},
		{"failure", "failure"},
		{"cancelled", "error"}, // documented choice: neither a clean pass nor a job failure
	}
	for _, tc := range cases {
		gotState, gotDesc := commitStatusForRunStatus(tc.runStatus)
		if gotState != tc.wantState {
			t.Errorf("commitStatusForRunStatus(%q) state = %q, want %q", tc.runStatus, gotState, tc.wantState)
		}
		if gotDesc == "" {
			t.Errorf("commitStatusForRunStatus(%q) description = %q, want non-empty", tc.runStatus, gotDesc)
		}
	}
}

// TestFinalizeRunIfComplete_CommitStatus exercises the T-M2-03 hook end to
// end against a real Postgres instance (skipped unless DATABASE_URL is
// set): a push (webhook) run's terminal status is reported through the
// installed reporter, a manual run's is not, and a Service with no reporter
// installed at all (the zero value / pre-T-M2-03 behavior) still finalizes
// cleanly.
func TestFinalizeRunIfComplete_CommitStatus(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping dispatch/commit-status integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	cfg := config.Config{
		HTTPAddr:                 ":0",
		GRPCAddr:                 ":0",
		DBDSN:                    dsn,
		HeartbeatIntervalSeconds: 10,
		HeartbeatMissLimit:       3,
	}

	newRunner := func(t *testing.T) *store.Runner {
		t.Helper()
		id := uuid.New()
		r := &store.Runner{
			ID:        id,
			Name:      "test-runner-" + id.String()[:8],
			Labels:    []string{"self-hosted"},
			Mode:      "host",
			Capacity:  2,
			Version:   "test",
			Status:    "online",
			AuthToken: "test-token-" + id.String(),
		}
		if err := runners.Create(ctx, r); err != nil {
			t.Fatalf("runners.Create: %v", err)
		}
		t.Cleanup(func() {
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cCancel()
			if _, err := pool.Exec(cCtx, `DELETE FROM runners WHERE id = $1`, id); err != nil {
				t.Logf("cleanup runner: %v", err)
			}
		})
		return r
	}

	// completeSingleJobRun creates a run with a single job, leases + reports
	// it running, then CompleteJobs it with result (a terminal
	// dispatchpb.Phase), which drives finalizeRunIfComplete via the same
	// path production code uses (CompleteJob -> finalizeRunIfComplete).
	completeSingleJobRun := func(t *testing.T, svc *Service, runner *store.Runner, event, repo, sha string, result dispatchpb.Phase) uuid.UUID {
		t.Helper()

		runID := uuid.New()
		run := &store.Run{
			ID:          runID,
			Repo:        repo,
			Ref:         "refs/heads/main",
			SHA:         sha,
			Event:       event,
			TriggeredBy: "octocat",
			Status:      "running",
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

		jobID := uuid.New()
		job := &store.Job{ID: jobID, RunID: runID, JobIDYAML: "build", Name: "build", Status: "queued"}
		if err := jobs.CreateJob(ctx, job); err != nil {
			t.Fatalf("CreateJob: %v", err)
		}

		authCtx := authContextFor(ctx, runner.AuthToken)

		lease := uuid.New()
		if err := jobs.Lease(ctx, jobID, lease, runner.ID, time.Now().UTC().Add(time.Minute)); err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if _, err := svc.ReportStatus(authCtx, &dispatchpb.StatusUpdate{
			LeaseId: lease.String(),
			Phase:   dispatchpb.Phase_PHASE_RUNNING,
			TsUnix:  time.Now().Unix(),
		}); err != nil {
			t.Fatalf("ReportStatus running: %v", err)
		}
		if _, err := svc.CompleteJob(authCtx, &dispatchpb.CompleteJobRequest{
			LeaseId: lease.String(),
			Result:  result,
		}); err != nil {
			t.Fatalf("CompleteJob: %v", err)
		}
		return runID
	}

	t.Run("webhook push run reports success to the installed reporter", func(t *testing.T) {
		svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)
		reporter := &fakeCommitStatusReporter{}
		svc.SetCommitStatusReporter(reporter)
		runner := newRunner(t)

		runID := completeSingleJobRun(t, svc, runner, "push", "octo/webhook-repo", "cafef00d1234", dispatchpb.Phase_PHASE_SUCCESS)

		gotRun, err := runs.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Status != "success" {
			t.Fatalf("run status = %q, want success", gotRun.Status)
		}

		calls := reporter.snapshot()
		if len(calls) != 1 {
			t.Fatalf("reporter calls = %d, want 1: %+v", len(calls), calls)
		}
		got := calls[0]
		if got.repo != "octo/webhook-repo" || got.sha != "cafef00d1234" || got.state != "success" {
			t.Fatalf("unexpected reporter call: %+v", got)
		}
	})

	t.Run("webhook push run reports failure to the installed reporter", func(t *testing.T) {
		svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)
		reporter := &fakeCommitStatusReporter{}
		svc.SetCommitStatusReporter(reporter)
		runner := newRunner(t)

		completeSingleJobRun(t, svc, runner, "push", "octo/webhook-repo2", "deadbeef5678", dispatchpb.Phase_PHASE_FAILURE)

		calls := reporter.snapshot()
		if len(calls) != 1 || calls[0].state != "failure" {
			t.Fatalf("expected exactly one failure call, got %+v", calls)
		}
	})

	t.Run("manual run does not call the reporter", func(t *testing.T) {
		svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)
		reporter := &fakeCommitStatusReporter{}
		svc.SetCommitStatusReporter(reporter)
		runner := newRunner(t)

		completeSingleJobRun(t, svc, runner, "manual", "octo/manual-repo", "manualsha0001", dispatchpb.Phase_PHASE_SUCCESS)

		if calls := reporter.snapshot(); len(calls) != 0 {
			t.Fatalf("manual run must not report a commit status, got %+v", calls)
		}
	})

	t.Run("no reporter installed: finalize still succeeds", func(t *testing.T) {
		svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg) // no SetCommitStatusReporter call
		runner := newRunner(t)

		runID := completeSingleJobRun(t, svc, runner, "push", "octo/no-reporter-repo", "nofacesha0001", dispatchpb.Phase_PHASE_SUCCESS)

		gotRun, err := runs.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Status != "success" {
			t.Fatalf("run status = %q, want success (finalize must not be broken by a nil reporter)", gotRun.Status)
		}
	})

	t.Run("reporter error is swallowed (best-effort, does not fail finalize)", func(t *testing.T) {
		svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)
		reporter := &fakeCommitStatusReporter{err: errors.New("simulated GitHub API failure")}
		svc.SetCommitStatusReporter(reporter)
		runner := newRunner(t)

		runID := completeSingleJobRun(t, svc, runner, "push", "octo/erroring-reporter-repo", "erroringsha01", dispatchpb.Phase_PHASE_SUCCESS)

		gotRun, err := runs.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}
		if gotRun.Status != "success" {
			t.Fatalf("run status = %q, want success (a reporter error must not affect the run's own status)", gotRun.Status)
		}
		if calls := reporter.snapshot(); len(calls) != 1 {
			t.Fatalf("expected the reporter to still be called once despite returning an error, got %+v", calls)
		}
	})
}
