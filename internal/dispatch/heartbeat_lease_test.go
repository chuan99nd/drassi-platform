// This file implements the T-M1-13 test coverage: Heartbeat renews the
// lease_deadline of every lease a runner reports as active
// (HeartbeatPing.active_lease_ids), scoped to leases that runner actually
// holds, and a job that runs longer than leaseTTL() survives (is leased
// exactly once, never requeued mid-run) as long as heartbeats keep renewing
// it.
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

// heartbeatOnce opens a fresh Heartbeat stream authenticated as token, sends
// a single HeartbeatPing carrying activeLeaseIDs, waits for the ack, and
// closes the stream. Using one short-lived stream per ping (rather than one
// long-lived stream for the whole test) keeps each call's server-side
// processing easy to sequence against with plain, synchronous test code.
func heartbeatOnce(t *testing.T, client dispatchpb.DispatchClient, token, runnerID string, activeLeaseIDs []string) {
	t.Helper()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
	stream, err := client.Heartbeat(ctx)
	if err != nil {
		t.Fatalf("Heartbeat: open stream: %v", err)
	}
	if err := stream.Send(&dispatchpb.HeartbeatPing{
		RunnerId:       runnerID,
		ActiveLeaseIds: activeLeaseIDs,
	}); err != nil {
		t.Fatalf("Heartbeat: send ping: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Heartbeat: recv ack: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("Heartbeat: close send: %v", err)
	}
}

// TestDispatchService_HeartbeatRenewsLease proves the core T-M1-13 contract
// against a real Postgres instance and a real (bufconn) gRPC Heartbeat
// stream:
//   - a runner reporting its own active lease id on a Heartbeat gets that
//     lease's lease_deadline pushed forward via JobStore.RenewLease;
//   - a DIFFERENT runner reporting someone else's lease id does NOT get it
//     renewed (ownership is validated by resolving lease -> job -> runner_id,
//     mirroring authorizeJobRunner);
//   - an unknown/garbage lease id in active_lease_ids is ignored without
//     erroring the Heartbeat RPC (last_heartbeat still advances normally).
//
// Skipped unless DATABASE_URL is set.
func TestDispatchService_HeartbeatRenewsLease(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping dispatch integration test")
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
		HeartbeatMissLimit:       3, // leaseTTL() = 30s
	}
	svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)

	newRunner := func(name string) *store.Runner {
		id := uuid.New()
		r := &store.Runner{
			ID:        id,
			Name:      name + "-" + id.String()[:8],
			Labels:    []string{"self-hosted"},
			Mode:      "host",
			Capacity:  1,
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

	// Lease the job to runnerA with a deadline already in the past -- proof
	// that RenewLease (not the deadline's current value) is what matters:
	// only the job's status ('leased') gates the conditional UPDATE.
	leaseID := uuid.New()
	originalDeadline := time.Now().UTC().Add(-time.Minute)
	if err := jobs.Lease(ctx, jobID, leaseID, runnerA.ID, originalDeadline); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	_, client, closeClient := startBufconnServer(t, svc)
	defer closeClient()

	// --- runnerA heartbeats reporting its own lease: must be renewed -------

	beforeA := time.Now().UTC()
	heartbeatOnce(t, client, runnerA.AuthToken, runnerA.ID.String(), []string{leaseID.String()})

	gotJob, err := jobs.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJob after runnerA heartbeat: %v", err)
	}
	if gotJob.LeaseDeadline == nil {
		t.Fatalf("expected lease_deadline to be set after renewal, got nil")
	}
	if !gotJob.LeaseDeadline.After(originalDeadline) {
		t.Fatalf("expected lease_deadline to advance past %s, got %s", originalDeadline, gotJob.LeaseDeadline)
	}
	// The renewed deadline should be roughly now + leaseTTL() (30s): assert
	// it lands comfortably in that window rather than pinning an exact value.
	renewedDeadline := *gotJob.LeaseDeadline
	if renewedDeadline.Before(beforeA.Add(20*time.Second)) || renewedDeadline.After(beforeA.Add(40*time.Second)) {
		t.Fatalf("renewed lease_deadline %s not within expected ~30s-from-now window (beforeA=%s)", renewedDeadline, beforeA)
	}
	if gotJob.Status != "leased" || gotJob.RunnerID == nil || *gotJob.RunnerID != runnerA.ID {
		t.Fatalf("renewal must not change ownership/status: got status=%q runner=%v", gotJob.Status, gotJob.RunnerID)
	}

	// --- runnerB heartbeats reporting runnerA's lease id: must NOT renew ---
	// (and must NOT error -- Heartbeat still acks normally).

	heartbeatOnce(t, client, runnerB.AuthToken, runnerB.ID.String(), []string{leaseID.String()})

	gotJob2, err := jobs.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJob after runnerB heartbeat: %v", err)
	}
	if gotJob2.LeaseDeadline == nil || !gotJob2.LeaseDeadline.Equal(renewedDeadline) {
		t.Fatalf("runnerB's heartbeat must not renew a lease it doesn't own: deadline changed from %s to %v", renewedDeadline, gotJob2.LeaseDeadline)
	}

	// --- runnerA heartbeats with an unknown/garbage lease id mixed in: must
	// be ignored gracefully, real lease still renews, RPC does not error.

	unknownLease := uuid.New().String()
	beforeC := time.Now().UTC()
	heartbeatOnce(t, client, runnerA.AuthToken, runnerA.ID.String(), []string{unknownLease, leaseID.String(), "not-a-uuid"})

	gotJob3, err := jobs.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJob after mixed heartbeat: %v", err)
	}
	if gotJob3.LeaseDeadline == nil || !gotJob3.LeaseDeadline.After(beforeC.Add(-time.Second)) {
		t.Fatalf("expected the known lease to still be renewed alongside unknown/garbage ids, got %v", gotJob3.LeaseDeadline)
	}

	// last_heartbeat / online flip must be unaffected (existing behavior).
	gotRunnerA, err := runners.GetByID(ctx, runnerA.ID)
	if err != nil {
		t.Fatalf("GetByID runnerA: %v", err)
	}
	if gotRunnerA.Status != "online" || gotRunnerA.LastHeartbeat == nil {
		t.Fatalf("expected runnerA to remain online with last_heartbeat set, got status=%q last_heartbeat=%v", gotRunnerA.Status, gotRunnerA.LastHeartbeat)
	}
}

// TestDispatchService_LongJobSurvivesLeaseRenewal is the DoD's "job longer
// than lease TTL completes exactly once" proof. It sets a very short
// leaseTTL() (HeartbeatIntervalSeconds=1 * HeartbeatMissLimit=1 = 1s) and
// then, over ~3 seconds (3x the TTL), alternates:
//   - a Heartbeat carrying the lease id (what the real runner's heartbeatLoop
//     does every interval while a.leases holds it, see
//     drassi-runner/internal/agent/agent.go), and
//   - a direct JobStore.RequeueExpired call (what StartJobReaper's ticker
//     does every jobReaperInterval), run inline here instead of via
//     StartJobReaper's fixed 5s ticker so the test can drive both sides on a
//     fast, deterministic cadence without waiting out a real reaper tick.
//
// It asserts RequeueExpired never actually requeues anything (0 rows
// affected on every tick) and the job is leased exactly once (lease_id never
// changes) all the way to a single CompleteJob call setting it to success.
//
// Skipped unless DATABASE_URL is set.
func TestDispatchService_LongJobSurvivesLeaseRenewal(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping dispatch integration test")
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
		HeartbeatIntervalSeconds: 1,
		HeartbeatMissLimit:       1, // leaseTTL() = 1s
	}
	svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)

	runnerID := uuid.New()
	runner := &store.Runner{
		ID:        runnerID,
		Name:      "long-job-runner-" + runnerID.String()[:8],
		Labels:    []string{"self-hosted"},
		Mode:      "host",
		Capacity:  1,
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
	job := &store.Job{ID: jobID, RunID: runID, JobIDYAML: "long-build", Name: "long-build", Status: "queued"}
	if err := jobs.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	leaseTTL := svc.leaseTTL() // 1s per cfg above
	leaseID := uuid.New()
	if err := jobs.Lease(ctx, jobID, leaseID, runnerID, time.Now().UTC().Add(leaseTTL)); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	_, client, closeClient := startBufconnServer(t, svc)
	defer closeClient()

	authCtx := func() context.Context {
		return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+runner.AuthToken))
	}
	if _, err := svc.ReportStatus(authCtx(), &dispatchpb.StatusUpdate{
		LeaseId: leaseID.String(),
		Phase:   dispatchpb.Phase_PHASE_RUNNING,
		TsUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("ReportStatus (running): %v", err)
	}

	// Simulate the job "running" for 3x the lease TTL. Every tick (well
	// inside one TTL), heartbeat the lease (renewing it, exactly like the
	// runner's heartbeatLoop) and then run the reaper's requeue query
	// (exactly like StartJobReaper's ticker) to prove it never finds this
	// lease expired.
	tickInterval := leaseTTL / 3 // 3 reaper-equivalent checks per TTL window
	deadlineSim := time.Now().Add(3 * leaseTTL)
	totalRequeued := 0
	for time.Now().Before(deadlineSim) {
		heartbeatOnce(t, client, runner.AuthToken, runnerID.String(), []string{leaseID.String()})

		n, err := jobs.RequeueExpired(ctx, time.Now().UTC())
		if err != nil {
			t.Fatalf("RequeueExpired: %v", err)
		}
		totalRequeued += n

		time.Sleep(tickInterval)
	}

	if totalRequeued != 0 {
		t.Fatalf("job was requeued %d time(s) mid-run despite heartbeat renewals; leaseTTL=%s", totalRequeued, leaseTTL)
	}

	stillRunning, err := jobs.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJob after simulated long run: %v", err)
	}
	if stillRunning.Status != "running" {
		t.Fatalf("expected job to still be 'running' (never requeued), got %q", stillRunning.Status)
	}
	if stillRunning.LeaseID == nil || *stillRunning.LeaseID != leaseID {
		t.Fatalf("expected the SAME lease_id throughout (never re-leased), got %v want %s", stillRunning.LeaseID, leaseID)
	}

	// Now complete it: exactly one CompleteJob, result success.
	if _, err := svc.CompleteJob(authCtx(), &dispatchpb.CompleteJobRequest{
		LeaseId: leaseID.String(),
		Result:  dispatchpb.Phase_PHASE_SUCCESS,
	}); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}

	final, err := jobs.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJob after CompleteJob: %v", err)
	}
	if final.Status != "success" || final.Result == nil || *final.Result != "success" {
		t.Fatalf("expected final job status/result 'success', got status=%q result=%v", final.Status, final.Result)
	}
	if final.FinishedAt == nil {
		t.Fatalf("expected finished_at to be set")
	}

	// One final RequeueExpired pass confirms a terminal job is never touched.
	if n, err := jobs.RequeueExpired(ctx, time.Now().UTC()); err != nil || n != 0 {
		t.Fatalf("RequeueExpired after completion: n=%d err=%v", n, err)
	}
}
