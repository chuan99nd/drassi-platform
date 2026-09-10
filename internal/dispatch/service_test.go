package dispatch

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/chuan99nd/drassi-platform/internal/config"
	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/store"
	"github.com/chuan99nd/drassi-platform/pkg/dispatchpb"
)

// TestDispatchService_StreamLogsStatusComplete exercises StreamLogs,
// ReportStatus and CompleteJob (T-M1-06) against a real Postgres instance:
// ordered idempotent log persistence + live pub/sub fan-out, step/job status
// transitions, job completion + outputs, run status roll-up, and the
// stale-lease drop guard. It is skipped unless DATABASE_URL is set.
func TestDispatchService_StreamLogsStatusComplete(t *testing.T) {
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
		HeartbeatMissLimit:       3,
	}
	svc := NewService(runners, jobs, steps, runs, logs, outputs, cfg)

	// --- fixtures: a runner + a run with two jobs -----------------------

	runnerID := uuid.New()
	runner := &store.Runner{
		ID:        runnerID,
		Name:      "test-runner-" + runnerID.String()[:8],
		Labels:    []string{"self-hosted"},
		Mode:      "host",
		Capacity:  2,
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

	job1ID := uuid.New()
	job1 := &store.Job{ID: job1ID, RunID: runID, JobIDYAML: "build", Name: "build", Status: "queued"}
	if err := jobs.CreateJob(ctx, job1); err != nil {
		t.Fatalf("CreateJob (job1): %v", err)
	}
	job2ID := uuid.New()
	job2 := &store.Job{ID: job2ID, RunID: runID, JobIDYAML: "deploy", Name: "deploy", Status: "queued"}
	if err := jobs.CreateJob(ctx, job2); err != nil {
		t.Fatalf("CreateJob (job2): %v", err)
	}

	authCtx := func() context.Context {
		return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+runner.AuthToken))
	}

	// --- lease job1 and drive it through running -------------------------

	lease1 := uuid.New()
	if err := jobs.Lease(ctx, job1ID, lease1, runnerID, time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatalf("Lease job1: %v", err)
	}

	if _, err := svc.ReportStatus(authCtx(), &dispatchpb.StatusUpdate{
		LeaseId: lease1.String(),
		Phase:   dispatchpb.Phase_PHASE_RUNNING,
		TsUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("ReportStatus (job1 running): %v", err)
	}
	gotJob1, err := jobs.GetJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("GetJob (job1): %v", err)
	}
	if gotJob1.Status != "running" {
		t.Fatalf("expected job1 status 'running', got %q", gotJob1.Status)
	}

	// Step-level updates: step 0 running, then success.
	if _, err := svc.ReportStatus(authCtx(), &dispatchpb.StatusUpdate{
		LeaseId:   lease1.String(),
		StepId:    "checkout",
		StepIndex: 0,
		StepName:  "checkout",
		Phase:     dispatchpb.Phase_PHASE_RUNNING,
		TsUnix:    time.Now().Unix(),
	}); err != nil {
		t.Fatalf("ReportStatus (step running): %v", err)
	}
	if _, err := svc.ReportStatus(authCtx(), &dispatchpb.StatusUpdate{
		LeaseId:   lease1.String(),
		StepId:    "checkout",
		StepIndex: 0,
		StepName:  "checkout",
		Phase:     dispatchpb.Phase_PHASE_SUCCESS,
		TsUnix:    time.Now().Unix(),
	}); err != nil {
		t.Fatalf("ReportStatus (step success): %v", err)
	}
	stepList, err := steps.ListStepsForJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("ListStepsForJob: %v", err)
	}
	if len(stepList) != 1 || stepList[0].Status != "success" || stepList[0].StartedAt == nil || stepList[0].FinishedAt == nil {
		t.Fatalf("unexpected step state: %+v", stepList)
	}

	// --- StreamLogs over a real (bufconn) gRPC connection ----------------

	grpcServer, client, closeClient := startBufconnServer(t, svc)
	defer closeClient()

	sub, unsubscribe := logs.Subscribe(job1ID)
	defer unsubscribe()

	streamCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+runner.AuthToken)
	stream, err := client.StreamLogs(streamCtx)
	if err != nil {
		t.Fatalf("StreamLogs: %v", err)
	}
	chunks := []*dispatchpb.LogChunk{
		{LeaseId: lease1.String(), Seq: 0, TsUnix: time.Now().Unix(), Data: []byte("hello ")},
		{LeaseId: lease1.String(), Seq: 1, TsUnix: time.Now().Unix(), Data: []byte("world")},
		// Duplicate seq 0: must be a no-op (not re-persisted, not re-published).
		{LeaseId: lease1.String(), Seq: 0, TsUnix: time.Now().Unix(), Data: []byte("DUPLICATE")},
	}
	for _, c := range chunks {
		if err := stream.Send(c); err != nil {
			t.Fatalf("stream.Send: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("stream.CloseAndRecv: %v", err)
	}
	t.Cleanup(func() {
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cCancel()
		if _, err := pool.Exec(cCtx, `DELETE FROM log_chunks WHERE lease_id = $1`, lease1); err != nil {
			t.Logf("cleanup log_chunks: %v", err)
		}
	})

	// Persisted, in order, duplicate ignored.
	var chunkCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM log_chunks WHERE lease_id = $1`, lease1).Scan(&chunkCount); err != nil {
		t.Fatalf("count log_chunks: %v", err)
	}
	if chunkCount != 2 {
		t.Fatalf("expected 2 persisted log_chunks (duplicate seq ignored), got %d", chunkCount)
	}
	replayed, err := logs.ReplayFrom(ctx, job1ID, 0)
	if err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	if len(replayed) != 2 || replayed[0].Seq != 0 || string(replayed[0].Data) != "hello " || replayed[1].Seq != 1 || string(replayed[1].Data) != "world" {
		t.Fatalf("ReplayFrom: unexpected chunks: %+v", replayed)
	}

	// Live subscriber received exactly the 2 newly-inserted chunks, in
	// order; the duplicate seq 0 must NOT have been re-published.
	var received []logstore.Chunk
	drainTimeout := time.After(2 * time.Second)
drainLoop:
	for len(received) < 2 {
		select {
		case c, ok := <-sub:
			if !ok {
				break drainLoop
			}
			received = append(received, c)
		case <-drainTimeout:
			break drainLoop
		}
	}
	if len(received) != 2 || received[0].Seq != 0 || received[1].Seq != 1 {
		t.Fatalf("Subscribe: expected 2 chunks (seq 0,1), got %+v", received)
	}
	// Confirm nothing further (the duplicate) arrives.
	select {
	case extra, ok := <-sub:
		if ok {
			t.Fatalf("Subscribe: unexpected extra chunk published (duplicate seq must not republish): %+v", extra)
		}
	case <-time.After(200 * time.Millisecond):
	}

	// --- CompleteJob(job1, SUCCESS) with outputs -------------------------

	if _, err := svc.CompleteJob(authCtx(), &dispatchpb.CompleteJobRequest{
		LeaseId: lease1.String(),
		Result:  dispatchpb.Phase_PHASE_SUCCESS,
		Outputs: map[string]string{"artifact_url": "https://example/artifact.tar"},
	}); err != nil {
		t.Fatalf("CompleteJob (job1): %v", err)
	}
	gotJob1, err = jobs.GetJob(ctx, job1ID)
	if err != nil {
		t.Fatalf("GetJob (job1 after complete): %v", err)
	}
	if gotJob1.Status != "success" || gotJob1.Result == nil || *gotJob1.Result != "success" || gotJob1.FinishedAt == nil {
		t.Fatalf("unexpected job1 state after CompleteJob: %+v", gotJob1)
	}
	gotOutputs, err := outputs.ListOutputs(ctx, job1ID)
	if err != nil {
		t.Fatalf("ListOutputs: %v", err)
	}
	if gotOutputs["artifact_url"] != "https://example/artifact.tar" {
		t.Fatalf("expected output round-trip, got %+v", gotOutputs)
	}

	// Run is not finalized yet: job2 is still queued.
	gotRun, err := runs.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != "running" {
		t.Fatalf("expected run to remain 'running' with job2 outstanding, got %q", gotRun.Status)
	}

	// --- lease job2, then simulate an expired-lease requeue: the OLD
	// lease_id must be dropped (stale) once a NEW lease has been issued. ---

	leaseOld := uuid.New()
	if err := jobs.Lease(ctx, job2ID, leaseOld, runnerID, time.Now().UTC().Add(-time.Minute)); err != nil {
		t.Fatalf("Lease job2 (old): %v", err)
	}
	if n, err := jobs.RequeueExpired(ctx, time.Now().UTC()); err != nil || n != 1 {
		t.Fatalf("RequeueExpired: n=%d err=%v", n, err)
	}
	leaseNew := uuid.New()
	if err := jobs.Lease(ctx, job2ID, leaseNew, runnerID, time.Now().UTC().Add(time.Minute)); err != nil {
		t.Fatalf("Lease job2 (new): %v", err)
	}

	// A status update under the stale OLD lease must be dropped gracefully
	// (StatusAck with no error), and must not affect job2's state.
	if _, err := svc.ReportStatus(authCtx(), &dispatchpb.StatusUpdate{
		LeaseId: leaseOld.String(),
		Phase:   dispatchpb.Phase_PHASE_RUNNING,
		TsUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("ReportStatus (stale lease): expected graceful drop, got error: %v", err)
	}
	gotJob2, err := jobs.GetJob(ctx, job2ID)
	if err != nil {
		t.Fatalf("GetJob (job2): %v", err)
	}
	if gotJob2.Status != "leased" {
		t.Fatalf("stale lease ReportStatus must not mutate job2; expected 'leased', got %q", gotJob2.Status)
	}

	// Now drive job2 to completion under its current (new) lease.
	if _, err := svc.ReportStatus(authCtx(), &dispatchpb.StatusUpdate{
		LeaseId: leaseNew.String(),
		Phase:   dispatchpb.Phase_PHASE_RUNNING,
		TsUnix:  time.Now().Unix(),
	}); err != nil {
		t.Fatalf("ReportStatus (job2 running): %v", err)
	}
	if _, err := svc.CompleteJob(authCtx(), &dispatchpb.CompleteJobRequest{
		LeaseId:      leaseNew.String(),
		Result:       dispatchpb.Phase_PHASE_FAILURE,
		ErrorSummary: "boom",
	}); err != nil {
		t.Fatalf("CompleteJob (job2): %v", err)
	}
	gotJob2, err = jobs.GetJob(ctx, job2ID)
	if err != nil {
		t.Fatalf("GetJob (job2 after complete): %v", err)
	}
	if gotJob2.Status != "failure" {
		t.Fatalf("expected job2 status 'failure', got %q", gotJob2.Status)
	}

	// Both jobs are now terminal (one success, one failure): the run must
	// roll up to 'failure'.
	gotRun, err = runs.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun (after both jobs terminal): %v", err)
	}
	if gotRun.Status != "failure" {
		t.Fatalf("expected run status 'failure' (any job failed), got %q", gotRun.Status)
	}

	grpcServer.Stop()
}

// startBufconnServer starts svc on an in-process bufconn listener and
// returns a connected dispatchpb.DispatchClient plus a close func.
func startBufconnServer(t *testing.T, svc dispatchpb.DispatchServer) (*grpc.Server, dispatchpb.DispatchClient, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	dispatchpb.RegisterDispatchServer(grpcServer, svc)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	client := dispatchpb.NewDispatchClient(conn)
	closeFn := func() {
		_ = conn.Close()
	}
	return grpcServer, client, closeFn
}
