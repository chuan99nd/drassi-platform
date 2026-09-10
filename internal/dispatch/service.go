// Package dispatch implements the dispatch.v1.Dispatch gRPC service: runner
// registration, job leasing, heartbeats, status/log reporting and job
// completion. This file implements RegisterRunner + Heartbeat (T-M0-05), the
// background reaper that flips stale runners offline, and StreamLogs +
// ReportStatus + CompleteJob (T-M1-06). AcquireJob, the job queue/leasing
// engine and the lease-expiry reaper are implemented in queue.go (T-M1-04).
package dispatch

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/chuan99nd/drassi-platform/internal/config"
	"github.com/chuan99nd/drassi-platform/internal/github"
	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/store"
	"github.com/chuan99nd/drassi-platform/pkg/dispatchpb"
)

// authTokenBytes is the size of the crypto-random auth token minted at
// registration (encoded as base64url, so the returned string is longer).
const authTokenBytes = 32

// Service implements dispatchpb.DispatchServer. It embeds
// UnimplementedDispatchServer so RPCs not yet implemented here (AcquireJob)
// return codes.Unimplemented.
type Service struct {
	dispatchpb.UnimplementedDispatchServer

	runners store.RunnerStore
	jobs    store.JobStore
	steps   store.StepStore
	runs    store.RunStore
	logs    *logstore.Store
	outputs store.JobOutputStore
	cfg     config.Config

	// gh builds JobLease.repo_url (github.CloneURL, T-M1-03). Built
	// internally from cfg.GitHubToken rather than threaded through
	// NewService's signature (T-M1-04 constraint: no new constructor args).
	gh *github.Client
	// queue is the in-memory notify layer AcquireJob (T-M1-04) blocks on
	// while waiting for a job to become queued; the DB queued->leased
	// conditional UPDATE (JobStore.Lease) remains the source of truth for
	// which runner actually wins a job. Notify wakes every AcquireJob call
	// currently waiting; it is safe to call whether or not anyone is
	// waiting.
	queue *jobQueueNotifier

	// commitStatus, when installed via SetCommitStatusReporter, is called
	// from finalizeRunIfComplete to report a run's terminal outcome back to
	// GitHub as a commit status (T-M2-03). Nil-safe: left unset (the
	// default — e.g. every existing test that constructs a Service via
	// NewService), finalizeRunIfComplete skips reporting entirely.
	//
	// This is a narrow structural interface rather than
	// *github.CommitStatusReporter so a test fake needs no dependency on
	// the github package, and so this field can be satisfied by anything
	// with the right method set.
	commitStatus interface {
		ReportStatus(ctx context.Context, repo, sha, state, description string) error
	}
}

// SetCommitStatusReporter installs r as the commit-status reporter used by
// finalizeRunIfComplete (T-M2-03). Passing nil (or never calling this)
// disables reporting, which is the default. This is a setter rather than a
// NewService parameter so main.go can wire a reporter in as a
// post-construction step without changing NewService's signature (the same
// no-new-constructor-args constraint gh already follows, see above).
func (s *Service) SetCommitStatusReporter(r interface {
	ReportStatus(ctx context.Context, repo, sha, state, description string) error
}) {
	s.commitStatus = r
}

// NewService constructs the Dispatch service backed by the given stores, the
// in-process logstore (StreamLogs persistence + pub/sub, T-M1-06), and cfg
// (heartbeat interval / miss-limit).
func NewService(
	runners store.RunnerStore,
	jobs store.JobStore,
	steps store.StepStore,
	runs store.RunStore,
	logs *logstore.Store,
	outputs store.JobOutputStore,
	cfg config.Config,
) *Service {
	return &Service{
		runners: runners,
		jobs:    jobs,
		steps:   steps,
		runs:    runs,
		logs:    logs,
		outputs: outputs,
		cfg:     cfg,
		gh:      github.New(cfg.GitHubToken, nil),
		queue:   newJobQueueNotifier(),
	}
}

// RegisterRunner persists a new runner row, mints a runner_id + auth_token,
// and returns the configured heartbeat interval. Two calls always yield
// distinct ids/tokens (uuid.New + crypto/rand per call).
func (s *Service) RegisterRunner(ctx context.Context, req *dispatchpb.RegisterRunnerRequest) (*dispatchpb.RegisterRunnerResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name must not be empty")
	}
	if req.GetCapacity() < 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity must be >= 0")
	}
	mode, err := runnerModeToString(req.GetMode())
	if err != nil {
		return nil, err
	}

	token, err := generateAuthToken()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate auth token: %v", err)
	}

	now := time.Now().UTC()
	runner := &store.Runner{
		ID:            uuid.New(),
		Name:          req.GetName(),
		Labels:        req.GetLabels(),
		Mode:          mode,
		Capacity:      int(req.GetCapacity()),
		Version:       req.GetVersion(),
		Status:        "online",
		LastHeartbeat: &now,
		AuthToken:     token,
	}

	if err := s.runners.Create(ctx, runner); err != nil {
		return nil, status.Errorf(codes.Internal, "register runner: %v", err)
	}

	return &dispatchpb.RegisterRunnerResponse{
		RunnerId:                 runner.ID.String(),
		AuthToken:                token,
		HeartbeatIntervalSeconds: int32(s.cfg.HeartbeatIntervalSeconds),
	}, nil
}

// Heartbeat is a bidi stream: for every HeartbeatPing it validates the
// runner_id + Bearer auth_token (metadata key "authorization"), advances
// last_heartbeat, flips the runner back online if it wasn't, and replies with
// an ack carrying the configured interval.
//
// If the server restarted since a runner registered, the runner_id is still
// resolvable (the auth_token is persisted in the runners table, not just kept
// in memory), so a Heartbeat after a restart is validated normally rather
// than forcing a re-Register. An unrecognized runner_id (never registered, or
// its row was removed) still yields codes.NotFound so the runner knows to
// re-Register (see T-M0-06).
func (s *Service) Heartbeat(stream dispatchpb.Dispatch_HeartbeatServer) error {
	ctx := stream.Context()

	token, err := bearerToken(ctx)
	if err != nil {
		return err
	}

	for {
		ping, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if ctx.Err() != nil || status.Code(err) == codes.Canceled {
				// Normal client disconnect / shutdown — not an error.
				return nil
			}
			return err
		}

		runnerID, err := uuid.Parse(ping.GetRunnerId())
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid runner_id: %v", err)
		}

		runner, err := s.runners.GetByID(ctx, runnerID)
		if err != nil {
			return status.Errorf(codes.NotFound, "unknown runner_id %q; re-register", ping.GetRunnerId())
		}

		if !validToken(token, runner.AuthToken) {
			return status.Error(codes.Unauthenticated, "auth token mismatch")
		}

		now := time.Now().UTC()
		if err := s.runners.UpdateHeartbeat(ctx, runnerID, now); err != nil {
			return status.Errorf(codes.Internal, "update heartbeat: %v", err)
		}
		if runner.Status != "online" {
			if err := s.runners.SetStatus(ctx, runnerID, "online"); err != nil {
				return status.Errorf(codes.Internal, "set status online: %v", err)
			}
		}

		// T-M1-13: renew every lease the runner reports as still executing so
		// a job that runs longer than leaseTTL() isn't reaped mid-execution
		// by StartJobReaper's RequeueExpired. Best-effort: never fails the
		// Heartbeat RPC (see renewActiveLeases' doc comment).
		s.renewActiveLeases(ctx, runnerID, ping.GetActiveLeaseIds(), now)

		ack := &dispatchpb.HeartbeatAck{
			NextIntervalSeconds: int32(s.cfg.HeartbeatIntervalSeconds),
			CancelLeaseIds:      nil,
		}
		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// RunReaper ticks every configured heartbeat interval and marks runners
// offline whose last_heartbeat is older than
// HeartbeatMissLimit*HeartbeatIntervalSeconds. It blocks until ctx is
// cancelled, so callers should run it in its own goroutine and cancel ctx on
// shutdown.
func (s *Service) RunReaper(ctx context.Context) {
	interval := time.Duration(s.cfg.HeartbeatIntervalSeconds) * time.Second
	staleAfter := time.Duration(s.cfg.HeartbeatMissLimit) * interval

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().UTC().Add(-staleAfter)
			n, err := s.runners.MarkStaleOffline(ctx, cutoff)
			if err != nil {
				log.Printf("dispatch: reaper: mark stale offline: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("dispatch: reaper: marked %d runner(s) offline (cutoff=%s)", n, cutoff.Format(time.RFC3339))
			}
		}
	}
}

// StreamLogs is a client-stream: for each LogChunk received it resolves
// lease_id -> job (dropping chunks for a stale/unknown lease, e.g. the job
// was requeued to a new lease after this runner's lease expired), validates
// the runner's Bearer token against that job's assigned runner, persists the
// chunk idempotently, and — only for a chunk that was newly inserted (never
// for a duplicate retry) — publishes it to live logstore subscribers. It
// replies with a single LogAck once the runner closes its send side.
func (s *Service) StreamLogs(stream dispatchpb.Dispatch_StreamLogsServer) error {
	ctx := stream.Context()

	token, err := bearerToken(ctx)
	if err != nil {
		return err
	}

	// Cache lease -> job resolution (and its auth check) across chunks in
	// this stream: a runner typically streams many chunks for the same
	// lease, and re-resolving + re-authorizing per chunk would be wasteful.
	// A cached nil entry means "known stale/unknown lease, keep dropping".
	resolved := make(map[uuid.UUID]*store.Job)

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&dispatchpb.LogAck{})
		}
		if err != nil {
			if ctx.Err() != nil || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}

		leaseID, err := uuid.Parse(chunk.GetLeaseId())
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid lease_id: %v", err)
		}

		job, cached := resolved[leaseID]
		if !cached {
			job, err = s.resolveLeaseJob(ctx, leaseID)
			if err != nil {
				return err
			}
			if job != nil {
				if err := s.authorizeJobRunner(ctx, token, job); err != nil {
					return err
				}
			} else {
				log.Printf("dispatch: StreamLogs: unknown/stale lease %s; dropping chunks for it", leaseID)
			}
			resolved[leaseID] = job
		}
		if job == nil {
			continue
		}

		c := logstore.Chunk{
			JobRowID: job.ID,
			LeaseID:  leaseID,
			StepID:   chunk.GetStepId(),
			Seq:      chunk.GetSeq(),
			Ts:       time.Unix(chunk.GetTsUnix(), 0).UTC(),
			Data:     chunk.GetData(),
		}

		inserted, err := s.logs.Append(ctx, c)
		if err != nil {
			return status.Errorf(codes.Internal, "append log chunk: %v", err)
		}
		if inserted {
			s.logs.Publish(c)
		}
	}
}

// ReportStatus maps a StatusUpdate to a steps row upsert (step-level, i.e.
// step_id or step_name set) or a job status transition (job-level, i.e.
// step_id == "" and step_name == ""). See the Phase -> status mapping table
// in tasks/M1/T-M1-06-logstore-status-complete.md.
//
// Only a job-level PHASE_RUNNING drives a job transition here (leased ->
// running, stamping started_at was already done at Lease() time); other
// job-level phases are reported informationally and ignored — terminal job
// states are set exclusively by CompleteJob. An illegal transition (e.g. a
// duplicate/racing RUNNING report) is logged and ignored rather than failing
// the RPC, per the task's "log and ignore" guidance.
func (s *Service) ReportStatus(ctx context.Context, req *dispatchpb.StatusUpdate) (*dispatchpb.StatusAck, error) {
	token, err := bearerToken(ctx)
	if err != nil {
		return nil, err
	}

	leaseID, err := uuid.Parse(req.GetLeaseId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid lease_id: %v", err)
	}

	job, err := s.resolveLeaseJob(ctx, leaseID)
	if err != nil {
		return nil, err
	}
	if job == nil {
		log.Printf("dispatch: ReportStatus: unknown/stale lease %s; dropping", leaseID)
		return &dispatchpb.StatusAck{}, nil
	}
	if err := s.authorizeJobRunner(ctx, token, job); err != nil {
		return nil, err
	}

	stepStatus, err := phaseToStatus(req.GetPhase())
	if err != nil {
		return nil, err
	}
	ts := time.Unix(req.GetTsUnix(), 0).UTC()

	if req.GetStepId() != "" || req.GetStepName() != "" {
		// UpsertStep is a full-row upsert (not a partial merge), so a
		// second call for the same step (e.g. RUNNING then SUCCESS) would
		// otherwise clobber started_at back to NULL when this call doesn't
		// itself carry a RUNNING phase. Look up any existing timestamps for
		// this (job_row_id, step_index) first and carry them forward.
		var startedAt, finishedAt *time.Time
		existing, err := s.steps.ListStepsForJob(ctx, job.ID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list steps for job: %v", err)
		}
		for _, es := range existing {
			if es.StepIndex == int(req.GetStepIndex()) {
				startedAt, finishedAt = es.StartedAt, es.FinishedAt
				break
			}
		}
		if stepStatus == "running" && startedAt == nil {
			startedAt = &ts
		}
		if isTerminalStatus(stepStatus) {
			finishedAt = &ts
		}

		st := &store.Step{
			ID:         uuid.New(),
			JobRowID:   job.ID,
			StepIndex:  int(req.GetStepIndex()),
			Name:       req.GetStepName(),
			Status:     stepStatus,
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
		}
		if err := s.steps.UpsertStep(ctx, st); err != nil {
			return nil, status.Errorf(codes.Internal, "upsert step: %v", err)
		}
		// The runner reports step-level status only; drive the job leased->running
		// on the first step that starts, so live status reflects execution.
		if stepStatus == "running" {
			if err := s.jobs.TransitionStatus(ctx, job.ID, "leased", "running"); err != nil && !errors.Is(err, store.ErrIllegalTransition) {
				return nil, status.Errorf(codes.Internal, "transition job leased->running: %v", err)
			}
		}
		return &dispatchpb.StatusAck{}, nil
	}

	// Job-level update: only a first RUNNING drives a state transition.
	if stepStatus == "running" {
		if err := s.jobs.TransitionStatus(ctx, job.ID, "leased", "running"); err != nil {
			if !errors.Is(err, store.ErrIllegalTransition) {
				return nil, status.Errorf(codes.Internal, "transition job to running: %v", err)
			}
			log.Printf("dispatch: ReportStatus: job %s leased->running: %v (ignored)", job.ID, err)
		}
	}
	return &dispatchpb.StatusAck{}, nil
}

// CompleteJob finalizes a lease: sets the job's terminal result +
// finished_at, transitions the job to that terminal status, writes each
// outputs[k]=v into job_outputs, and — once every job belonging to the run is
// terminal — rolls up and sets workflow_runs.status (success iff every job
// succeeded; failure if any job failed; otherwise cancelled). A stale/unknown
// lease (job already requeued to a different lease) is dropped gracefully.
func (s *Service) CompleteJob(ctx context.Context, req *dispatchpb.CompleteJobRequest) (*dispatchpb.CompleteJobResponse, error) {
	token, err := bearerToken(ctx)
	if err != nil {
		return nil, err
	}

	leaseID, err := uuid.Parse(req.GetLeaseId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid lease_id: %v", err)
	}

	job, err := s.resolveLeaseJob(ctx, leaseID)
	if err != nil {
		return nil, err
	}
	if job == nil {
		log.Printf("dispatch: CompleteJob: unknown/stale lease %s; dropping", leaseID)
		return &dispatchpb.CompleteJobResponse{}, nil
	}
	if err := s.authorizeJobRunner(ctx, token, job); err != nil {
		return nil, err
	}

	result, err := phaseToResult(req.GetResult())
	if err != nil {
		return nil, err
	}
	if !isTerminalStatus(result) {
		return nil, status.Errorf(codes.InvalidArgument, "result phase %v is not a terminal result", req.GetResult())
	}

	finishedAt := time.Now().UTC()
	if err := s.jobs.SetResult(ctx, job.ID, result, finishedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "set job result: %v", err)
	}
	// The runner reports step-level status but does not emit a job-level
	// RUNNING, so a job may still be "leased" at completion (no ReportStatus
	// advanced it). Best-effort advance leased->running so the terminal
	// transition below is legal regardless of what status arrived first.
	if err := s.jobs.TransitionStatus(ctx, job.ID, "leased", "running"); err != nil && !errors.Is(err, store.ErrIllegalTransition) {
		return nil, status.Errorf(codes.Internal, "transition job leased->running: %v", err)
	}
	if err := s.jobs.TransitionStatus(ctx, job.ID, "running", result); err != nil {
		if !errors.Is(err, store.ErrIllegalTransition) {
			return nil, status.Errorf(codes.Internal, "transition job to %q: %v", result, err)
		}
		log.Printf("dispatch: CompleteJob: job %s running->%s: %v (ignored)", job.ID, result, err)
	}

	for k, v := range req.GetOutputs() {
		if err := s.outputs.UpsertOutput(ctx, job.ID, k, v); err != nil {
			return nil, status.Errorf(codes.Internal, "write job output %q: %v", k, err)
		}
	}

	// T-M3-01: this job just went terminal, so every job in the run whose
	// needs include it may now be decidable -- either newly leasable (all
	// needs succeeded) or skip-by-rule (this job ended failure/cancelled/
	// skipped and the dependent has no if: override). gateRun runs that
	// fixpoint (cascading transitively), then rolls up the run itself
	// (finalizeRunIfComplete) exactly like the plain call below used to.
	if err := s.gateRun(ctx, job.RunID); err != nil {
		// The job itself is fully recorded at this point; a gating/roll-up
		// failure shouldn't fail the runner's CompleteJob call (it would
		// just retry, re-completing an already-terminal job). Log instead --
		// the next terminal completion in this run (or a future AcquireJob's
		// own defensive check) gets another chance to make progress.
		log.Printf("dispatch: CompleteJob: gate run %s: %v", job.RunID, err)
	}

	return &dispatchpb.CompleteJobResponse{}, nil
}

// renewActiveLeases extends lease_deadline (via JobStore.RenewLease) for
// every lease id a runner reports as currently executing on a Heartbeat
// (HeartbeatPing.active_lease_ids, T-M1-13). It is the fix for a job that
// runs longer than leaseTTL(): StartJobReaper.RequeueExpired only reaps a
// lease whose deadline has actually passed, and a live runner heartbeating
// every HeartbeatIntervalSeconds pushes the deadline forward faster than the
// reaper's window can lapse.
//
// Each reported id is resolved to its current job (JobStore.GetByLease) and
// the job's assigned runner is checked against runnerID before renewing --
// mirroring authorizeJobRunner's Bearer-token check, but by identity rather
// than token since Heartbeat already authenticated runnerID itself. This
// stops a runner from renewing a lease it doesn't actually hold (a stale id
// left over from a job that was reaped and re-leased to a different runner,
// or a buggy/malicious report).
//
// Every failure mode here — a malformed id, an unknown/already-
// completed/requeued lease (RenewLease's conditional UPDATE affects 0 rows,
// wrapped as pgx.ErrNoRows), or a lease that belongs to a different runner —
// is logged and skipped rather than propagated: a stale active_lease_ids
// entry must never fail the Heartbeat RPC (it would flip the runner offline
// for no good reason).
func (s *Service) renewActiveLeases(ctx context.Context, runnerID uuid.UUID, leaseIDs []string, now time.Time) {
	if len(leaseIDs) == 0 {
		return
	}
	deadline := now.Add(s.leaseTTL())

	for _, raw := range leaseIDs {
		leaseID, err := uuid.Parse(raw)
		if err != nil {
			log.Printf("dispatch: Heartbeat: runner %s: invalid lease id %q in active_lease_ids: %v", runnerID, raw, err)
			continue
		}

		job, err := s.jobs.GetByLease(ctx, leaseID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Stale/unknown lease (already completed, or reaped and
				// requeued under a new lease_id) -- harmless, ignore.
				continue
			}
			log.Printf("dispatch: Heartbeat: runner %s: resolve lease %s: %v", runnerID, leaseID, err)
			continue
		}
		if job.RunnerID == nil || *job.RunnerID != runnerID {
			log.Printf("dispatch: Heartbeat: runner %s: reported lease %s belongs to a different/no runner; not renewing", runnerID, leaseID)
			continue
		}

		if err := s.jobs.RenewLease(ctx, leaseID, deadline); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Printf("dispatch: Heartbeat: runner %s: renew lease %s: %v", runnerID, leaseID, err)
			}
			continue
		}
	}
}

// resolveLeaseJob resolves leaseID to the jobs row currently holding it. A
// stale or unknown lease_id (no job currently holds it — e.g. it expired and
// the job was requeued under a new lease_id) is reported as (nil, nil) so
// callers can drop the update gracefully instead of failing the whole
// call/stream.
func (s *Service) resolveLeaseJob(ctx context.Context, leaseID uuid.UUID) (*store.Job, error) {
	job, err := s.jobs.GetByLease(ctx, leaseID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, status.Errorf(codes.Internal, "resolve lease: %v", err)
	}
	return job, nil
}

// authorizeJobRunner validates that token is the Bearer auth_token of job's
// currently assigned runner, the same check Heartbeat performs by runner_id.
// LogChunk/StatusUpdate/CompleteJobRequest carry no runner_id, only
// lease_id, so the runner is looked up via the job the lease resolved to.
func (s *Service) authorizeJobRunner(ctx context.Context, token string, job *store.Job) error {
	if job.RunnerID == nil {
		return status.Error(codes.FailedPrecondition, "job has no assigned runner")
	}
	runner, err := s.runners.GetByID(ctx, *job.RunnerID)
	if err != nil {
		return status.Errorf(codes.Internal, "lookup runner: %v", err)
	}
	if !validToken(token, runner.AuthToken) {
		return status.Error(codes.Unauthenticated, "auth token mismatch")
	}
	return nil
}

// terminalJobStatuses mirrors jobTransitions' implicit terminal set
// (internal/store/job_store.go): statuses with no outgoing transitions.
var terminalJobStatuses = map[string]bool{
	"success":   true,
	"failure":   true,
	"skipped":   true,
	"cancelled": true,
}

// isTerminalStatus reports whether st is a terminal job/step status.
func isTerminalStatus(st string) bool {
	return terminalJobStatuses[st]
}

// finalizeRunIfComplete rolls up runID's workflow_runs.status once every job
// belonging to it is terminal: success iff every job is success OR skipped
// (T-M3-01: a job skipped-by-rule, or reported skipped by the runner's if:
// pre-eval, is a clean non-failing outcome, not a reason to fail the run),
// failure if any job failed, otherwise cancelled (e.g. a mix of
// skipped/cancelled jobs with no failure and no success at all).
func (s *Service) finalizeRunIfComplete(ctx context.Context, runID uuid.UUID) error {
	jobs, err := s.jobs.ListJobsForRun(ctx, runID)
	if err != nil {
		return status.Errorf(codes.Internal, "list jobs for run: %v", err)
	}

	allSuccessOrSkipped, anyFailure := true, false
	for _, j := range jobs {
		if !isTerminalStatus(j.Status) {
			return nil // run still has non-terminal jobs; nothing to finalize yet
		}
		if j.Status == "failure" {
			anyFailure = true
		}
		if j.Status != "success" && j.Status != "skipped" {
			allSuccessOrSkipped = false
		}
	}

	runStatus := "cancelled"
	switch {
	case anyFailure:
		runStatus = "failure"
	case allSuccessOrSkipped:
		runStatus = "success"
	}

	if err := s.runs.SetRunStatus(ctx, runID, runStatus); err != nil {
		return status.Errorf(codes.Internal, "set run status: %v", err)
	}

	s.reportCommitStatus(ctx, runID, runStatus)

	return nil
}

// reportCommitStatus posts runID's just-finalized terminal status back to
// GitHub via the installed commit-status reporter (T-M2-03), if any. It is
// a no-op when:
//   - no reporter has been installed (SetCommitStatusReporter was never
//     called — the default for a Service built with only NewService), or
//   - the run's event is "manual", or it has no repo/sha — there is no
//     GitHub commit to report against (manual, non-GitHub runs post
//     nothing, per T-M2-03's scope).
//
// This is strictly best-effort: finalizeRunIfComplete's own caller
// (CompleteJob) already treats a finalize failure as log-only, and a
// GitHub API hiccup here must never fail the run or block dispatch. Any
// failure (looking up the run, or the reporter call itself) is logged and
// swallowed.
func (s *Service) reportCommitStatus(ctx context.Context, runID uuid.UUID, runStatus string) {
	if s.commitStatus == nil {
		return
	}

	run, err := s.runs.GetRun(ctx, runID)
	if err != nil {
		log.Printf("dispatch: reportCommitStatus: get run %s: %v", runID, err)
		return
	}
	if run.Event == "manual" || run.Repo == "" || run.SHA == "" {
		return
	}

	state, description := commitStatusForRunStatus(runStatus)
	if err := s.commitStatus.ReportStatus(ctx, run.Repo, run.SHA, state, description); err != nil {
		// The reporter itself already logs the underlying HTTP failure
		// (without the token); log here too so it's visible from the
		// dispatch/finalize side, tagged with the run it belongs to.
		log.Printf("dispatch: reportCommitStatus: run %s: post %s status to %s@%s: %v", runID, state, run.Repo, run.SHA, err)
	}
}

// commitStatusForRunStatus maps a terminal workflow_runs.status value to the
// GitHub commit-status `state` plus a short human description.
//
// "cancelled" (e.g. a run whose jobs are a mix of skipped/cancelled with no
// outright failure — see finalizeRunIfComplete above) maps to GitHub's
// "error" state rather than "success" or "failure": it is neither a clean
// pass nor a job failure, and GitHub renders "error" distinctly from both in
// its checks UI. This is the documented choice called out as an option in
// T-M2-03 ("cancelled -> error (or failure; pick one and document)").
func commitStatusForRunStatus(runStatus string) (state, description string) {
	switch runStatus {
	case "success":
		return "success", "Drassi run succeeded"
	case "failure":
		return "failure", "Drassi run failed"
	default:
		return "error", "Drassi run " + runStatus
	}
}

// phaseToStatus maps the proto Phase enum to the status string stored in the
// jobs/steps status columns.
func phaseToStatus(p dispatchpb.Phase) (string, error) {
	switch p {
	case dispatchpb.Phase_PHASE_PENDING:
		return "queued", nil
	case dispatchpb.Phase_PHASE_RUNNING:
		return "running", nil
	case dispatchpb.Phase_PHASE_SUCCESS:
		return "success", nil
	case dispatchpb.Phase_PHASE_FAILURE:
		return "failure", nil
	case dispatchpb.Phase_PHASE_SKIPPED:
		return "skipped", nil
	case dispatchpb.Phase_PHASE_CANCELLED:
		return "cancelled", nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "phase must be specified, got %v", p)
	}
}

// phaseToResult maps a terminal dispatchpb.Phase to the result string
// stored in jobs.result and in NeedOutputs.result (T-M3-02) -- exactly the
// strings act's expression engine compares against (success/failure/
// cancelled/skipped, see CONVENTIONS.md's "Cross-machine needs bridge").
// It is the same table phaseToStatus uses (a job's terminal "status" and
// "result" are always the same string, see CompleteJob/skipJob); this name
// exists so call sites that specifically want "the result string for a
// terminal phase" (CompleteJob, and skipJob's hard-coded "skipped") read as
// such.
func phaseToResult(p dispatchpb.Phase) (string, error) {
	return phaseToStatus(p)
}

// runnerModeToString maps the proto RunnerMode to the mode column's string
// representation, rejecting the zero value.
func runnerModeToString(m dispatchpb.RunnerMode) (string, error) {
	switch m {
	case dispatchpb.RunnerMode_RUNNER_MODE_HOST:
		return "host", nil
	case dispatchpb.RunnerMode_RUNNER_MODE_DOCKER:
		return "docker", nil
	case dispatchpb.RunnerMode_RUNNER_MODE_K8S:
		return "k8s", nil
	default:
		return "", status.Error(codes.InvalidArgument, "mode must be specified")
	}
}

// generateAuthToken returns a crypto-random, base64url-encoded opaque token.
func generateAuthToken() (string, error) {
	buf := make([]byte, authTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// bearerToken extracts the token from the "authorization: Bearer <token>"
// gRPC metadata on ctx. It never logs the token value.
func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 || vals[0] == "" {
		return "", status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(vals[0], prefix) {
		return "", status.Error(codes.Unauthenticated, "authorization metadata must be a Bearer token")
	}
	token := strings.TrimPrefix(vals[0], prefix)
	if token == "" {
		return "", status.Error(codes.Unauthenticated, "empty bearer token")
	}
	return token, nil
}

// validToken reports whether presented matches stored using a constant-time
// comparison. An empty stored token (shouldn't happen post-registration)
// never validates.
func validToken(presented, stored string) bool {
	if stored == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(stored)) == 1
}
