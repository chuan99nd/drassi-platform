// This file implements T-M1-04: the job queue/leasing engine behind
// Dispatch.AcquireJob, plus the lease-expiry reaper.
//
// Exactly-once handoff never does read-then-write to pick a job: candidate
// selection (ListQueuedJobs) is advisory only, and the actual handoff is a
// single conditional UPDATE (JobStore.Lease, queued->leased WHERE
// status='queued'). Two concurrent AcquireJob calls that pick the same
// candidate race on that UPDATE; exactly one affects a row and wins the
// job, the other's Lease call returns ErrIllegalTransition (0 rows
// affected) and moves on to its next candidate. This is what makes the
// queue safe without any cross-process lock.
//
// Waiting for work: a connected runner blocks (does not busy-poll) between
// candidates using jobQueueNotifier, a simple broadcast-once channel that
// Notify() closes to wake every current waiter, plus a short ticker as a
// backstop (covers the case where a job becomes queued by some path that
// forgets to call Notify, e.g. a future direct DB write, or a Notify racing
// a new waiter registering just after the broadcast). The DB remains the
// only source of truth for what is actually queued/leased; the notifier is
// purely a wakeup hint.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/chuan99nd/drassi-platform/internal/github"
	"github.com/chuan99nd/drassi-platform/internal/store"
	"github.com/chuan99nd/drassi-platform/pkg/dispatchpb"
)

const (
	// jobPollInterval is the backstop poll cadence AcquireJob falls back to
	// between Notify wakeups, so a missed/racy notification never blocks a
	// waiting runner longer than this.
	jobPollInterval = 2 * time.Second

	// jobReaperInterval is how often StartJobReaper calls
	// JobStore.RequeueExpired.
	jobReaperInterval = 5 * time.Second

	// candidateBatchSize bounds how many queued jobs AcquireJob considers
	// per attempt before giving up and waiting for the next wakeup. Since
	// T-M3-01, a "queued" row is not necessarily leasable yet (its needs may
	// still be non-terminal, or it may be about to be skipped-by-rule), so a
	// batch also absorbs those non-candidates without an extra round trip.
	candidateBatchSize = 10
)

// terminalNonSuccessJobStatuses is the T-M3-01 "blocking" subset of the
// terminal job statuses: a need that lands in one of these (rather than
// "success") is what default GitHub semantics treat as "this dependent does
// not run" -- see needsSatisfied.
var terminalNonSuccessJobStatuses = map[string]bool{
	"failure":   true,
	"cancelled": true,
	"skipped":   true,
}

// needsSatisfied resolves job's needs[] (YAML job_id, T-M1-01/T-M1-02)
// against byJobID (every job row in the same run, grouped by JobIDYAML --
// T-M4-01: a matrixed need has MULTIPLE rows sharing one JobIDYAML, so this
// is a group, not a single row) and reports:
//   - allTerminal: every need's ENTIRE group of rows has reached a terminal
//     status (success|failure|skipped|cancelled) -- a matrixed need with 2
//     cells is only terminal once BOTH cells are terminal. A job with no
//     needs is trivially allTerminal (unchanged M1 behavior: immediately
//     leasable).
//   - anyBlocking: at least one row, across all needs' groups, is terminal
//     but NOT "success" -- failure, cancelled, OR skipped. GitHub's default
//     `success()` gating treats all three the same way: none of them ran
//     clean, so by default the dependent should not run either (this also
//     drives transitive skip propagation for a chain like
//     A(fail)->B(skipped)->C). For a matrixed need this means ANY cell
//     ending non-success blocks the dependent, matching GitHub's "all cells
//     of a matrix job must succeed for needs: to be satisfied" semantics.
//
// A need id absent from byJobID, or resolving to an empty group (shouldn't
// happen once the planner persists the full needs closure into the same
// run), is conservatively treated as non-terminal rather than ignored.
func needsSatisfied(job *store.Job, byJobID map[string][]*store.Job) (allTerminal bool, anyBlocking bool) {
	allTerminal = true
	for _, need := range job.Needs {
		rows := byJobID[need]
		if len(rows) == 0 {
			allTerminal = false
			continue
		}
		for _, n := range rows {
			if !isTerminalStatus(n.Status) {
				allTerminal = false
				continue
			}
			if terminalNonSuccessJobStatuses[n.Status] {
				anyBlocking = true
			}
		}
	}
	return allTerminal, anyBlocking
}

// groupJobsByJobID buckets jobs by JobIDYAML: T-M4-01 gives a matrixed job
// multiple rows sharing one JobIDYAML, so any needs-gating lookup that used
// to be a plain "one row per job_id_yaml" map must use this instead of
// dropping duplicates into a single-row map.
func groupJobsByJobID(jobs []*store.Job) map[string][]*store.Job {
	byJobID := make(map[string][]*store.Job, len(jobs))
	for _, j := range jobs {
		byJobID[j.JobIDYAML] = append(byJobID[j.JobIDYAML], j)
	}
	return byJobID
}

// hasDefaultIfExpr reports whether job carries no job-level `if:` override.
// The planner (act's default) stamps "success()" on every job with no
// explicit `if:`, so both the empty string and the literal "success()"
// count as "no override" here -- the ordinary default-gating rule applies:
// skip the job if any of its needs didn't succeed.
//
// Any OTHER if_expr (always(), failure(), a custom expression, ...) means
// this task must not decide the skip/run outcome locally: the expression
// might deliberately want to run even though a need failed. That job is
// instead routed to the runner path (dispatched normally) and T-M3-03's
// pre-evaluation on the runner produces the correct skip/run + report. See
// tasks/M3/T-M3-01-needs-gating.md and T-M3-03.
func hasDefaultIfExpr(job *store.Job) bool {
	return job.IfExpr == nil || *job.IfExpr == "" || *job.IfExpr == "success()"
}

// gateRun runs the T-M3-01 needs-gating fixpoint for runID: repeatedly scans
// every job row in the run and marks skip-by-rule jobs `skipped` (queued ->
// skipped) until a pass makes no further change, so a chain like
// A(failure)->B->C skips B on the first pass and then sees B terminal and
// skips C on the second. It never leases/dispatches anything -- that stays
// AcquireJob's job -- it only resolves jobs that must never be leased at
// all. After the fixpoint it rolls up the run (finalizeRunIfComplete, which
// already treats skipped as terminal/non-failing) and, if anything actually
// changed, wakes any AcquireJob call blocked on the queue (a downstream job
// may have just become leasable, or the run may have just finished).
//
// Concurrency: each skip is a single TransitionStatus(queued->skipped) call,
// which is (per JobStore's doc comment) a conditional UPDATE ... WHERE
// status = 'queued'. Two callers racing to gate the same job (e.g. two
// CompleteJob calls for two different needs finishing at nearly the same
// time) either both attempt the same transition and exactly one wins (the
// loser gets ErrIllegalTransition and is skipped over, not treated as an
// error), or one call has already leased the job out from under the other
// (also ErrIllegalTransition, also correctly ignored: it must not be
// force-skipped once a runner holds it). No advisory lock is needed beyond
// this existing single-conditional-UPDATE pattern (see AcquireJob's package
// doc comment for the same reasoning applied to leasing).
//
// gateRun is idempotent: calling it again on an already-settled run is a
// single no-op pass.
func (s *Service) gateRun(ctx context.Context, runID uuid.UUID) error {
	changed := false
	for {
		jobs, err := s.jobs.ListJobsForRun(ctx, runID)
		if err != nil {
			return fmt.Errorf("gateRun: list jobs for run %s: %w", runID, err)
		}
		byJobID := groupJobsByJobID(jobs)

		progressed := false
		for _, j := range jobs {
			if j.Status != "queued" || len(j.Needs) == 0 {
				continue
			}
			allTerminal, anyBlocking := needsSatisfied(j, byJobID)
			if !allTerminal || !anyBlocking || !hasDefaultIfExpr(j) {
				// Either not ready to decide yet, all needs succeeded (leave
				// it queued/leasable), or it has an if: override (route to
				// the runner path per T-M3-01/T-M3-03 -- leave it queued).
				continue
			}
			if err := s.skipJob(ctx, j.ID); err != nil {
				if errors.Is(err, store.ErrIllegalTransition) {
					// Lost the race (leased/skipped/cancelled concurrently
					// by another gateRun/AcquireJob call) -- fine, the next
					// pass reflects whatever the real state ended up being.
					continue
				}
				return fmt.Errorf("gateRun: skip job %s: %w", j.ID, err)
			}
			progressed = true
			changed = true
		}
		if !progressed {
			break
		}
	}

	if err := s.finalizeRunIfComplete(ctx, runID); err != nil {
		return fmt.Errorf("gateRun: finalize run %s: %w", runID, err)
	}
	if changed {
		s.Notify()
	}
	return nil
}

// skipJob marks job id `skipped` (queued -> skipped only; see gateRun's
// concurrency note) and, only once that transition actually wins, also
// stamps it with a terminal result of "skipped" (via JobStore.SetResult) so
// T-M3-02's needs_outputs assembly can read a real result/outputs pair for
// it exactly like it would for a runner-completed job. Result is set AFTER
// the transition succeeds, not before, so a job that loses the transition
// race (e.g. a concurrent AcquireJob leased it first) never has its result
// column touched.
func (s *Service) skipJob(ctx context.Context, id uuid.UUID) error {
	if err := s.jobs.TransitionStatus(ctx, id, "queued", "skipped"); err != nil {
		return err
	}
	if err := s.jobs.SetResult(ctx, id, "skipped", time.Now().UTC()); err != nil {
		return fmt.Errorf("set result for skipped job %s: %w", id, err)
	}
	return nil
}

// jobQueueNotifier is a minimal broadcast-once wakeup channel: any number of
// callers can wait() for the next notify(); every call to notify() wakes
// every waiter registered since the previous notify() and then clears the
// waiter list. It carries no payload -- a woken waiter must re-check the DB
// itself, since by the time it wakes the job that triggered the wakeup may
// already have been leased by someone else.
type jobQueueNotifier struct {
	mu   sync.Mutex
	subs []chan struct{}
}

func newJobQueueNotifier() *jobQueueNotifier {
	return &jobQueueNotifier{}
}

// wait registers a new waiter and returns a channel that is closed on the
// next notify() call.
func (n *jobQueueNotifier) wait() <-chan struct{} {
	ch := make(chan struct{})
	n.mu.Lock()
	n.subs = append(n.subs, ch)
	n.mu.Unlock()
	return ch
}

// notify wakes every waiter registered since the previous notify call. Safe
// to call with zero waiters.
func (n *jobQueueNotifier) notify() {
	n.mu.Lock()
	subs := n.subs
	n.subs = nil
	n.mu.Unlock()
	for _, ch := range subs {
		close(ch)
	}
}

// Notify wakes every AcquireJob call currently blocked waiting for a
// queued job. Callers that insert new queued jobs (the planner/REST path,
// T-M1-05) or requeue an expired lease (see StartJobReaper below) should
// call this so a waiting runner is served without waiting out a full
// jobPollInterval. Safe to call even if nothing is waiting.
func (s *Service) Notify() {
	s.queue.notify()
}

// leaseTTL is how long a lease is granted for before the reaper considers
// it expired and requeues the job. Derived from the existing heartbeat
// config (interval * miss-limit) rather than a separate config field, per
// T-M1-04's scope constraint of not touching internal/config: a runner that
// is still heartbeating within that window is presumed alive and working
// the job; RenewLease (driven by Heartbeat's active_lease_ids, see the
// task's DoD) is expected to push the deadline forward before it lapses.
func (s *Service) leaseTTL() time.Duration {
	interval := time.Duration(s.cfg.HeartbeatIntervalSeconds) * time.Second
	return interval * time.Duration(s.cfg.HeartbeatMissLimit)
}

// AcquireJob is a server-stream: for each of req.free_slots, block until a
// queued job is available, atomically lease it (JobStore.Lease), and send
// one JobLease. It validates the runner's identity + Bearer auth_token the
// same way Heartbeat does.
//
// MVP slot accounting (documented per the task's "document the chosen
// behavior" note): AcquireJob honors free_slots only at connect time -- it
// sends up to free_slots leases and then closes the stream. It does not
// keep the stream open indefinitely to hand out further leases as jobs the
// runner already holds complete; the runner is expected to reconnect
// (call AcquireJob again) once a slot frees up, or once it wants more work.
// This keeps the handler simple (no need to plumb job-completion events
// back into a still-open AcquireJob call) at the cost of one extra
// reconnect per batch of freed slots, which is cheap on a gRPC channel that
// is already kept warm by Heartbeat.
//
// Stops early (returns nil, sends nothing further) on ctx.Done() (runner
// disconnect / server shutdown) without requeuing anything -- an
// in-progress lease is left as-is; StartJobReaper is what returns it to
// queued if the runner never shows up again.
func (s *Service) AcquireJob(req *dispatchpb.AcquireJobRequest, stream dispatchpb.Dispatch_AcquireJobServer) error {
	ctx := stream.Context()

	token, err := bearerToken(ctx)
	if err != nil {
		return err
	}

	runnerID, err := uuid.Parse(req.GetRunnerId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid runner_id: %v", err)
	}
	runner, err := s.runners.GetByID(ctx, runnerID)
	if err != nil {
		return status.Errorf(codes.NotFound, "unknown runner_id %q; re-register", req.GetRunnerId())
	}
	if !validToken(token, runner.AuthToken) {
		return status.Error(codes.Unauthenticated, "auth token mismatch")
	}

	freeSlots := int(req.GetFreeSlots())
	if freeSlots <= 0 {
		return status.Error(codes.InvalidArgument, "free_slots must be > 0")
	}

	leaseTTL := s.leaseTTL()

	for sent := 0; sent < freeSlots; {
		job, leaseID, err := s.acquireOneQueuedJob(ctx, runnerID, leaseTTL)
		if err != nil {
			return status.Errorf(codes.Internal, "acquire queued job: %v", err)
		}
		if job == nil {
			// Nothing queued right now: block until Notify() wakes us, the
			// backstop ticker fires, or the runner disconnects.
			select {
			case <-ctx.Done():
				return nil
			case <-s.queue.wait():
			case <-time.After(jobPollInterval):
			}
			continue
		}

		lease, err := s.buildJobLease(ctx, job, leaseID, leaseTTL)
		if err != nil {
			return err
		}
		if err := stream.Send(lease); err != nil {
			return err
		}
		sent++
	}
	return nil
}

// acquireOneQueuedJob picks a candidate queued job and attempts to lease it
// for runnerID via a single conditional UPDATE (JobStore.Lease). Candidate
// selection (ListQueuedJobs) is advisory only: if the Lease call for a
// candidate loses the race (someone else leased it first,
// store.ErrIllegalTransition, 0 rows affected), it moves on to the next
// candidate in the same batch. Returns (nil, uuid.Nil, nil) -- not an error
// -- when no candidate could be leased (either none were queued, or every
// candidate in the batch was already taken by a concurrent caller, or every
// candidate had unsatisfied needs); the caller is expected to wait and
// retry.
//
// T-M3-01 needs-gating: a candidate with needs[] is only actually leasable
// once needsSatisfied reports allTerminal. gateRun (invoked from CompleteJob
// after every terminal transition) is the primary place a skip-by-rule job
// leaves 'queued', but this loop re-checks the same predicate defensively --
// so a job is NEVER leased with unsatisfied needs, and a skip-by-rule job
// that somehow reached this point still queued gets skipped here instead of
// leased -- rather than relying solely on gateRun having already run.
func (s *Service) acquireOneQueuedJob(ctx context.Context, runnerID uuid.UUID, leaseTTL time.Duration) (*store.Job, uuid.UUID, error) {
	candidates, err := s.jobs.ListQueuedJobs(ctx, candidateBatchSize)
	if err != nil {
		return nil, uuid.Nil, err
	}

	// Cache each run's job rows (grouped by JobIDYAML) across candidates in
	// this batch -- candidates commonly share a run, and needsSatisfied
	// needs every sibling's status, not just the candidate's own row. A
	// group can hold more than one row per JobIDYAML (T-M4-01: a matrixed
	// need has one row per cell).
	runJobsCache := make(map[uuid.UUID]map[string][]*store.Job)

	for _, candidate := range candidates {
		if len(candidate.Needs) > 0 {
			byJobID, err := s.jobsByJobIDForRun(ctx, candidate.RunID, runJobsCache)
			if err != nil {
				return nil, uuid.Nil, err
			}
			allTerminal, anyBlocking := needsSatisfied(candidate, byJobID)
			if !allTerminal {
				// Needs not all terminal yet: not dispatchable (T-M3-01).
				continue
			}
			if anyBlocking && hasDefaultIfExpr(candidate) {
				// Skip-by-rule, caught here defensively (gateRun should
				// normally have already done this from the completion that
				// made allTerminal true). Never lease it.
				if err := s.skipJob(ctx, candidate.ID); err != nil && !errors.Is(err, store.ErrIllegalTransition) {
					return nil, uuid.Nil, err
				}
				continue
			}
			// allTerminal && (all needs succeeded, or this job has an if:
			// override routing it to the runner per T-M3-01/T-M3-03) --
			// fall through and lease it below.
		}

		leaseID := uuid.New()
		deadline := time.Now().UTC().Add(leaseTTL)
		if err := s.jobs.Lease(ctx, candidate.ID, leaseID, runnerID, deadline); err != nil {
			if errors.Is(err, store.ErrIllegalTransition) {
				// Lost the race for this candidate (another runner's
				// AcquireJob leased it first, or it was requeued/cancelled
				// between ListQueuedJobs and Lease) -- try the next one.
				continue
			}
			return nil, uuid.Nil, err
		}
		leased, err := s.jobs.GetJob(ctx, candidate.ID)
		if err != nil {
			return nil, uuid.Nil, err
		}
		return leased, leaseID, nil
	}
	return nil, uuid.Nil, nil
}

// jobsByJobIDForRun returns runID's job rows grouped by JobIDYAML (T-M4-01:
// a matrixed job has multiple rows sharing one JobIDYAML), populating cache
// on first use so repeated lookups for candidates sharing a run within one
// acquireOneQueuedJob call don't re-query the same run.
func (s *Service) jobsByJobIDForRun(ctx context.Context, runID uuid.UUID, cache map[uuid.UUID]map[string][]*store.Job) (map[string][]*store.Job, error) {
	if byJobID, ok := cache[runID]; ok {
		return byJobID, nil
	}
	jobs, err := s.jobs.ListJobsForRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	byJobID := groupJobsByJobID(jobs)
	cache[runID] = byJobID
	return byJobID, nil
}

// buildJobLease assembles a JobLease from job's row (already leased --
// job.LeaseDeadline is authoritative and set by the Lease call in
// acquireOneQueuedJob) plus its workflow_runs row (T-M1-01) and
// internal/github.CloneURL (T-M1-03), per the field mapping table in
// tasks/M1/T-M1-04-dispatcher-queue.md. env/vars/secrets are left as empty
// maps (out of scope here). needs_outputs is assembled by
// buildNeedsOutputs (T-M3-02): by the time a job with needs[] reaches here
// it has already been leased, which T-M3-01's gating guarantees only
// happens once every need is terminal, so every entry has a persisted
// result (and outputs, for a "success" need).
func (s *Service) buildJobLease(ctx context.Context, job *store.Job, leaseID uuid.UUID, leaseTTL time.Duration) (*dispatchpb.JobLease, error) {
	run, err := s.runs.GetRun(ctx, job.RunID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load run %s for job %s: %v", job.RunID, job.ID, err)
	}

	needsOutputs, err := s.buildNeedsOutputs(ctx, job)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "assemble needs_outputs for job %s: %v", job.ID, err)
	}

	matrixCellJSON := ""
	if len(job.MatrixCellJSON) > 0 {
		matrixCellJSON = string(job.MatrixCellJSON)
	}

	leaseDeadlineUnix := time.Now().UTC().Add(leaseTTL).Unix()
	if job.LeaseDeadline != nil {
		leaseDeadlineUnix = job.LeaseDeadline.Unix()
	}

	repoURL := s.gh.CloneURL(run.Repo)
	log.Printf("dispatch: AcquireJob: leased job %s (run %s) as lease %s, repo=%s", job.ID, job.RunID, leaseID, github.Redact(repoURL))

	return &dispatchpb.JobLease{
		LeaseId:           leaseID.String(),
		JobRunId:          job.ID.String(),
		WorkflowYaml:      run.WorkflowYAML,
		JobId:             job.JobIDYAML,
		MatrixCellJson:    matrixCellJSON,
		EventName:         run.Event,
		EventJson:         string(run.EventPayloadJSON),
		RepoUrl:           repoURL,
		Ref:               run.Ref,
		Sha:               run.SHA,
		Actor:             run.TriggeredBy,
		Env:               map[string]string{},
		Vars:              map[string]string{},
		Secrets:           map[string]string{},
		NeedsOutputs:      needsOutputs,
		LeaseDeadlineUnix: leaseDeadlineUnix,
	}, nil
}

// buildNeedsOutputs resolves every entry of job.Needs (YAML job_id) to its
// sibling job row(s) in the same run and reads back that need's terminal
// {result, outputs} (T-M3-02), keyed by the YAML job_id -- exactly what
// wf.Jobs[need] is keyed by on the runner (see CONVENTIONS.md's "Cross-
// machine needs bridge"). Returns an empty (non-nil) map for a job with no
// needs.
//
// T-M4-01 §5 canonical-cell simplification: when a need's job_id_yaml
// resolves to MULTIPLE rows (a matrixed need -- e.g. `build` ran as a 2x2
// matrix), there is no single well-defined {result, outputs} for the
// group, so this picks one canonical row per canonicalNeedRow's "last-
// completed cell wins" rule and reports only that cell's outputs. Per-cell
// output routing (e.g. exposing every cell's outputs individually to the
// dependent) is explicitly deferred to T-M4-02.
//
// A need id that can't be resolved to a terminal sibling row is logged and
// simply omitted rather than failing the whole lease -- by contract
// (T-M3-01 gates leasing on every need's entire group being terminal) this
// should never happen, but a JobLease missing one map entry is far less
// damaging than an AcquireJob stream erroring out entirely.
func (s *Service) buildNeedsOutputs(ctx context.Context, job *store.Job) (map[string]*dispatchpb.NeedOutputs, error) {
	needsOutputs := make(map[string]*dispatchpb.NeedOutputs, len(job.Needs))
	if len(job.Needs) == 0 {
		return needsOutputs, nil
	}

	siblings, err := s.jobs.ListJobsForRun(ctx, job.RunID)
	if err != nil {
		return nil, fmt.Errorf("list jobs for run %s: %w", job.RunID, err)
	}
	byJobID := groupJobsByJobID(siblings)

	for _, need := range job.Needs {
		n := canonicalNeedRow(byJobID[need])
		if n == nil || n.Result == nil {
			log.Printf("dispatch: buildNeedsOutputs: job %s need %q has no terminal result; omitting from needs_outputs", job.ID, need)
			continue
		}
		outs, err := s.outputs.ListOutputs(ctx, n.ID)
		if err != nil {
			return nil, fmt.Errorf("list outputs for need %q (job %s): %w", need, n.ID, err)
		}
		needsOutputs[need] = &dispatchpb.NeedOutputs{
			Result:  *n.Result,
			Outputs: outs,
		}
	}
	return needsOutputs, nil
}

// canonicalNeedRow picks the single row to treat as "the" need out of rows
// (every jobs row sharing one JobIDYAML) for needs_outputs purposes. This is
// the MVP simplification documented in the master plan / T-M4-01 §5: a
// matrixed need has no single natural {result, outputs} pair, so the
// "last-completed cell wins" -- the row with the latest finished_at. A row
// with no finished_at (shouldn't happen once T-M3-01's gating has confirmed
// the whole group is terminal, but handled defensively) sorts before any
// row that has one; ties are broken deterministically by row id so this
// function's result never depends on slice iteration order.
//
// A non-matrix need's group is always exactly one row, so this degenerates
// to returning that row -- unchanged behavior from before T-M4-01.
func canonicalNeedRow(rows []*store.Job) *store.Job {
	var canonical *store.Job
	for _, n := range rows {
		if n.Result == nil {
			continue
		}
		switch {
		case canonical == nil:
			canonical = n
		case n.FinishedAt == nil:
			// n has no finished_at to compare with; keep the current
			// canonical row.
		case canonical.FinishedAt == nil:
			canonical = n
		case n.FinishedAt.After(*canonical.FinishedAt):
			canonical = n
		case n.FinishedAt.Equal(*canonical.FinishedAt) && n.ID.String() > canonical.ID.String():
			canonical = n
		}
	}
	if canonical == nil && len(rows) > 0 {
		// No row in the group has a result yet -- contractually shouldn't
		// happen (needs-gating only calls this once the whole group is
		// terminal), but fall back to the first row rather than nil so a
		// caller sees a row (with n.Result == nil, which it already checks
		// for) instead of silently dropping the need.
		canonical = rows[0]
	}
	return canonical
}

// StartJobReaper ticks every jobReaperInterval and calls
// JobStore.RequeueExpired to flip any leased job whose lease_deadline has
// passed (no heartbeat renewal in time) back to queued, clearing its lease
// fields. It is idempotent: the next lease on a requeued job gets a fresh
// lease_id, so a late report/heartbeat/log under the old lease_id is
// already treated as stale everywhere else in this service
// (resolveLeaseJob). Blocks until ctx is cancelled; run it in its own
// goroutine and cancel ctx on shutdown, alongside RunReaper.
func (s *Service) StartJobReaper(ctx context.Context) {
	ticker := time.NewTicker(jobReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.jobs.RequeueExpired(ctx, time.Now().UTC())
			if err != nil {
				log.Printf("dispatch: job reaper: requeue expired: %v", err)
				continue
			}
			if n > 0 {
				log.Printf("dispatch: job reaper: requeued %d expired lease(s)", n)
				// Wake any runner blocked in AcquireJob so a freshly
				// requeued job is picked up promptly instead of waiting
				// out jobPollInterval.
				s.Notify()
			}
		}
	}
}
