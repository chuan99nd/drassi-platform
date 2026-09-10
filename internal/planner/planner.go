// Package planner is the orchestrator's "break a workflow into jobs" step.
// It schema-validates a workflow YAML (via act's pkg/schema, invoked from
// pkg/model.ReadWorkflow), parses it into an act model.Workflow, plans it
// (pkg/model.WorkflowPlanner), and persists exactly one workflow_runs row
// plus one jobs row per *matrix cell* (status "queued", with its needs and
// if_expr) -- a non-matrix job has exactly one (empty) cell, so it still
// gets exactly one jobs row (see T-M4-01).
//
// Fetching the workflow YAML/resolving the ref to a sha is T-M1-03's job;
// this package only consumes them.
package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/nektos/act/pkg/model"

	"github.com/chuan99nd/drassi-platform/internal/store"
)

// PlanInput is everything Plan needs to schema-validate a workflow, plan its
// jobs, and persist the run + job rows.
type PlanInput struct {
	Repo         string // owner/name
	Ref          string
	SHA          string
	EventName    string // "push" | "pull_request" | "manual"
	EventPayload []byte // JSON; "{}" ok for manual
	TriggeredBy  string
	WorkflowYAML []byte // verbatim YAML fetched by T-M1-03
}

// PlanResult is what Plan created.
type PlanResult struct {
	RunID uuid.UUID

	// JobIDs are the created jobs rows' ids, in plan order (stage order,
	// then run order within a stage).
	JobIDs []uuid.UUID
}

// Planner turns a workflow YAML + trigger context into a persisted run plus
// one queued job row per job.
type Planner interface {
	Plan(ctx context.Context, in PlanInput) (*PlanResult, error)
}

type planner struct {
	runs store.RunStore
	jobs store.JobStore
}

// New builds a Planner backed by runs/jobs stores (see T-M1-01).
func New(runs store.RunStore, jobs store.JobStore) Planner {
	return &planner{runs: runs, jobs: jobs}
}

// Plan schema-validates in.WorkflowYAML, parses + plans it with act's
// pkg/model, and persists 1 workflow_runs row + 1 jobs row per job. On any
// parse/plan failure it returns an error wrapping ErrInvalidWorkflow and
// performs no writes at all (the run is only created after the workflow has
// parsed and planned successfully).
func (p *planner) Plan(ctx context.Context, in PlanInput) (*PlanResult, error) {
	// Parse + schema-validate independently first, purely to snapshot each
	// job's *raw* `if:` scalar before act's own planning mutates it. act's
	// Workflow.GetJob (called internally by createStages, which both
	// PlanEvent and PlanAll use) defaults job.If.Value to "success()" the
	// first time it is called for a job that had none at all -- see
	// pkg/model/workflow.go GetJob. That default-injection would clobber the
	// "empty if_expr means always run" contract T-M3-01 (gating) and
	// T-M3-03 (runner skip pre-eval) rely on, so we must read If.Value from
	// a workflow object act has not yet planned.
	rawWF, err := model.ReadWorkflow(bytes.NewReader(in.WorkflowYAML), false)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidWorkflow, err)
	}
	rawIfByJobID := make(map[string]string, len(rawWF.Jobs))
	for jobID, job := range rawWF.Jobs {
		rawIfByJobID[jobID] = job.If.Value
	}

	// Build the WorkflowPlanner from an independent second parse: it owns
	// its own *model.Workflow, so act mutating job.Name/job.If.Value inside
	// GetJob while planning below never touches rawWF/rawIfByJobID above.
	plannerName := fmt.Sprintf("%s@%s", in.Repo, in.Ref)
	wp, err := model.NewSingleWorkflowPlanner(plannerName, bytes.NewReader(in.WorkflowYAML))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidWorkflow, err)
	}

	plan, err := wp.PlanEvent(in.EventName)
	if err != nil {
		return nil, fmt.Errorf("%w: plan event %q: %v", ErrInvalidWorkflow, in.EventName, err)
	}
	if len(plan.Stages) == 0 {
		// "manual" dispatch (or any event name that matches none of the
		// workflow's `on:` triggers) has nothing to match against `on:` --
		// fall back to planning every job in the workflow.
		plan, err = wp.PlanAll()
		if err != nil {
			return nil, fmt.Errorf("%w: plan all: %v", ErrInvalidWorkflow, err)
		}
	}

	// Expand every planned job's matrix (T-M4-01) *before* touching the
	// database at all: job.GetMatrixes() can fail (a bad include/exclude
	// shape -- see act's pkg/model/workflow.go), and per this package's
	// existing "no partial writes" contract, that must surface as a plan-time
	// ErrInvalidWorkflow with zero rows persisted, exactly like a parse/plan
	// failure above. A job with no `strategy.matrix` yields a slice of one
	// empty map (act's documented behavior), which plannedJob below turns
	// into the historical single, non-matrix jobs row.
	plannedJobs := make([]plannedJob, 0, len(plan.Stages))
	for _, stage := range plan.Stages {
		for _, r := range stage.Runs {
			job := r.Job()
			if job == nil {
				continue
			}

			name := job.Name
			if name == "" {
				name = r.JobID
			}

			var ifExpr *string
			if raw := rawIfByJobID[r.JobID]; raw != "" {
				ifExpr = &raw
			}

			cells, err := job.GetMatrixes()
			if err != nil {
				return nil, fmt.Errorf("%w: job %q: invalid strategy.matrix: %v", ErrInvalidWorkflow, r.JobID, err)
			}

			plannedJobs = append(plannedJobs, plannedJob{
				jobIDYAML: r.JobID,
				name:      name,
				needs:     job.Needs(),
				ifExpr:    ifExpr,
				cells:     cells,
			})
		}
	}

	// Only now that the workflow (and every job's matrix) has parsed and
	// planned cleanly do we touch the database, so a malformed/unplannable
	// workflow never leaves a run or job row behind.
	runID := uuid.New()
	run := &store.Run{
		ID:               runID,
		Repo:             in.Repo,
		Ref:              in.Ref,
		SHA:              in.SHA,
		Event:            in.EventName,
		EventPayloadJSON: in.EventPayload,
		TriggeredBy:      in.TriggeredBy,
		Status:           "queued",
		// Persisted so the dispatcher (T-M1-04) can build
		// JobLease.workflow_yaml without re-fetching from GitHub.
		WorkflowYAML: string(in.WorkflowYAML),
	}
	if err := p.runs.CreateRun(ctx, run); err != nil {
		return nil, fmt.Errorf("planner: create run: %w", err)
	}

	result := &PlanResult{RunID: runID}
	for _, pj := range plannedJobs {
		for _, cell := range pj.cells {
			cellJSON, nameSuffix, err := encodeMatrixCell(cell)
			if err != nil {
				// GetMatrixes() already succeeded for this job above, so a
				// cell that fails to json.Marshal here would be a decode-node
				// oddity in the YAML's raw matrix values rather than a real
				// invalid-matrix shape -- still surfaced as a plan failure,
				// just after the run row already exists (see the CreateJob
				// error handling below for why a full rollback isn't
				// attempted).
				_ = p.runs.SetRunStatus(ctx, runID, "failure")
				return nil, fmt.Errorf("planner: encode matrix cell for job %q: %w", pj.jobIDYAML, err)
			}

			rowName := pj.name
			if nameSuffix != "" {
				rowName = fmt.Sprintf("%s (%s)", pj.name, nameSuffix)
			}

			jobRowID := uuid.New()
			jobRow := &store.Job{
				ID:             jobRowID,
				RunID:          runID,
				JobIDYAML:      pj.jobIDYAML,
				Name:           rowName,
				MatrixCellJSON: cellJSON,
				Needs:          pj.needs,
				IfExpr:         pj.ifExpr,
				Status:         "queued",
			}
			if err := p.jobs.CreateJob(ctx, jobRow); err != nil {
				// Best-effort recovery: RunStore has no Delete (T-M1-01
				// scope) and there is no shared transaction across
				// RunStore/JobStore, so we cannot fully roll back a run or
				// the job rows already created earlier in this loop. At
				// minimum, don't leave the run stuck reporting "queued"
				// when it is actually missing a job.
				_ = p.runs.SetRunStatus(ctx, runID, "failure")
				return nil, fmt.Errorf("planner: create job %q (cell %s): %w", pj.jobIDYAML, cellJSON, err)
			}
			result.JobIDs = append(result.JobIDs, jobRowID)
		}
	}

	return result, nil
}

// plannedJob is one act-planned job (pre matrix-expansion), captured after
// GetMatrixes() has already been validated for every job in the plan so the
// database-touching loop below (Plan's second half) never has to fail
// mid-way through persisting a job's matrix cells.
type plannedJob struct {
	jobIDYAML string
	name      string
	needs     []string
	ifExpr    *string
	cells     []map[string]interface{}
}

// encodeMatrixCell canonicalizes one map returned by Job.GetMatrixes() into
// the jobs.matrix_cell_json value to persist plus a display-name suffix.
//
//   - A non-matrix job's single empty cell (act's documented behavior --
//     see pkg/model/workflow.go GetMatrixes) encodes to (nil, ""): no
//     matrix, matrix_cell_json stays null, and the row keeps the plain job
//     name.
//   - A real cell encodes to canonical-sorted-key JSON (Go's encoding/json
//     already marshals a map's keys in sorted order, which is exactly the
//     canonical form T-M4-01 asks for) plus a "v1, v2" name suffix built
//     from the same sorted key order. GetMatrixes() returns a plain
//     map[string]interface{}, which does not preserve the YAML
//     `strategy.matrix` key declaration order, so sorted order is the
//     closest deterministic stand-in for "the same convention GitHub/act
//     uses" the task asks for -- documented here rather than attempting to
//     recover the original YAML key order.
func encodeMatrixCell(cell map[string]interface{}) (cellJSON []byte, nameSuffix string, err error) {
	if len(cell) == 0 {
		return nil, "", nil
	}

	keys := make([]string, 0, len(cell))
	for k := range cell {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	values := make([]string, 0, len(keys))
	for _, k := range keys {
		values = append(values, fmt.Sprintf("%v", cell[k]))
	}

	b, err := json.Marshal(cell)
	if err != nil {
		return nil, "", fmt.Errorf("marshal matrix cell: %w", err)
	}
	return b, strings.Join(values, ", "), nil
}
