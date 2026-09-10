// This file implements T-M1-05: the /api/runs and /api/jobs REST surface.
// It wires internal/github (resolve ref + fetch workflow YAML) ->
// internal/planner (schema-validate + plan + persist run/jobs) ->
// (optionally) the dispatcher's runner-wakeup notification, and exposes
// read endpoints backed by internal/store.
//
// All dependencies the POST handler needs to reach out over the network or
// mutate state are expressed as small interfaces on RunsDeps (RunsGitHub,
// planner.Planner, RunsNotifier) so tests can fake the GitHub call without
// touching the network, per the task's scope note. Helper identifiers are
// prefixed runs* to avoid clashing with sibling files in package api (e.g.
// runners.go already defines its own JSON helpers/handlers).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/chuan99nd/drassi-platform/internal/github"
	"github.com/chuan99nd/drassi-platform/internal/planner"
	"github.com/chuan99nd/drassi-platform/internal/store"
)

// runsRepoPattern matches the required "owner/name" shape for the repo
// field, checked before ever calling out to GitHub so a malformed repo
// fails fast with 400 instead of a confusing not-found from the API.
var runsRepoPattern = regexp.MustCompile(`^[^/]+/[^/]+$`)

// RunsGitHub is the subset of *github.Client the runs API needs. It exists
// so tests can fake GitHub without hitting the network; *github.Client
// (internal/github) satisfies it structurally.
type RunsGitHub interface {
	ResolveRef(ctx context.Context, repo, ref string) (string, error)
	FetchWorkflow(ctx context.Context, repo, sha, path string) ([]byte, error)
}

// RunsNotifier is the subset of the dispatcher's wakeup mechanism the POST
// handler calls after a run has been planned, so a connected runner picks
// up the newly queued job(s) promptly. It is optional (RunsDeps.Notifier
// may be left nil, e.g. until T-M1-04's dispatch service exposes this) --
// the handler no-ops when it is unset, and still returns 201 either way.
type RunsNotifier interface {
	Notify()
}

// RunsDeps carries everything RegisterRuns' handlers need. Construct one
// per process (or per test) and pass it to RegisterRuns.
type RunsDeps struct {
	GitHub  RunsGitHub
	Planner planner.Planner

	Runs  store.RunStore
	Jobs  store.JobStore
	Steps store.StepStore

	// Notifier is told about newly-planned jobs so a connected runner can
	// react immediately instead of waiting for its next poll. Optional.
	Notifier RunsNotifier

	// TriggeredBy is the placeholder actor recorded on runs created via
	// POST /api/runs until real auth exists (see task's "Out of scope").
	// Defaults to "manual" when empty.
	TriggeredBy string
}

func (d RunsDeps) triggeredBy() string {
	if d.TriggeredBy == "" {
		return "manual"
	}
	return d.TriggeredBy
}

// RegisterRuns adds the four /api/runs + /api/jobs routes documented in
// tasks/M1/T-M1-05-runs-api.md to r. Mount r at "/api" (e.g. via
// r.Route("/api", func(r chi.Router) { api.RegisterRuns(r, deps) }));
// RegisterRuns itself only ever registers relative paths.
func RegisterRuns(r chi.Router, deps RunsDeps) {
	r.Post("/runs", runsCreateHandler(deps))
	r.Get("/runs", runsListHandler(deps))
	r.Get("/runs/{id}", runsGetHandler(deps))
	r.Get("/jobs/{id}", runsGetJobHandler(deps))
}

// --- JSON DTOs (snake_case wire contract; never the raw store structs) ---

type runsCreateRequest struct {
	Repo         string `json:"repo"`
	Ref          string `json:"ref"`
	WorkflowPath string `json:"workflow_path"`
}

type runsCreateResponse struct {
	RunID string `json:"run_id"`
}

type runsErrorResponse struct {
	Error string `json:"error"`
}

// runSummary is the shape returned in the GET /api/runs list.
type runSummary struct {
	ID        string `json:"id"`
	Repo      string `json:"repo"`
	Ref       string `json:"ref"`
	SHA       string `json:"sha"`
	Event     string `json:"event"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type runsListResponse struct {
	Runs []runSummary `json:"runs"`
}

// runDetailJob is the shape of one entry in GET /api/runs/{id}'s "jobs".
type runDetailJob struct {
	ID        string          `json:"id"`
	JobIDYAML string          `json:"job_id_yaml"`
	Name      string          `json:"name"`
	Status    string          `json:"status"`
	Needs     []string        `json:"needs"`
	Matrix    json.RawMessage `json:"matrix"`
}

type runDetailResponse struct {
	ID          string         `json:"id"`
	Repo        string         `json:"repo"`
	Ref         string         `json:"ref"`
	SHA         string         `json:"sha"`
	Event       string         `json:"event"`
	TriggeredBy string         `json:"triggered_by"`
	Status      string         `json:"status"`
	CreatedAt   string         `json:"created_at"`
	Jobs        []runDetailJob `json:"jobs"`
}

// jobDetailStep is the shape of one entry in GET /api/jobs/{id}'s "steps".
type jobDetailStep struct {
	StepIndex  int     `json:"step_index"`
	Name       string  `json:"name"`
	Status     string  `json:"status"`
	StartedAt  *string `json:"started_at"`
	FinishedAt *string `json:"finished_at"`
}

type jobDetailResponse struct {
	ID         string          `json:"id"`
	RunID      string          `json:"run_id"`
	JobIDYAML  string          `json:"job_id_yaml"`
	Name       string          `json:"name"`
	Status     string          `json:"status"`
	Result     *string         `json:"result"`
	Needs      []string        `json:"needs"`
	Matrix     json.RawMessage `json:"matrix"`
	StartedAt  *string         `json:"started_at"`
	FinishedAt *string         `json:"finished_at"`
	Steps      []jobDetailStep `json:"steps"`
}

// --- DTO builders ---

// runsFormatTime renders t as RFC3339 UTC, the wire format for every
// timestamp this API emits.
func runsFormatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// runsFormatTimePtr is runsFormatTime for a nullable *time.Time: nil stays
// nil (encodes as JSON null) rather than becoming the zero time.
func runsFormatTimePtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := runsFormatTime(*t)
	return &s
}

func runsToSummary(r *store.Run) runSummary {
	return runSummary{
		ID:        r.ID.String(),
		Repo:      r.Repo,
		Ref:       r.Ref,
		SHA:       r.SHA,
		Event:     r.Event,
		Status:    r.Status,
		CreatedAt: runsFormatTime(r.CreatedAt),
	}
}

// runsMatrixField renders j.MatrixCellJSON (the store's raw jsonb bytes) as
// the wire "matrix" field: json.RawMessage's own MarshalJSON already encodes
// a nil/empty value as the JSON literal `null` (T-M4-01: null/{} means "not
// a matrix job"), so this only needs to normalize the empty-vs-nil-slice
// distinction before handing it to encoding/json.
func runsMatrixField(j *store.Job) json.RawMessage {
	if len(j.MatrixCellJSON) == 0 {
		return nil
	}
	return json.RawMessage(j.MatrixCellJSON)
}

func runsToDetailJob(j *store.Job) runDetailJob {
	needs := j.Needs
	if needs == nil {
		needs = []string{}
	}
	return runDetailJob{
		ID:        j.ID.String(),
		JobIDYAML: j.JobIDYAML,
		Name:      j.Name,
		Status:    j.Status,
		Needs:     needs,
		Matrix:    runsMatrixField(j),
	}
}

func runsToDetail(r *store.Run, jobs []*store.Job) runDetailResponse {
	jobResponses := make([]runDetailJob, 0, len(jobs))
	for _, j := range jobs {
		jobResponses = append(jobResponses, runsToDetailJob(j))
	}
	return runDetailResponse{
		ID:          r.ID.String(),
		Repo:        r.Repo,
		Ref:         r.Ref,
		SHA:         r.SHA,
		Event:       r.Event,
		TriggeredBy: r.TriggeredBy,
		Status:      r.Status,
		CreatedAt:   runsFormatTime(r.CreatedAt),
		Jobs:        jobResponses,
	}
}

func runsToStep(s *store.Step) jobDetailStep {
	return jobDetailStep{
		StepIndex:  s.StepIndex,
		Name:       s.Name,
		Status:     s.Status,
		StartedAt:  runsFormatTimePtr(s.StartedAt),
		FinishedAt: runsFormatTimePtr(s.FinishedAt),
	}
}

func runsToJobDetail(j *store.Job, steps []*store.Step) jobDetailResponse {
	needs := j.Needs
	if needs == nil {
		needs = []string{}
	}
	stepResponses := make([]jobDetailStep, 0, len(steps))
	for _, s := range steps {
		stepResponses = append(stepResponses, runsToStep(s))
	}
	return jobDetailResponse{
		ID:         j.ID.String(),
		RunID:      j.RunID.String(),
		JobIDYAML:  j.JobIDYAML,
		Name:       j.Name,
		Status:     j.Status,
		Result:     j.Result,
		Needs:      needs,
		Matrix:     runsMatrixField(j),
		StartedAt:  runsFormatTimePtr(j.StartedAt),
		FinishedAt: runsFormatTimePtr(j.FinishedAt),
		Steps:      stepResponses,
	}
}

// --- wire helpers ---

func runsWriteJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: encode runs response: %v", err)
	}
}

func runsWriteError(w http.ResponseWriter, status int, msg string) {
	runsWriteJSON(w, status, runsErrorResponse{Error: msg})
}

// runsInternalError logs the real error (which may reference internal
// detail) and writes a generic 500 body, so no internal detail (or a
// tokenized clone URL) ever reaches the client -- see the task's "No
// secret ... in any response body" DoD bullet.
func runsInternalError(w http.ResponseWriter, context string, err error) {
	log.Printf("api: %s: %v", context, err)
	runsWriteError(w, http.StatusInternalServerError, "internal error")
}

// --- handlers ---

func runsCreateHandler(deps RunsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		var body runsCreateRequest
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			runsWriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		if body.Repo == "" || body.Ref == "" || body.WorkflowPath == "" {
			runsWriteError(w, http.StatusBadRequest, "repo, ref, and workflow_path are required")
			return
		}
		if !runsRepoPattern.MatchString(body.Repo) {
			runsWriteError(w, http.StatusBadRequest, `repo must look like "owner/name"`)
			return
		}

		ctx := req.Context()

		sha, err := deps.GitHub.ResolveRef(ctx, body.Repo, body.Ref)
		if err != nil {
			runsHandleUpstreamError(w, "resolve ref", err)
			return
		}

		workflowYAML, err := deps.GitHub.FetchWorkflow(ctx, body.Repo, sha, body.WorkflowPath)
		if err != nil {
			runsHandleUpstreamError(w, "fetch workflow", err)
			return
		}

		result, err := deps.Planner.Plan(ctx, planner.PlanInput{
			Repo:         body.Repo,
			Ref:          body.Ref,
			SHA:          sha,
			EventName:    "manual",
			EventPayload: []byte("{}"),
			TriggeredBy:  deps.triggeredBy(),
			WorkflowYAML: workflowYAML,
		})
		if err != nil {
			runsHandleUpstreamError(w, "plan workflow", err)
			return
		}

		if deps.Notifier != nil {
			deps.Notifier.Notify()
		}

		runsWriteJSON(w, http.StatusCreated, runsCreateResponse{RunID: result.RunID.String()})
	}
}

// runsHandleUpstreamError maps a resolve/fetch/plan error to the documented
// HTTP status: github.ErrNotFound and planner.ErrInvalidWorkflow are bad
// input (400, with the underlying message -- neither ever embeds a token or
// clone URL), everything else is an unexpected failure (500, generic body).
func runsHandleUpstreamError(w http.ResponseWriter, context string, err error) {
	switch {
	case errors.Is(err, github.ErrNotFound):
		runsWriteError(w, http.StatusBadRequest, "repo, ref, or workflow_path not found")
	case errors.Is(err, planner.ErrInvalidWorkflow):
		runsWriteError(w, http.StatusBadRequest, err.Error())
	default:
		runsInternalError(w, context, err)
	}
}

func runsListHandler(deps RunsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		limit := runsParseIntParam(req, "limit", 50)
		offset := runsParseIntParam(req, "offset", 0)

		runs, err := deps.Runs.ListRuns(req.Context(), limit, offset)
		if err != nil {
			runsInternalError(w, "list runs", err)
			return
		}

		resp := runsListResponse{Runs: make([]runSummary, 0, len(runs))}
		for _, r := range runs {
			resp.Runs = append(resp.Runs, runsToSummary(r))
		}
		runsWriteJSON(w, http.StatusOK, resp)
	}
}

// runsParseIntParam parses the named query param as a non-negative int,
// falling back to def when the param is absent, empty, or not a valid
// non-negative integer.
func runsParseIntParam(req *http.Request, name string, def int) int {
	raw := req.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return def
	}
	return v
}

func runsGetHandler(deps RunsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		id, err := uuid.Parse(chi.URLParam(req, "id"))
		if err != nil {
			runsWriteError(w, http.StatusBadRequest, "invalid run id")
			return
		}

		run, err := deps.Runs.GetRun(req.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				runsWriteError(w, http.StatusNotFound, "run not found")
				return
			}
			runsInternalError(w, "get run", err)
			return
		}

		jobs, err := deps.Jobs.ListJobsForRun(req.Context(), id)
		if err != nil {
			runsInternalError(w, "list jobs for run", err)
			return
		}

		runsWriteJSON(w, http.StatusOK, runsToDetail(run, jobs))
	}
}

func runsGetJobHandler(deps RunsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		id, err := uuid.Parse(chi.URLParam(req, "id"))
		if err != nil {
			runsWriteError(w, http.StatusBadRequest, "invalid job id")
			return
		}

		job, err := deps.Jobs.GetJob(req.Context(), id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				runsWriteError(w, http.StatusNotFound, "job not found")
				return
			}
			runsInternalError(w, "get job", err)
			return
		}

		steps, err := deps.Steps.ListStepsForJob(req.Context(), id)
		if err != nil {
			runsInternalError(w, "list steps for job", err)
			return
		}

		runsWriteJSON(w, http.StatusOK, runsToJobDetail(job, steps))
	}
}
