package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/chuan99nd/drassi-platform/internal/api"
	"github.com/chuan99nd/drassi-platform/internal/github"
	"github.com/chuan99nd/drassi-platform/internal/planner"
	"github.com/chuan99nd/drassi-platform/internal/store"
)

// singleJobYAML is a minimal, schema-valid, single-job workflow -- same
// shape used by internal/planner's own tests.
const singleJobYAML = `
name: single
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
`

// malformedYAML has a top-level key pkg/schema rejects, so Plan must return
// a wrapped planner.ErrInvalidWorkflow without persisting anything.
const malformedYAML = `
name: malformed
on: push
bogus_top_level_key: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
`

// fakeGitHub is a RunsGitHub fake so POST /api/runs tests never hit the
// network (per the task's scope note: "define RunsDeps to accept an
// interface for the github client to allow faking").
type fakeGitHub struct {
	sha         string
	resolveErr  error
	workflow    []byte
	fetchErr    error
	resolveCall func(repo, ref string)
	fetchCall   func(repo, sha, path string)
}

func (f *fakeGitHub) ResolveRef(_ context.Context, repo, ref string) (string, error) {
	if f.resolveCall != nil {
		f.resolveCall(repo, ref)
	}
	if f.resolveErr != nil {
		return "", f.resolveErr
	}
	return f.sha, nil
}

func (f *fakeGitHub) FetchWorkflow(_ context.Context, repo, sha, path string) ([]byte, error) {
	if f.fetchCall != nil {
		f.fetchCall(repo, sha, path)
	}
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	return f.workflow, nil
}

// fakeNotifier records Notify() calls.
type fakeNotifier struct{ calls int }

func (f *fakeNotifier) Notify() { f.calls++ }

// testRunsDeps opens a real Postgres pool from DATABASE_URL (skipping the
// test if unset, same convention as internal/planner and internal/store's
// own integration tests), wires real Run/Job/Step stores + a real Planner,
// and returns ready-to-use RunsDeps plus the pool for assertions/cleanup.
func testRunsDeps(t *testing.T, gh api.RunsGitHub, notifier api.RunsNotifier) (api.RunsDeps, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping runs API integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	runs := store.NewRunStore(pool)
	jobs := store.NewJobStore(pool)
	steps := store.NewStepStore(pool)

	return api.RunsDeps{
		GitHub:   gh,
		Planner:  planner.New(runs, jobs),
		Runs:     runs,
		Jobs:     jobs,
		Steps:    steps,
		Notifier: notifier,
	}, pool
}

func newRunsRouter(deps api.RunsDeps) http.Handler {
	r := chi.NewRouter()
	api.RegisterRuns(r, deps)
	return r
}

func cleanupTestRun(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, `DELETE FROM workflow_runs WHERE id = $1`, runID); err != nil {
			t.Logf("cleanup: failed to delete test run %s: %v", runID, err)
		}
	})
}

func TestRunsCreate_Success(t *testing.T) {
	notifier := &fakeNotifier{}
	gh := &fakeGitHub{sha: "deadbeefcafe", workflow: []byte(singleJobYAML)}
	deps, pool := testRunsDeps(t, gh, notifier)
	router := newRunsRouter(deps)

	body := `{"repo":"octo/example","ref":"main","workflow_path":".github/workflows/ci.yml"}`
	req := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var created struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}
	runID, err := uuid.Parse(created.RunID)
	if err != nil {
		t.Fatalf("run_id is not a uuid: %v", created.RunID)
	}
	cleanupTestRun(t, pool, runID)

	if notifier.calls != 1 {
		t.Fatalf("expected Notify to be called exactly once, got %d", notifier.calls)
	}

	// Verify via GET /api/runs/{id} that the job(s) are queued.
	getReq := httptest.NewRequest(http.MethodGet, "/runs/"+created.RunID, nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET /runs/{id}: expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}

	var detail struct {
		ID          string `json:"id"`
		Repo        string `json:"repo"`
		Ref         string `json:"ref"`
		SHA         string `json:"sha"`
		Event       string `json:"event"`
		TriggeredBy string `json:"triggered_by"`
		Status      string `json:"status"`
		CreatedAt   string `json:"created_at"`
		Jobs        []struct {
			ID        string   `json:"id"`
			JobIDYAML string   `json:"job_id_yaml"`
			Name      string   `json:"name"`
			Status    string   `json:"status"`
			Needs     []string `json:"needs"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode run detail: %v; body=%s", err, getRec.Body.String())
	}

	if detail.ID != created.RunID {
		t.Fatalf("expected id %q, got %q", created.RunID, detail.ID)
	}
	if detail.Repo != "octo/example" || detail.Ref != "main" || detail.SHA != "deadbeefcafe" {
		t.Fatalf("unexpected run fields: %+v", detail)
	}
	if detail.Event != "manual" {
		t.Fatalf("expected event 'manual', got %q", detail.Event)
	}
	if detail.TriggeredBy == "" {
		t.Fatalf("expected a non-empty triggered_by placeholder")
	}
	if detail.Status != "queued" {
		t.Fatalf("expected run status 'queued', got %q", detail.Status)
	}
	if _, err := time.Parse(time.RFC3339, detail.CreatedAt); err != nil {
		t.Fatalf("created_at not RFC3339: %q: %v", detail.CreatedAt, err)
	}
	if len(detail.Jobs) != 1 {
		t.Fatalf("expected exactly 1 job, got %d: %+v", len(detail.Jobs), detail.Jobs)
	}
	job := detail.Jobs[0]
	if job.JobIDYAML != "build" || job.Name != "build" {
		t.Fatalf("unexpected job: %+v", job)
	}
	if job.Status != "queued" {
		t.Fatalf("expected job status 'queued', got %q", job.Status)
	}
	if job.Needs == nil || len(job.Needs) != 0 {
		t.Fatalf("expected needs to be an empty array (never null), got %+v", job.Needs)
	}

	// No secret/token should ever leak into the response body.
	if bytes.Contains(getRec.Body.Bytes(), []byte("x-access-token")) {
		t.Fatalf("response body leaked a tokenized clone URL: %s", getRec.Body.String())
	}

	// GET /api/jobs/{id} shape check.
	jobReq := httptest.NewRequest(http.MethodGet, "/jobs/"+job.ID, nil)
	jobRec := httptest.NewRecorder()
	router.ServeHTTP(jobRec, jobReq)
	if jobRec.Code != http.StatusOK {
		t.Fatalf("GET /jobs/{id}: expected 200, got %d: %s", jobRec.Code, jobRec.Body.String())
	}

	var jobDetail struct {
		ID         string        `json:"id"`
		RunID      string        `json:"run_id"`
		JobIDYAML  string        `json:"job_id_yaml"`
		Name       string        `json:"name"`
		Status     string        `json:"status"`
		Result     interface{}   `json:"result"`
		Needs      []string      `json:"needs"`
		StartedAt  interface{}   `json:"started_at"`
		FinishedAt interface{}   `json:"finished_at"`
		Steps      []interface{} `json:"steps"`
	}
	if err := json.Unmarshal(jobRec.Body.Bytes(), &jobDetail); err != nil {
		t.Fatalf("decode job detail: %v; body=%s", err, jobRec.Body.String())
	}
	if jobDetail.ID != job.ID || jobDetail.RunID != created.RunID {
		t.Fatalf("unexpected job detail linkage: %+v", jobDetail)
	}
	if jobDetail.Result != nil {
		t.Fatalf("expected result to be null before execution, got %+v", jobDetail.Result)
	}
	if jobDetail.Needs == nil {
		t.Fatalf("expected needs to be [] not null")
	}
	if jobDetail.Steps == nil {
		t.Fatalf("expected steps to be [] not null")
	}
}

func TestRunsCreate_InvalidRepoFormat(t *testing.T) {
	gh := &fakeGitHub{resolveCall: func(repo, ref string) {
		t.Fatalf("ResolveRef must not be called for a malformed repo")
	}}
	deps, _ := testRunsDeps(t, gh, nil)
	router := newRunsRouter(deps)

	body := `{"repo":"notarepo","ref":"main","workflow_path":".github/workflows/ci.yml"}`
	req := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec.Body.Bytes())
}

func TestRunsCreate_MissingField(t *testing.T) {
	deps, _ := testRunsDeps(t, &fakeGitHub{}, nil)
	router := newRunsRouter(deps)

	body := `{"repo":"octo/example","ref":"main"}`
	req := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec.Body.Bytes())
}

func TestRunsCreate_GitHubNotFound(t *testing.T) {
	gh := &fakeGitHub{resolveErr: fmt.Errorf("github: ResolveRef: %w", github.ErrNotFound)}
	deps, _ := testRunsDeps(t, gh, nil)
	router := newRunsRouter(deps)

	body := `{"repo":"octo/example","ref":"no-such-branch","workflow_path":".github/workflows/ci.yml"}`
	req := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec.Body.Bytes())
}

func TestRunsCreate_InvalidWorkflow(t *testing.T) {
	gh := &fakeGitHub{sha: "deadbeef", workflow: []byte(malformedYAML)}
	deps, pool := testRunsDeps(t, gh, nil)
	router := newRunsRouter(deps)

	var before int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&before); err != nil {
		t.Fatalf("count workflow_runs (before): %v", err)
	}

	body := `{"repo":"octo/example","ref":"main","workflow_path":".github/workflows/ci.yml"}`
	req := httptest.NewRequest(http.MethodPost, "/runs", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec.Body.Bytes())

	var after int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&after); err != nil {
		t.Fatalf("count workflow_runs (after): %v", err)
	}
	if before != after {
		t.Fatalf("an invalid workflow must not persist a run row: before=%d after=%d", before, after)
	}
}

func TestRunsList(t *testing.T) {
	deps, pool := testRunsDeps(t, &fakeGitHub{}, nil)
	router := newRunsRouter(deps)

	runID := uuid.New()
	run := &store.Run{
		ID:               runID,
		Repo:             "octo/list-example",
		Ref:              "main",
		SHA:              "abc123",
		Event:            "manual",
		EventPayloadJSON: []byte(`{}`),
		TriggeredBy:      "manual",
		Status:           "queued",
	}
	if err := deps.Runs.CreateRun(context.Background(), run); err != nil {
		t.Fatalf("CreateRun (seed): %v", err)
	}
	cleanupTestRun(t, pool, runID)

	req := httptest.NewRequest(http.MethodGet, "/runs", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var listResp struct {
		Runs []struct {
			ID        string `json:"id"`
			Repo      string `json:"repo"`
			Ref       string `json:"ref"`
			SHA       string `json:"sha"`
			Event     string `json:"event"`
			Status    string `json:"status"`
			CreatedAt string `json:"created_at"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode list response: %v; body=%s", err, rec.Body.String())
	}

	var found bool
	for _, r := range listResp.Runs {
		if r.ID != runID.String() {
			continue
		}
		found = true
		if r.Repo != "octo/list-example" || r.Ref != "main" || r.SHA != "abc123" || r.Event != "manual" || r.Status != "queued" {
			t.Fatalf("unexpected run summary fields: %+v", r)
		}
		if _, err := time.Parse(time.RFC3339, r.CreatedAt); err != nil {
			t.Fatalf("created_at not RFC3339: %q: %v", r.CreatedAt, err)
		}
	}
	if !found {
		t.Fatalf("expected seeded run %s in GET /runs list, got %+v", runID, listResp.Runs)
	}
}

func TestRunsGet_NotFound(t *testing.T) {
	deps, _ := testRunsDeps(t, &fakeGitHub{}, nil)
	router := newRunsRouter(deps)

	req := httptest.NewRequest(http.MethodGet, "/runs/"+uuid.New().String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec.Body.Bytes())
}

func TestRunsGetJob_NotFound(t *testing.T) {
	deps, _ := testRunsDeps(t, &fakeGitHub{}, nil)
	router := newRunsRouter(deps)

	req := httptest.NewRequest(http.MethodGet, "/jobs/"+uuid.New().String(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
	assertErrorBody(t, rec.Body.Bytes())
}

func assertErrorBody(t *testing.T, body []byte) {
	t.Helper()
	var errResp struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("decode error response: %v; body=%s", err, body)
	}
	if errResp.Error == "" {
		t.Fatalf("expected a non-empty error message, got body=%s", body)
	}
}
