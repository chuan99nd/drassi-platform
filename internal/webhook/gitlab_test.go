package webhook_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/chuan99nd/drassi-platform/internal/gitlab"
	"github.com/chuan99nd/drassi-platform/internal/planner"
	"github.com/chuan99nd/drassi-platform/internal/webhook"
)

const (
	testSecret  = "shhh-webhook-secret"
	testProject = "group/allowed-project"
)

// fakeGitLabFetcher is a GitLabFetcher test double that never touches the
// network.
type fakeGitLabFetcher struct {
	listPaths    []string
	listErr      error
	workflowYAML []byte
	fetchErr     error

	listCalls  int
	fetchCalls int
	fetchPath  string
}

func (f *fakeGitLabFetcher) ListWorkflows(ctx context.Context, project, sha string) ([]string, error) {
	f.listCalls++
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.listPaths, nil
}

func (f *fakeGitLabFetcher) FetchWorkflow(ctx context.Context, project, sha, path string) ([]byte, error) {
	f.fetchCalls++
	f.fetchPath = path
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	return f.workflowYAML, nil
}

// fakePlanner is a planner.Planner test double that records whether/how it
// was called, without touching a database.
type fakePlanner struct {
	calls  int
	lastIn planner.PlanInput
	err    error
}

func (p *fakePlanner) Plan(ctx context.Context, in planner.PlanInput) (*planner.PlanResult, error) {
	p.calls++
	p.lastIn = in
	if p.err != nil {
		return nil, p.err
	}
	return &planner.PlanResult{RunID: uuid.New()}, nil
}

// fakeNotifier records Notify() calls.
type fakeNotifier struct {
	calls int
}

func (n *fakeNotifier) Notify() { n.calls++ }

const validWorkflowYAML = "on: push\njobs:\n  build:\n    runs-on: -self-hosted\n    steps:\n      - run: echo hi\n"

func gitlabPushBody(t *testing.T, project, objectKind string) []byte {
	t.Helper()
	body := map[string]interface{}{
		"object_kind":   objectKind,
		"ref":           "refs/heads/main",
		"checkout_sha":  "deadbeefcafefeed0000000000000000000000",
		"user_username": "alice",
		"project": map[string]interface{}{
			"path_with_namespace": project,
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal push body: %v", err)
	}
	return b
}

func newGitLabTestServer(deps webhook.GitLabWebhookDeps) *httptest.Server {
	r := chi.NewRouter()
	r.Route("/api", func(r chi.Router) {
		webhook.RegisterGitLabWebhook(r, deps)
	})
	return httptest.NewServer(r)
}

func postGitLabWebhook(t *testing.T, srv *httptest.Server, token string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/webhooks/gitlab", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Gitlab-Event", "Push Hook")
	if token != "" {
		req.Header.Set("X-Gitlab-Token", token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST webhook: %v", err)
	}
	return resp
}

func TestGitLabWebhook_ValidTokenAndPush_AllowlistedProject_PlansAndReturns202(t *testing.T) {
	fetcher := &fakeGitLabFetcher{workflowYAML: []byte(validWorkflowYAML)}
	pl := &fakePlanner{}
	notifier := &fakeNotifier{}

	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       fetcher,
		Planner:      pl,
		Notifier:     notifier,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	body := gitlabPushBody(t, testProject, "push")
	resp := postGitLabWebhook(t, srv, testSecret, body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	var out struct {
		Status string `json:"status"`
		RunID  string `json:"run_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Status != "accepted" {
		t.Errorf("status field = %q, want %q", out.Status, "accepted")
	}
	if out.RunID == "" {
		t.Error("run_id is empty, want a created run id")
	}

	if pl.calls != 1 {
		t.Fatalf("planner.Plan calls = %d, want 1", pl.calls)
	}
	if pl.lastIn.Repo != testProject {
		t.Errorf("PlanInput.Repo = %q, want %q", pl.lastIn.Repo, testProject)
	}
	if pl.lastIn.EventName != "push" {
		t.Errorf("PlanInput.EventName = %q, want push", pl.lastIn.EventName)
	}
	if pl.lastIn.TriggeredBy != "alice" {
		t.Errorf("PlanInput.TriggeredBy = %q, want alice", pl.lastIn.TriggeredBy)
	}
	if string(pl.lastIn.WorkflowYAML) != validWorkflowYAML {
		t.Errorf("PlanInput.WorkflowYAML mismatch")
	}
	if !bytes.Equal(pl.lastIn.EventPayload, body) {
		t.Errorf("PlanInput.EventPayload not the raw push body")
	}
	if fetcher.fetchPath != ".github/workflows/ci.yml" {
		t.Errorf("FetchWorkflow path = %q, want configured WorkflowPath", fetcher.fetchPath)
	}
	if notifier.calls != 1 {
		t.Errorf("notifier.Notify calls = %d, want 1", notifier.calls)
	}
}

func TestGitLabWebhook_NoWorkflowPathConfigured_UsesListWorkflows(t *testing.T) {
	fetcher := &fakeGitLabFetcher{
		listPaths:    []string{".github/workflows/ci.yml", ".github/workflows/release.yml"},
		workflowYAML: []byte(validWorkflowYAML),
	}
	pl := &fakePlanner{}

	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       fetcher,
		Planner:      pl,
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	resp := postGitLabWebhook(t, srv, testSecret, gitlabPushBody(t, testProject, "push"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if fetcher.listCalls != 1 {
		t.Errorf("ListWorkflows calls = %d, want 1", fetcher.listCalls)
	}
	if fetcher.fetchPath != ".github/workflows/ci.yml" {
		t.Errorf("FetchWorkflow path = %q, want first listed workflow", fetcher.fetchPath)
	}
	if pl.calls != 1 {
		t.Errorf("planner.Plan calls = %d, want 1", pl.calls)
	}
}

func TestGitLabWebhook_BadToken_Returns401(t *testing.T) {
	fetcher := &fakeGitLabFetcher{workflowYAML: []byte(validWorkflowYAML)}
	pl := &fakePlanner{}

	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       fetcher,
		Planner:      pl,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	resp := postGitLabWebhook(t, srv, "wrong-token", gitlabPushBody(t, testProject, "push"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	if pl.calls != 0 {
		t.Errorf("planner.Plan calls = %d, want 0 (bad token must never plan)", pl.calls)
	}
	if fetcher.fetchCalls != 0 {
		t.Errorf("FetchWorkflow calls = %d, want 0 (bad token must never fetch)", fetcher.fetchCalls)
	}
}

func TestGitLabWebhook_MissingToken_Returns401(t *testing.T) {
	fetcher := &fakeGitLabFetcher{workflowYAML: []byte(validWorkflowYAML)}
	pl := &fakePlanner{}

	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       fetcher,
		Planner:      pl,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	resp := postGitLabWebhook(t, srv, "", gitlabPushBody(t, testProject, "push"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestGitLabWebhook_NonPushEvent_Returns202Ignored(t *testing.T) {
	fetcher := &fakeGitLabFetcher{workflowYAML: []byte(validWorkflowYAML)}
	pl := &fakePlanner{}

	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       fetcher,
		Planner:      pl,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	resp := postGitLabWebhook(t, srv, testSecret, gitlabPushBody(t, testProject, "tag_push"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (ignored, not an error)", resp.StatusCode, http.StatusAccepted)
	}
	var out struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Status != "ignored" {
		t.Errorf("status field = %q, want ignored", out.Status)
	}
	if pl.calls != 0 {
		t.Errorf("planner.Plan calls = %d, want 0 (non-push must never plan)", pl.calls)
	}
}

func TestGitLabWebhook_NonAllowlistedProject_Returns202IgnoredNoRun(t *testing.T) {
	fetcher := &fakeGitLabFetcher{workflowYAML: []byte(validWorkflowYAML)}
	pl := &fakePlanner{}

	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       fetcher,
		Planner:      pl,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	resp := postGitLabWebhook(t, srv, testSecret, gitlabPushBody(t, "someone-else/other-project", "push"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (ignored, not an error)", resp.StatusCode, http.StatusAccepted)
	}
	var out struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.Status != "ignored" {
		t.Errorf("status field = %q, want ignored", out.Status)
	}
	if pl.calls != 0 {
		t.Errorf("planner.Plan calls = %d, want 0 (non-allowlisted project must never plan/create a run)", pl.calls)
	}
	if fetcher.fetchCalls != 0 || fetcher.listCalls != 0 {
		t.Errorf("GitLab fetch/list calls made for a non-allowlisted project: fetchCalls=%d listCalls=%d", fetcher.fetchCalls, fetcher.listCalls)
	}
}

func TestGitLabWebhook_InvalidJSONBody_Returns400(t *testing.T) {
	pl := &fakePlanner{}
	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       &fakeGitLabFetcher{},
		Planner:      pl,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	resp := postGitLabWebhook(t, srv, testSecret, []byte("{not-json"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if pl.calls != 0 {
		t.Errorf("planner.Plan calls = %d, want 0", pl.calls)
	}
}

func TestGitLabWebhook_FetchNotFound_Returns400(t *testing.T) {
	pl := &fakePlanner{}
	deps := webhook.GitLabWebhookDeps{
		Secret:       testSecret,
		AllowProject: testProject,
		GitLab:       &fakeGitLabFetcher{fetchErr: gitlab.ErrNotFound},
		Planner:      pl,
		WorkflowPath: ".github/workflows/ci.yml",
	}
	srv := newGitLabTestServer(deps)
	defer srv.Close()

	resp := postGitLabWebhook(t, srv, testSecret, gitlabPushBody(t, testProject, "push"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if pl.calls != 0 {
		t.Errorf("planner.Plan calls = %d, want 0", pl.calls)
	}
}
