package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/chuan99nd/drassi-platform/internal/planner"
)

// yamlUnmarshalForTest decodes a YAML fragment the same way act's
// yaml.v3-based model decodes on.<event> into interface{} (used to build
// realistic passesFilters fixtures without depending on act's unexported
// decodeNode helper).
func yamlUnmarshalForTest(s string, out interface{}) error {
	return yaml.Unmarshal([]byte(s), out)
}

const testSecret = "testsecret"
const testAllowRepo = "chuan99nd/drassi-demo"

// --- fakes ---

// fakeGitHubFetcher serves a fixed set of workflow files regardless of
// repo/sha, which is all these handler tests need.
type fakeGitHubFetcher struct {
	workflows map[string][]byte // path -> YAML
	listErr   error
	fetchErr  error
}

func (f *fakeGitHubFetcher) ListWorkflows(ctx context.Context, repo, sha string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	paths := make([]string, 0, len(f.workflows))
	for p := range f.workflows {
		paths = append(paths, p)
	}
	return paths, nil
}

func (f *fakeGitHubFetcher) FetchWorkflow(ctx context.Context, repo, sha, path string) ([]byte, error) {
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	return f.workflows[path], nil
}

// fakePlanner records every Plan call and returns a fresh run id each time.
type fakePlanner struct {
	calls []planner.PlanInput
	err   error
}

func (f *fakePlanner) Plan(ctx context.Context, in planner.PlanInput) (*planner.PlanResult, error) {
	f.calls = append(f.calls, in)
	if f.err != nil {
		return nil, f.err
	}
	return &planner.PlanResult{RunID: uuid.New()}, nil
}

// fakeNotifier records whether Notify was called.
type fakeNotifier struct{ notified int }

func (f *fakeNotifier) Notify() { f.notified++ }

// fakeReporter records every ReportStatus call.
type fakeReporter struct {
	calls []string // "repo|sha|state"
}

func (f *fakeReporter) ReportStatus(ctx context.Context, repo, sha, state, description string) error {
	f.calls = append(f.calls, repo+"|"+sha+"|"+state)
	return nil
}

// --- fixtures ---

const workflowMainOnly = `
name: CI
on:
  push:
    branches: [main]
jobs:
  build:
    runs-on: -self-hosted
    steps:
      - run: echo hi
`

func pushBody(t *testing.T, ref string, fork bool, fullName string) []byte {
	t.Helper()
	if fullName == "" {
		fullName = testAllowRepo
	}
	body := map[string]any{
		"ref":   ref,
		"after": "deadbeefcafebabe0000000000000000000000",
		"repository": map[string]any{
			"full_name":      fullName,
			"fork":           fork,
			"default_branch": "main",
			"clone_url":      "https://github.com/" + fullName + ".git",
		},
		"pusher": map[string]any{"name": "octocat"},
		"head_commit": map[string]any{
			"id":       "deadbeefcafebabe0000000000000000000000",
			"message":  "test commit",
			"added":    []string{},
			"removed":  []string{},
			"modified": []string{"src/main.go"},
		},
		"commits": []any{
			map[string]any{
				"id":       "deadbeefcafebabe0000000000000000000000",
				"message":  "test commit",
				"added":    []string{},
				"removed":  []string{},
				"modified": []string{"src/main.go"},
			},
		},
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal push body: %v", err)
	}
	return b
}

func sign(t *testing.T, body []byte, secret string) string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func newTestServer(deps GitHubWebhookDeps) *httptest.Server {
	mux := chi.NewRouter()
	RegisterGitHubWebhook(mux, deps)
	return httptest.NewServer(mux)
}

// --- Verify unit test ---

func TestVerify(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	secret := []byte("s3cr3t")

	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	cases := []struct {
		name   string
		sig    string
		body   []byte
		secret []byte
		want   bool
	}{
		{"valid", validSig, body, secret, true},
		{"tampered body", validSig, []byte(`{"hello":"WORLD"}`), secret, false},
		{"wrong secret", validSig, body, []byte("wrong"), false},
		{"missing prefix", strings.TrimPrefix(validSig, "sha256="), body, secret, false},
		{"empty sig", "", body, secret, false},
		{"empty secret", validSig, body, nil, false},
		{"non-hex digest", "sha256=zz", body, secret, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Verify(tc.sig, tc.body, tc.secret); got != tc.want {
				t.Errorf("Verify() = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- handler tests ---

func TestGitHubWebhookHandler_ValidPushEnqueues(t *testing.T) {
	gh := &fakeGitHubFetcher{workflows: map[string][]byte{
		".github/workflows/ci.yml": []byte(workflowMainOnly),
	}}
	pl := &fakePlanner{}
	notifier := &fakeNotifier{}
	reporter := &fakeReporter{}

	srv := newTestServer(GitHubWebhookDeps{
		Secret:       testSecret,
		AllowRepo:    testAllowRepo,
		GitHub:       gh,
		Planner:      pl,
		Notifier:     notifier,
		CommitStatus: reporter,
	})
	defer srv.Close()

	body := pushBody(t, "refs/heads/main", false, "")
	resp := doWebhookRequest(t, srv, body, sign(t, body, testSecret), "push")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(pl.calls) != 1 {
		t.Fatalf("planner.Plan calls = %d, want 1", len(pl.calls))
	}
	if pl.calls[0].EventName != "push" {
		t.Errorf("EventName = %q, want push", pl.calls[0].EventName)
	}
	if pl.calls[0].Repo != testAllowRepo {
		t.Errorf("Repo = %q, want %q", pl.calls[0].Repo, testAllowRepo)
	}
	if notifier.notified != 1 {
		t.Errorf("notifier.notified = %d, want 1", notifier.notified)
	}
	if len(reporter.calls) != 1 {
		t.Errorf("reporter.calls = %d, want 1", len(reporter.calls))
	}

	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := decoded["run_ids"]; !ok {
		t.Errorf("response missing run_ids: %v", decoded)
	}
}

func TestGitHubWebhookHandler_BadSignature(t *testing.T) {
	gh := &fakeGitHubFetcher{workflows: map[string][]byte{
		".github/workflows/ci.yml": []byte(workflowMainOnly),
	}}
	pl := &fakePlanner{}

	srv := newTestServer(GitHubWebhookDeps{
		Secret:    testSecret,
		AllowRepo: testAllowRepo,
		GitHub:    gh,
		Planner:   pl,
	})
	defer srv.Close()

	body := pushBody(t, "refs/heads/main", false, "")
	resp := doWebhookRequest(t, srv, body, "sha256=deadbeef", "push")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
	if len(pl.calls) != 0 {
		t.Fatalf("planner.Plan calls = %d, want 0", len(pl.calls))
	}
}

func TestGitHubWebhookHandler_ForkIgnored(t *testing.T) {
	gh := &fakeGitHubFetcher{workflows: map[string][]byte{
		".github/workflows/ci.yml": []byte(workflowMainOnly),
	}}
	pl := &fakePlanner{}

	srv := newTestServer(GitHubWebhookDeps{
		Secret:    testSecret,
		AllowRepo: testAllowRepo,
		GitHub:    gh,
		Planner:   pl,
	})
	defer srv.Close()

	body := pushBody(t, "refs/heads/main", true /* fork */, "")
	resp := doWebhookRequest(t, srv, body, sign(t, body, testSecret), "push")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(pl.calls) != 0 {
		t.Fatalf("planner.Plan calls = %d, want 0 (fork must not be planned)", len(pl.calls))
	}

	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := decoded["ignored"]; !ok {
		t.Errorf("response missing ignored reason: %v", decoded)
	}
}

func TestGitHubWebhookHandler_WrongRepoIgnored(t *testing.T) {
	gh := &fakeGitHubFetcher{workflows: map[string][]byte{
		".github/workflows/ci.yml": []byte(workflowMainOnly),
	}}
	pl := &fakePlanner{}

	srv := newTestServer(GitHubWebhookDeps{
		Secret:    testSecret,
		AllowRepo: testAllowRepo,
		GitHub:    gh,
		Planner:   pl,
	})
	defer srv.Close()

	body := pushBody(t, "refs/heads/main", false, "someoneelse/other-repo")
	resp := doWebhookRequest(t, srv, body, sign(t, body, testSecret), "push")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(pl.calls) != 0 {
		t.Fatalf("planner.Plan calls = %d, want 0 (wrong repo must not be planned)", len(pl.calls))
	}
}

func TestGitHubWebhookHandler_BranchExcludedBySkipped(t *testing.T) {
	gh := &fakeGitHubFetcher{workflows: map[string][]byte{
		".github/workflows/ci.yml": []byte(workflowMainOnly), // on.push.branches: [main]
	}}
	pl := &fakePlanner{}

	srv := newTestServer(GitHubWebhookDeps{
		Secret:    testSecret,
		AllowRepo: testAllowRepo,
		GitHub:    gh,
		Planner:   pl,
	})
	defer srv.Close()

	body := pushBody(t, "refs/heads/feature-x", false, "")
	resp := doWebhookRequest(t, srv, body, sign(t, body, testSecret), "push")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(pl.calls) != 0 {
		t.Fatalf("planner.Plan calls = %d, want 0 (branch excluded by on.push.branches)", len(pl.calls))
	}

	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := decoded["ignored"]; !ok {
		t.Errorf("response missing ignored reason: %v", decoded)
	}
}

func TestGitHubWebhookHandler_NonPushEventIgnored(t *testing.T) {
	gh := &fakeGitHubFetcher{workflows: map[string][]byte{
		".github/workflows/ci.yml": []byte(workflowMainOnly),
	}}
	pl := &fakePlanner{}

	srv := newTestServer(GitHubWebhookDeps{
		Secret:    testSecret,
		AllowRepo: testAllowRepo,
		GitHub:    gh,
		Planner:   pl,
	})
	defer srv.Close()

	body := []byte(`{"zen":"hi"}`)
	resp := doWebhookRequest(t, srv, body, sign(t, body, testSecret), "ping")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(pl.calls) != 0 {
		t.Fatalf("planner.Plan calls = %d, want 0", len(pl.calls))
	}
}

// --- unit tests for the T-M2-02 helper functions ---

func TestChangedFiles(t *testing.T) {
	push := PushEvent{
		HeadCommit: &GitHubCommit{
			Added:    []string{"docs/a.md"},
			Modified: []string{"src/main.go"},
			Removed:  []string{},
		},
		Commits: []GitHubCommit{
			{Added: []string{"docs/a.md"}, Modified: []string{"src/main.go", "src/util.go"}},
			{Removed: []string{"old.txt"}},
		},
	}

	got := changedFiles(push)
	want := map[string]bool{"docs/a.md": true, "src/main.go": true, "src/util.go": true, "old.txt": true}
	if len(got) != len(want) {
		t.Fatalf("changedFiles() = %v, want %d unique entries matching %v", got, len(want), want)
	}
	for _, f := range got {
		if !want[f] {
			t.Errorf("changedFiles() contains unexpected entry %q", f)
		}
	}
}

func TestShortBranch(t *testing.T) {
	cases := []struct{ ref, want string }{
		{"refs/heads/main", "main"},
		{"refs/heads/feature/x", "feature/x"},
		{"refs/tags/v1.0.0", ""},
		{"", ""},
	}
	for _, tc := range cases {
		if got := shortBranch(tc.ref); got != tc.want {
			t.Errorf("shortBranch(%q) = %q, want %q", tc.ref, got, tc.want)
		}
	}
}

func TestBuildEventJSON(t *testing.T) {
	valid := []byte(`{"ref":"refs/heads/main"}`)
	got, err := BuildEventJSON("push", valid)
	if err != nil {
		t.Fatalf("BuildEventJSON() unexpected error: %v", err)
	}
	if string(got) != string(valid) {
		t.Errorf("BuildEventJSON() = %s, want verbatim %s", got, valid)
	}

	if _, err := BuildEventJSON("push", nil); err == nil {
		t.Error("BuildEventJSON(nil) expected error, got nil")
	}
	if _, err := BuildEventJSON("push", []byte("not json")); err == nil {
		t.Error("BuildEventJSON(invalid json) expected error, got nil")
	}
}

func TestPassesFilters(t *testing.T) {
	on := func(yaml string) interface{} {
		// Decode the on.push mapping the same way act's Workflow.OnEvent
		// would hand it to passesFilters: a map[string]interface{}.
		var m map[string]interface{}
		if err := yamlUnmarshalForTest(yaml, &m); err != nil {
			t.Fatalf("decode on.push fixture: %v", err)
		}
		return m
	}

	cases := []struct {
		name       string
		on         interface{}
		branch     string
		files      []string
		wantOK     bool
		wantReason string // substring to look for when !wantOK; "" = don't check
	}{
		{
			name:   "no on config at all (bare `on: push`)",
			on:     nil,
			branch: "main",
			files:  nil,
			wantOK: true,
		},
		{
			name:   "branches match",
			on:     on("branches: [main]"),
			branch: "main",
			files:  nil,
			wantOK: true,
		},
		{
			name:       "branches no-match",
			on:         on("branches: [main]"),
			branch:     "feature-x",
			files:      nil,
			wantOK:     false,
			wantReason: "branches",
		},
		{
			name:       "branches-ignore match (excluded)",
			on:         on("branches-ignore: [main]"),
			branch:     "main",
			files:      nil,
			wantOK:     false,
			wantReason: "branches-ignore",
		},
		{
			name:   "branches-ignore no-match (allowed)",
			on:     on("branches-ignore: [main]"),
			branch: "feature-x",
			files:  nil,
			wantOK: true,
		},
		{
			name:   "paths match",
			on:     on("paths: ['src/**']"),
			branch: "main",
			files:  []string{"src/main.go"},
			wantOK: true,
		},
		{
			name:       "paths no-match",
			on:         on("paths: ['src/**']"),
			branch:     "main",
			files:      []string{"docs/readme.md"},
			wantOK:     false,
			wantReason: "paths",
		},
		{
			name:       "paths-ignore all-match (excluded)",
			on:         on("paths-ignore: ['docs/**']"),
			branch:     "main",
			files:      []string{"docs/readme.md"},
			wantOK:     false,
			wantReason: "paths-ignore",
		},
		{
			name:   "paths-ignore not all-match (allowed)",
			on:     on("paths-ignore: ['docs/**']"),
			branch: "main",
			files:  []string{"docs/readme.md", "src/main.go"},
			wantOK: true,
		},
		{
			name:       "branches filter present but ref is a tag",
			on:         on("branches: [main]"),
			branch:     "", // shortBranch() returns "" for a tag ref
			files:      nil,
			wantOK:     false,
			wantReason: "not a branch",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := passesFilters(tc.on, tc.branch, tc.files)
			if ok != tc.wantOK {
				t.Fatalf("passesFilters() ok = %v, reason = %q, want ok = %v", ok, reason, tc.wantOK)
			}
			if !ok && tc.wantReason != "" && !strings.Contains(reason, tc.wantReason) {
				t.Errorf("passesFilters() reason = %q, want substring %q", reason, tc.wantReason)
			}
		})
	}
}

// --- test helpers ---

func doWebhookRequest(t *testing.T, srv *httptest.Server, body []byte, sig, event string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/webhooks/github", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", "test-delivery-id")
	if sig != "" {
		req.Header.Set("X-Hub-Signature-256", sig)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}
