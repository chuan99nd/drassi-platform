package gitlab

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- unit tests: URL building, headers, redaction (no real GitLab needed) -

func TestNew_TrimsTrailingSlash(t *testing.T) {
	c := New("https://gitlab.example.com/", "tok", nil)
	if c.baseURL != "https://gitlab.example.com" {
		t.Errorf("baseURL = %q, want trailing slash trimmed", c.baseURL)
	}
	if got, want := c.apiBase(), "https://gitlab.example.com/api/v4"; got != want {
		t.Errorf("apiBase() = %q, want %q", got, want)
	}
}

func TestEncodeProjectPath(t *testing.T) {
	cases := map[string]string{
		"group/project":          "group%2Fproject",
		"group/subgroup/project": "group%2Fsubgroup%2Fproject",
		"single":                 "single",
	}
	for in, want := range cases {
		if got := encodeProjectPath(in); got != want {
			t.Errorf("encodeProjectPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEncodeFilePath(t *testing.T) {
	in := ".github/workflows/ci.yml"
	want := ".github%2Fworkflows%2Fci.yml"
	if got := encodeFilePath(in); got != want {
		t.Errorf("encodeFilePath(%q) = %q, want %q", in, got, want)
	}
}

func TestNewRequest_URLBuildingAndAuthHeader(t *testing.T) {
	c := New("https://gitlab.example.com", "test-token-123", nil)

	req, err := c.newRequest(context.Background(), "/projects/group%2Fname/repository/files/.github%2Fworkflows%2Fci.yml/raw?ref=main")
	if err != nil {
		t.Fatalf("newRequest: %v", err)
	}
	wantURL := "https://gitlab.example.com/api/v4/projects/group%2Fname/repository/files/.github%2Fworkflows%2Fci.yml/raw?ref=main"
	if req.URL.String() != wantURL {
		t.Errorf("URL = %q, want %q", req.URL.String(), wantURL)
	}
	if got := req.Header.Get("PRIVATE-TOKEN"); got != "test-token-123" {
		t.Errorf("PRIVATE-TOKEN header = %q, want test-token-123", got)
	}
}

func TestNewRequest_NoAuthHeaderWhenTokenEmpty(t *testing.T) {
	c := New("https://gitlab.example.com", "", nil)

	req, err := c.newRequest(context.Background(), "/projects/group%2Fname/repository/tree")
	if err != nil {
		t.Fatalf("newRequest: %v", err)
	}
	if got := req.Header.Get("PRIVATE-TOKEN"); got != "" {
		t.Errorf("PRIVATE-TOKEN header = %q, want empty (no token configured)", got)
	}
}

func TestCloneURL(t *testing.T) {
	c := New("https://gitlab.example.com", "shhh-secret-token", nil)
	got := c.CloneURL("group/name")
	want := "https://oauth2:shhh-secret-token@gitlab.example.com/group/name.git"
	if got != want {
		t.Errorf("CloneURL = %q, want %q", got, want)
	}
}

func TestCloneURL_NoToken(t *testing.T) {
	c := New("https://gitlab.example.com", "", nil)
	got := c.CloneURL("group/name")
	want := "https://gitlab.example.com/group/name.git"
	if got != want {
		t.Errorf("CloneURL (no token) = %q, want %q", got, want)
	}
}

func TestRedact(t *testing.T) {
	cloneURL := New("https://gitlab.example.com", "shhh-secret-token", nil).CloneURL("group/name")
	redacted := Redact(cloneURL)

	if strings.Contains(redacted, "shhh-secret-token") {
		t.Fatalf("Redact(%q) = %q still contains the raw token", cloneURL, redacted)
	}
	want := "https://oauth2:***REDACTED***@gitlab.example.com/group/name.git"
	if redacted != want {
		t.Errorf("Redact = %q, want %q", redacted, want)
	}

	// A URL with no embedded token passes through unchanged.
	plain := "https://gitlab.example.com/group/name.git"
	if got := Redact(plain); got != plain {
		t.Errorf("Redact(%q) = %q, want unchanged", plain, got)
	}
}

func TestFetchWorkflow_EmptyArgsRejected(t *testing.T) {
	c := New("https://gitlab.example.com", "", nil)
	if _, err := c.FetchWorkflow(context.Background(), "", "sha", "path"); err == nil {
		t.Error("FetchWorkflow with empty project: expected error, got nil")
	}
	if _, err := c.FetchWorkflow(context.Background(), "group/name", "", "path"); err == nil {
		t.Error("FetchWorkflow with empty sha: expected error, got nil")
	}
	if _, err := c.FetchWorkflow(context.Background(), "group/name", "sha", ""); err == nil {
		t.Error("FetchWorkflow with empty path: expected error, got nil")
	}
}

func TestListWorkflows_EmptyArgsRejected(t *testing.T) {
	c := New("https://gitlab.example.com", "", nil)
	if _, err := c.ListWorkflows(context.Background(), "", "sha"); err == nil {
		t.Error("ListWorkflows with empty project: expected error, got nil")
	}
	if _, err := c.ListWorkflows(context.Background(), "group/name", ""); err == nil {
		t.Error("ListWorkflows with empty sha: expected error, got nil")
	}
}

// --- tests against a local httptest server (no real GitLab instance needed)

func TestFetchWorkflow_AgainstLocalServer(t *testing.T) {
	const wantBody = "on: push\njobs:\n  build:\n    runs-on: -self-hosted\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := "/api/v4/projects/group%2Fname/repository/files/.github%2Fworkflows%2Fci.yml/raw"
		if got := r.URL.EscapedPath(); got != wantPath {
			t.Errorf("request path = %q, want %q", got, wantPath)
		}
		if got := r.URL.Query().Get("ref"); got != "deadbeef" {
			t.Errorf("ref query param = %q, want deadbeef", got)
		}
		if got := r.Header.Get("PRIVATE-TOKEN"); got != "tok-abc" {
			t.Errorf("PRIVATE-TOKEN header = %q, want tok-abc", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(wantBody))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok-abc", nil)
	body, err := c.FetchWorkflow(context.Background(), "group/name", "deadbeef", ".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("FetchWorkflow: %v", err)
	}
	if string(body) != wantBody {
		t.Errorf("FetchWorkflow body = %q, want %q", body, wantBody)
	}
}

func TestFetchWorkflow_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", nil)
	_, err := c.FetchWorkflow(context.Background(), "group/name", "deadbeef", ".github/workflows/missing.yml")
	if err == nil {
		t.Fatal("FetchWorkflow against 404: expected error, got nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("FetchWorkflow against 404: err = %v, want errors.Is(err, ErrNotFound)", err)
	}
}

func TestFetchWorkflow_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := New(srv.URL, "wrong-tok", nil)
	_, err := c.FetchWorkflow(context.Background(), "group/name", "deadbeef", ".github/workflows/ci.yml")
	if err == nil {
		t.Fatal("FetchWorkflow against 401: expected error, got nil")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("FetchWorkflow against 401: err = %v, want errors.Is(err, ErrUnauthorized)", err)
	}
}

func TestListWorkflows_AgainstLocalServer(t *testing.T) {
	const treeJSON = `[
		{"id":"a1","name":"ci.yml","type":"blob","path":".github/workflows/ci.yml"},
		{"id":"a2","name":"release.yaml","type":"blob","path":".github/workflows/release.yaml"},
		{"id":"a3","name":"notes.md","type":"blob","path":".github/workflows/notes.md"},
		{"id":"a4","name":"nested","type":"tree","path":".github/workflows/nested"}
	]`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wantPath := "/api/v4/projects/group%2Fname/repository/tree"
		if got := r.URL.EscapedPath(); got != wantPath {
			t.Errorf("request path = %q, want %q", got, wantPath)
		}
		if got := r.URL.Query().Get("path"); got != workflowsDir {
			t.Errorf("path query param = %q, want %q", got, workflowsDir)
		}
		if got := r.URL.Query().Get("ref"); got != "deadbeef" {
			t.Errorf("ref query param = %q, want deadbeef", got)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(treeJSON))
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", nil)
	paths, err := c.ListWorkflows(context.Background(), "group/name", "deadbeef")
	if err != nil {
		t.Fatalf("ListWorkflows: %v", err)
	}
	want := []string{".github/workflows/ci.yml", ".github/workflows/release.yaml"}
	if len(paths) != len(want) {
		t.Fatalf("ListWorkflows = %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Errorf("ListWorkflows[%d] = %q, want %q", i, paths[i], want[i])
		}
	}
}

func TestContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled

	_, err := c.FetchWorkflow(ctx, "group/name", "deadbeef", ".github/workflows/ci.yml")
	if err == nil {
		t.Fatal("FetchWorkflow with canceled context: expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("FetchWorkflow with canceled context: err = %v, want errors.Is(err, context.Canceled)", err)
	}
}
