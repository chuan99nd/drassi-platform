package github

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

// skipIfOffline does a quick, short-timeout reachability check against the
// real GitHub API and skips the calling test if it's unreachable, so this
// package's live tests degrade gracefully in a sandboxed/offline CI run
// instead of failing.
func skipIfOffline(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase, nil)
	if err != nil {
		t.Fatalf("build reachability request: %v", err)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Skipf("skipping live GitHub test: api.github.com unreachable: %v", err)
	}
	resp.Body.Close()
}

var shaRE = regexp.MustCompile(`^[0-9a-f]{40}$`)

// --- unit tests: no network required -------------------------------------

func TestSplitRepo(t *testing.T) {
	cases := []struct {
		repo      string
		wantOwner string
		wantName  string
		wantErr   bool
	}{
		{"owner/name", "owner", "name", false},
		{"nektos/act", "nektos", "act", false},
		{"", "", "", true},
		{"noSlash", "", "", true},
		{"too/many/slashes", "", "", true},
		{"/name", "", "", true},
		{"owner/", "", "", true},
	}
	for _, tc := range cases {
		owner, name, err := splitRepo(tc.repo)
		if tc.wantErr {
			if err == nil {
				t.Errorf("splitRepo(%q): expected error, got owner=%q name=%q", tc.repo, owner, name)
			}
			continue
		}
		if err != nil {
			t.Errorf("splitRepo(%q): unexpected error: %v", tc.repo, err)
			continue
		}
		if owner != tc.wantOwner || name != tc.wantName {
			t.Errorf("splitRepo(%q) = (%q, %q), want (%q, %q)", tc.repo, owner, name, tc.wantOwner, tc.wantName)
		}
	}
}

func TestNewRequest_URLBuildingAndHeaders(t *testing.T) {
	c := New("test-token-123", nil)

	req, err := c.newRequest(context.Background(), http.MethodGet, "/repos/owner/name/commits/main", "application/vnd.github.sha")
	if err != nil {
		t.Fatalf("newRequest: %v", err)
	}
	wantURL := apiBase + "/repos/owner/name/commits/main"
	if req.URL.String() != wantURL {
		t.Errorf("URL = %q, want %q", req.URL.String(), wantURL)
	}
	if got := req.Header.Get("Accept"); got != "application/vnd.github.sha" {
		t.Errorf("Accept header = %q, want application/vnd.github.sha", got)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer test-token-123" {
		t.Errorf("Authorization header = %q, want Bearer test-token-123", got)
	}
	if got := req.Header.Get("X-GitHub-Api-Version"); got != apiVersion {
		t.Errorf("X-GitHub-Api-Version = %q, want %q", got, apiVersion)
	}
}

func TestNewRequest_DefaultAcceptAndNoAuthWhenTokenEmpty(t *testing.T) {
	c := New("", nil)

	req, err := c.newRequest(context.Background(), http.MethodGet, "/repos/owner/name/contents/.github/workflows", "")
	if err != nil {
		t.Fatalf("newRequest: %v", err)
	}
	if got := req.Header.Get("Accept"); got != "application/vnd.github+json" {
		t.Errorf("Accept header = %q, want default application/vnd.github+json", got)
	}
	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("Authorization header = %q, want empty (no token configured)", got)
	}
}

func TestCloneURL(t *testing.T) {
	c := New("shhh-secret-token", nil)
	got := c.CloneURL("owner/name")
	want := "https://x-access-token:shhh-secret-token@github.com/owner/name.git"
	if got != want {
		t.Errorf("CloneURL = %q, want %q", got, want)
	}
}

func TestCloneURL_NoToken(t *testing.T) {
	c := New("", nil)
	got := c.CloneURL("owner/name")
	want := "https://github.com/owner/name.git"
	if got != want {
		t.Errorf("CloneURL (no token) = %q, want %q", got, want)
	}
}

func TestRedact(t *testing.T) {
	cloneURL := New("shhh-secret-token", nil).CloneURL("owner/name")
	redacted := Redact(cloneURL)

	if strings.Contains(redacted, "shhh-secret-token") {
		t.Fatalf("Redact(%q) = %q still contains the raw token", cloneURL, redacted)
	}
	want := "https://x-access-token:***REDACTED***@github.com/owner/name.git"
	if redacted != want {
		t.Errorf("Redact = %q, want %q", redacted, want)
	}

	// A URL with no embedded token passes through unchanged.
	plain := "https://github.com/owner/name.git"
	if got := Redact(plain); got != plain {
		t.Errorf("Redact(%q) = %q, want unchanged", plain, got)
	}
}

func TestResolveRef_EmptyRefRejected(t *testing.T) {
	c := New("", nil)
	if _, err := c.ResolveRef(context.Background(), "owner/name", ""); err == nil {
		t.Fatal("ResolveRef with empty ref: expected error, got nil")
	}
}

func TestResolveRef_InvalidRepoRejected(t *testing.T) {
	c := New("", nil)
	if _, err := c.ResolveRef(context.Background(), "not-a-valid-repo", "main"); err == nil {
		t.Fatal("ResolveRef with invalid repo: expected error, got nil")
	}
}

// --- live tests: hit the real GitHub API, skip gracefully if unreachable --

const (
	liveRepo         = "actions/checkout" // public, has .github/workflows, stable
	liveRef          = "main"
	liveNonexistRepo = "chuan99nd-does-not-exist-xyz/also-does-not-exist-xyz"
)

func TestLive_ResolveRef_PublicRepo(t *testing.T) {
	skipIfOffline(t)
	c := New("", nil) // token-less: public repo, rate-limited but sufficient for one call

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sha, err := c.ResolveRef(ctx, liveRepo, liveRef)
	if err != nil {
		t.Fatalf("ResolveRef(%s, %s): %v", liveRepo, liveRef, err)
	}
	if !shaRE.MatchString(sha) {
		t.Fatalf("ResolveRef(%s, %s) = %q, want 40-hex sha", liveRepo, liveRef, sha)
	}
}

func TestLive_ListWorkflows_PublicRepo(t *testing.T) {
	skipIfOffline(t)
	c := New("", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sha, err := c.ResolveRef(ctx, liveRepo, liveRef)
	if err != nil {
		t.Fatalf("ResolveRef(%s, %s): %v", liveRepo, liveRef, err)
	}

	paths, err := c.ListWorkflows(ctx, liveRepo, sha)
	if err != nil {
		t.Fatalf("ListWorkflows(%s, %s): %v", liveRepo, sha, err)
	}
	if len(paths) == 0 {
		t.Fatalf("ListWorkflows(%s, %s): got no workflow paths, want >= 1", liveRepo, sha)
	}
	for _, p := range paths {
		if !strings.HasPrefix(p, ".github/workflows/") {
			t.Errorf("workflow path %q does not start with .github/workflows/", p)
		}
		if !strings.HasSuffix(p, ".yml") && !strings.HasSuffix(p, ".yaml") {
			t.Errorf("workflow path %q does not end in .yml/.yaml", p)
		}
	}
}

func TestLive_FetchWorkflow_PublicRepo(t *testing.T) {
	skipIfOffline(t)
	c := New("", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sha, err := c.ResolveRef(ctx, liveRepo, liveRef)
	if err != nil {
		t.Fatalf("ResolveRef(%s, %s): %v", liveRepo, liveRef, err)
	}
	paths, err := c.ListWorkflows(ctx, liveRepo, sha)
	if err != nil {
		t.Fatalf("ListWorkflows(%s, %s): %v", liveRepo, sha, err)
	}
	if len(paths) == 0 {
		t.Fatalf("ListWorkflows(%s, %s): no paths to fetch", liveRepo, sha)
	}

	body, err := c.FetchWorkflow(ctx, liveRepo, sha, paths[0])
	if err != nil {
		t.Fatalf("FetchWorkflow(%s, %s, %s): %v", liveRepo, sha, paths[0], err)
	}
	if len(body) == 0 {
		t.Fatalf("FetchWorkflow(%s, %s, %s): empty body", liveRepo, sha, paths[0])
	}
	text := string(body)
	if !strings.Contains(text, "on") || !strings.Contains(text, "jobs") {
		t.Errorf("FetchWorkflow(%s, %s, %s): body doesn't look like a workflow YAML (missing on/jobs):\n%s", liveRepo, sha, paths[0], text)
	}
}

func TestLive_ResolveRef_UnknownRepoIsNotFound(t *testing.T) {
	skipIfOffline(t)
	c := New("", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := c.ResolveRef(ctx, liveNonexistRepo, "main")
	if err == nil {
		t.Fatalf("ResolveRef(%s): expected error, got nil", liveNonexistRepo)
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResolveRef(%s): err = %v, want errors.Is(err, ErrNotFound)", liveNonexistRepo, err)
	}
}

func TestLive_FetchWorkflow_UnknownPathIsNotFound(t *testing.T) {
	skipIfOffline(t)
	c := New("", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sha, err := c.ResolveRef(ctx, liveRepo, liveRef)
	if err != nil {
		t.Fatalf("ResolveRef(%s, %s): %v", liveRepo, liveRef, err)
	}

	_, err = c.FetchWorkflow(ctx, liveRepo, sha, ".github/workflows/this-file-does-not-exist.yml")
	if err == nil {
		t.Fatal("FetchWorkflow with unknown path: expected error, got nil")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("FetchWorkflow with unknown path: err = %v, want errors.Is(err, ErrNotFound)", err)
	}
}

func TestLive_ContextCancellation(t *testing.T) {
	skipIfOffline(t)
	c := New("", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled

	_, err := c.ResolveRef(ctx, liveRepo, liveRef)
	if err == nil {
		t.Fatal("ResolveRef with canceled context: expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveRef with canceled context: err = %v, want errors.Is(err, context.Canceled)", err)
	}
}
