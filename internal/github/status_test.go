package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newTestClient builds a *Client whose base URL points at srv (an
// httptest server standing in for api.github.com), carrying token so
// ReportStatus's auth header can be asserted against it.
func newTestClient(srv *httptest.Server, token string) *Client {
	c := New(token, srv.Client())
	c.baseURL = srv.URL
	return c
}

func TestReportStatus_PostsExpectedPathBodyAndAuthHeader(t *testing.T) {
	cases := []struct {
		state string
	}{
		{"pending"},
		{"success"},
		{"failure"},
		{"error"},
	}

	for _, tc := range cases {
		t.Run(tc.state, func(t *testing.T) {
			var (
				gotMethod string
				gotPath   string
				gotAuth   string
				gotBody   commitStatusRequestBody
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				gotPath = r.URL.Path
				gotAuth = r.Header.Get("Authorization")
				if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
					t.Errorf("decode request body: %v", err)
				}
				w.WriteHeader(http.StatusCreated)
			}))
			defer srv.Close()

			c := newTestClient(srv, "test-token-abc")
			r := NewCommitStatusReporter(c, "drassi")

			err := r.ReportStatus(context.Background(), "owner/repo", "deadbeefcafe", tc.state, "a description")
			if err != nil {
				t.Fatalf("ReportStatus: unexpected error: %v", err)
			}

			if gotMethod != http.MethodPost {
				t.Errorf("method = %q, want POST", gotMethod)
			}
			wantPath := "/repos/owner/repo/statuses/deadbeefcafe"
			if gotPath != wantPath {
				t.Errorf("path = %q, want %q", gotPath, wantPath)
			}
			if gotAuth != "Bearer test-token-abc" {
				t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer test-token-abc")
			}
			if gotBody.State != tc.state {
				t.Errorf("body.State = %q, want %q", gotBody.State, tc.state)
			}
			if gotBody.Context != "drassi" {
				t.Errorf("body.Context = %q, want %q", gotBody.Context, "drassi")
			}
			if gotBody.Description != "a description" {
				t.Errorf("body.Description = %q, want %q", gotBody.Description, "a description")
			}
		})
	}
}

func TestReportStatus_DefaultContext(t *testing.T) {
	var gotBody commitStatusRequestBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := newTestClient(srv, "tok")
	r := NewCommitStatusReporter(c, "") // empty -> DefaultStatusContext

	if err := r.ReportStatus(context.Background(), "o/r", "sha1", "pending", "queued"); err != nil {
		t.Fatalf("ReportStatus: %v", err)
	}
	if gotBody.Context != DefaultStatusContext {
		t.Errorf("body.Context = %q, want default %q", gotBody.Context, DefaultStatusContext)
	}
}

func TestReportStatus_SameContextAcrossPendingAndTerminal(t *testing.T) {
	// The context string must be identical across a check's pending and
	// terminal posts for the same commit, so GitHub coalesces them into one
	// transitioning row (T-M2-03).
	var contexts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b commitStatusRequestBody
		_ = json.NewDecoder(r.Body).Decode(&b)
		contexts = append(contexts, b.Context)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := newTestClient(srv, "tok")
	r := NewCommitStatusReporter(c, "drassi")

	if err := r.ReportStatus(context.Background(), "o/r", "sha1", "pending", "Run queued"); err != nil {
		t.Fatalf("pending post: %v", err)
	}
	if err := r.ReportStatus(context.Background(), "o/r", "sha1", "success", "Run succeeded"); err != nil {
		t.Fatalf("terminal post: %v", err)
	}

	if len(contexts) != 2 || contexts[0] != contexts[1] {
		t.Fatalf("expected identical context across pending/terminal posts, got %+v", contexts)
	}
}

func TestReportStatus_NonSuccessStatusReturnsWrappedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"boom"}`))
	}))
	defer srv.Close()

	c := newTestClient(srv, "test-token-xyz")
	r := NewCommitStatusReporter(c, "drassi")

	err := r.ReportStatus(context.Background(), "owner/repo", "sha1", "success", "desc")
	if err == nil {
		t.Fatal("ReportStatus: expected error on non-2xx response, got nil")
	}
	if strings.Contains(err.Error(), "test-token-xyz") {
		t.Fatalf("ReportStatus error must not contain the token: %v", err)
	}
}

func TestReportStatus_NotFoundIsWrappedErrNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := newTestClient(srv, "tok")
	r := NewCommitStatusReporter(c, "drassi")

	err := r.ReportStatus(context.Background(), "owner/repo", "sha1", "failure", "desc")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReportStatus: err = %v, want errors.Is(err, ErrNotFound)", err)
	}
}

func TestReportStatus_InvalidStateRejectedWithoutNetworkCall(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := newTestClient(srv, "tok")
	r := NewCommitStatusReporter(c, "drassi")

	err := r.ReportStatus(context.Background(), "owner/repo", "sha1", "bogus-state", "desc")
	if err == nil {
		t.Fatal("ReportStatus with invalid state: expected error, got nil")
	}
	if called {
		t.Fatal("ReportStatus with invalid state: must not make a network call")
	}
}

func TestReportStatus_InvalidRepoOrEmptySHARejected(t *testing.T) {
	c := New("tok", nil)
	r := NewCommitStatusReporter(c, "drassi")

	if err := r.ReportStatus(context.Background(), "not-a-valid-repo", "sha1", "success", "d"); err == nil {
		t.Fatal("ReportStatus with invalid repo: expected error, got nil")
	}
	if err := r.ReportStatus(context.Background(), "owner/repo", "", "success", "d"); err == nil {
		t.Fatal("ReportStatus with empty sha: expected error, got nil")
	}
}

func TestReportStatus_DescriptionTruncatedTo140Runes(t *testing.T) {
	var gotBody commitStatusRequestBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := newTestClient(srv, "tok")
	r := NewCommitStatusReporter(c, "drassi")

	long := strings.Repeat("x", 500)
	if err := r.ReportStatus(context.Background(), "o/r", "sha1", "success", long); err != nil {
		t.Fatalf("ReportStatus: %v", err)
	}
	if len(gotBody.Description) != maxStatusDescriptionRunes {
		t.Fatalf("Description length = %d, want %d (truncated)", len(gotBody.Description), maxStatusDescriptionRunes)
	}
}

func TestReportStatus_NoTokenNoAuthHeader(t *testing.T) {
	var gotAuth string
	seenAuth := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		seenAuth = gotAuth != ""
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	c := newTestClient(srv, "") // no token configured
	r := NewCommitStatusReporter(c, "drassi")

	if err := r.ReportStatus(context.Background(), "o/r", "sha1", "pending", "d"); err != nil {
		t.Fatalf("ReportStatus: %v", err)
	}
	if seenAuth {
		t.Fatalf("expected no Authorization header when client has no token, got %q", gotAuth)
	}
}
