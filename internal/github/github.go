// Package github is the orchestrator's read-side integration with GitHub: it
// resolves a ref to a commit sha, lists and fetches workflow YAML files from
// a repo at a given sha, and produces a tokenized clone URL for the runner.
//
// Auth is a single Personal Access Token (PAT), read by the caller from
// config (DRASSI_GITHUB_TOKEN, falling back to GITHUB_TOKEN) and passed to
// New. Requests are made with the standard library net/http against the
// GitHub REST API (api.github.com) — no external GitHub client library is
// used, by design (see T-M1-03).
//
// Commit-status/checks reporting, webhook verification, and GitHub App/OIDC
// auth are out of scope for this package (see T-M2-01, T-M2-03).
package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// apiBase is the GitHub REST API root. Overridable in tests via
// Client.baseURL.
const apiBase = "https://api.github.com"

// apiVersion is sent as the X-GitHub-Api-Version header, pinning us to a
// stable REST API surface.
const apiVersion = "2022-11-28"

// defaultTimeout bounds an individual HTTP request when the caller supplies
// an http.Client with no timeout of its own. Callers should still prefer
// passing a context with its own deadline; this is only a backstop.
const defaultTimeout = 30 * time.Second

// ErrNotFound is returned (wrapped) when GitHub responds 404 to a ref, repo,
// or path lookup. Callers (e.g. the REST layer, T-M1-05) can match it with
// errors.Is to map to an HTTP 404.
var ErrNotFound = errors.New("github: not found")

// ErrUnauthorized is returned (wrapped) when GitHub responds 401 or 403,
// distinguishing an auth/permission failure from a plain not-found.
var ErrUnauthorized = errors.New("github: unauthorized")

// Client is a minimal GitHub REST API client scoped to the operations this
// package needs. It holds the PAT (if any) and the http.Client used to make
// requests. Zero value is not usable; construct with New.
type Client struct {
	token      string
	httpClient *http.Client
	// baseURL overrides apiBase; used only by tests.
	baseURL string
}

// New builds a Client. token may be empty, in which case requests are made
// unauthenticated (works for public repos, subject to GitHub's stricter
// unauthenticated rate limits). If httpClient is nil, a client with
// defaultTimeout is used.
func New(token string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return &Client{token: token, httpClient: httpClient}
}

func (c *Client) base() string {
	if c.baseURL != "" {
		return c.baseURL
	}
	return apiBase
}

// splitRepo validates and splits a "owner/name" repo string.
func splitRepo(repo string) (owner, name string, err error) {
	parts := strings.SplitN(repo, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("github: invalid repo %q, want \"owner/name\"", repo)
	}
	if strings.Contains(parts[1], "/") {
		return "", "", fmt.Errorf("github: invalid repo %q, want \"owner/name\"", repo)
	}
	return parts[0], parts[1], nil
}

// newRequest builds an API request against path (which must start with "/"),
// setting auth, API version, and Accept headers. accept overrides the
// default "application/vnd.github+json" media type when non-empty.
func (c *Client) newRequest(ctx context.Context, method, path, accept string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base()+path, nil)
	if err != nil {
		return nil, fmt.Errorf("github: build request: %w", err)
	}
	if accept == "" {
		accept = "application/vnd.github+json"
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

// do executes req and returns the raw response body on 2xx. On non-2xx it
// drains and discards the body (small, bounded reads only) and returns a
// wrapped error: ErrNotFound for 404, ErrUnauthorized for 401/403, or a
// generic status error otherwise. The token is never included in returned
// errors.
func (c *Client) do(req *http.Request) ([]byte, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Context cancellation/timeout surfaces here; preserve it via %w so
		// errors.Is(err, context.Canceled) etc. still works for callers.
		return nil, fmt.Errorf("github: request %s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // 10MiB cap
	if err != nil {
		return nil, fmt.Errorf("github: read response body for %s: %w", req.URL.Path, err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}

	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil, fmt.Errorf("github: %s %s: %w", req.Method, req.URL.Path, ErrNotFound)
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, fmt.Errorf("github: %s %s: status %d: %w", req.Method, req.URL.Path, resp.StatusCode, ErrUnauthorized)
	default:
		return nil, fmt.Errorf("github: %s %s: unexpected status %d", req.Method, req.URL.Path, resp.StatusCode)
	}
}

// ResolveRef resolves a branch, tag, or (already-full) sha to a full 40-hex
// commit sha. repo is "owner/name". Uses
// GET /repos/{owner}/{repo}/commits/{ref} with the sha media type so the
// response body is exactly the sha, no JSON parsing required.
func (c *Client) ResolveRef(ctx context.Context, repo, ref string) (string, error) {
	owner, name, err := splitRepo(repo)
	if err != nil {
		return "", err
	}
	if ref == "" {
		return "", fmt.Errorf("github: ResolveRef: ref must not be empty")
	}

	path := fmt.Sprintf("/repos/%s/%s/commits/%s", owner, name, ref)
	req, err := c.newRequest(ctx, http.MethodGet, path, "application/vnd.github.sha")
	if err != nil {
		return "", err
	}

	body, err := c.do(req)
	if err != nil {
		return "", fmt.Errorf("github: ResolveRef(%s, %s): %w", repo, ref, err)
	}

	sha := strings.TrimSpace(string(body))
	if sha == "" {
		return "", fmt.Errorf("github: ResolveRef(%s, %s): empty sha in response", repo, ref)
	}
	return sha, nil
}

// contentsEntry mirrors the fields we need from the GitHub "contents API"
// JSON response, for both a single file and a directory listing entry.
type contentsEntry struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Type     string `json:"type"` // "file" or "dir"
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

// ListWorkflows lists workflow file paths (*.yml, *.yaml) directly under
// .github/workflows/ in repo at sha, via the contents API. Returns
// repo-relative paths, e.g. ".github/workflows/ci.yml". Returns
// ErrNotFound (wrapped) if the repo/sha has no .github/workflows directory.
func (c *Client) ListWorkflows(ctx context.Context, repo, sha string) ([]string, error) {
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	if sha == "" {
		return nil, fmt.Errorf("github: ListWorkflows: sha must not be empty")
	}

	path := fmt.Sprintf("/repos/%s/%s/contents/.github/workflows?ref=%s", owner, name, sha)
	req, err := c.newRequest(ctx, http.MethodGet, path, "")
	if err != nil {
		return nil, err
	}

	body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("github: ListWorkflows(%s, %s): %w", repo, sha, err)
	}

	var entries []contentsEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("github: ListWorkflows(%s, %s): decode response: %w", repo, sha, err)
	}

	var paths []string
	for _, e := range entries {
		if e.Type != "file" {
			continue
		}
		if strings.HasSuffix(e.Name, ".yml") || strings.HasSuffix(e.Name, ".yaml") {
			paths = append(paths, e.Path)
		}
	}
	return paths, nil
}

// FetchWorkflow returns the raw bytes of the workflow file at path (e.g.
// ".github/workflows/ci.yml") in repo at sha, via the contents API. Prefers
// the raw media type to avoid a base64 round trip; if GitHub still returns
// the JSON form (e.g. a proxy/cache stripping Accept), falls back to
// base64-decoding the `content` field, honoring `encoding`.
func (c *Client) FetchWorkflow(ctx context.Context, repo, sha, path string) ([]byte, error) {
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	if sha == "" {
		return nil, fmt.Errorf("github: FetchWorkflow: sha must not be empty")
	}
	if path == "" {
		return nil, fmt.Errorf("github: FetchWorkflow: path must not be empty")
	}

	apiPath := fmt.Sprintf("/repos/%s/%s/contents/%s?ref=%s", owner, name, path, sha)
	req, err := c.newRequest(ctx, http.MethodGet, apiPath, "application/vnd.github.raw")
	if err != nil {
		return nil, err
	}

	body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("github: FetchWorkflow(%s, %s, %s): %w", repo, sha, path, err)
	}

	// The raw media type returns the file bytes directly. If GitHub (or an
	// intermediary) ignored Accept and returned the JSON contents object
	// instead, detect and decode that form.
	var entry contentsEntry
	if json.Unmarshal(body, &entry) == nil && entry.Content != "" {
		if entry.Encoding != "" && entry.Encoding != "base64" {
			return nil, fmt.Errorf("github: FetchWorkflow(%s, %s, %s): unsupported encoding %q", repo, sha, path, entry.Encoding)
		}
		// GitHub's base64 content is chunked with embedded newlines.
		clean := strings.ReplaceAll(entry.Content, "\n", "")
		decoded, err := base64.StdEncoding.DecodeString(clean)
		if err != nil {
			return nil, fmt.Errorf("github: FetchWorkflow(%s, %s, %s): decode base64 content: %w", repo, sha, path, err)
		}
		return decoded, nil
	}

	return body, nil
}

// CloneURL returns an HTTPS clone URL for repo ("owner/name") with the
// client's token embedded as an x-access-token credential, e.g.
// "https://x-access-token:<token>@github.com/owner/name.git", for use by the
// runner (see T-M1-04, T-M1-08). If the client has no token, the plain
// (token-less) clone URL is returned, which only works for public repos.
//
// SECURITY: the returned string contains a live secret. Never log it, never
// return it from a REST endpoint, never write it to inventory.yaml or any
// other committed file. Use Redact (or your own equivalent) before putting a
// clone URL string into a log line or error message.
func (c *Client) CloneURL(repo string) string {
	if c.token == "" {
		return fmt.Sprintf("https://github.com/%s.git", repo)
	}
	return fmt.Sprintf("https://x-access-token:%s@github.com/%s.git", c.token, repo)
}

// Redact returns cloneURL with any embedded x-access-token credential
// replaced by a fixed placeholder, safe to include in logs or error
// messages. Non-tokenized URLs (or strings that aren't a clone URL at all)
// are returned unchanged.
func Redact(cloneURL string) string {
	const marker = "x-access-token:"
	i := strings.Index(cloneURL, marker)
	if i < 0 {
		return cloneURL
	}
	rest := cloneURL[i+len(marker):]
	at := strings.Index(rest, "@")
	if at < 0 {
		return cloneURL
	}
	return cloneURL[:i] + "x-access-token:***REDACTED***" + rest[at:]
}
