// Package gitlab is the orchestrator's read-side integration with GitLab: it
// lists and fetches workflow YAML files from a repo (a "project" in GitLab
// terms) at a given commit sha, and produces a tokenized clone URL for the
// runner.
//
// This exists for T-M2-04's MVP (Option A from T-M6-03): the repo is hosted
// on GitLab, but its workflow files are still GitHub-Actions-shaped
// (.github/workflows/*.yml) so the existing act-based planner
// (internal/planner) is reused unchanged. Native .gitlab-ci.yml parsing is
// out of scope for this package.
//
// Auth is a single Personal Access Token (PAT) sent as the PRIVATE-TOKEN
// header, read by the caller from config (DRASSI_GITLAB_TOKEN) and passed to
// New, together with the GitLab instance's base URL (DRASSI_GITLAB_BASE_URL,
// e.g. "https://gitlab.example.com" -- no "/api/v4" suffix). Requests are
// made with the standard library net/http against that instance's REST API
// (baseURL + "/api/v4") -- no external GitLab client library is used, by
// design, mirroring internal/github (T-M1-03).
//
// Per workspace CLAUDE.md, this package never hardcodes a real GitLab host:
// baseURL is always supplied by the caller from config.
package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// defaultTimeout bounds an individual HTTP request when the caller supplies
// an http.Client with no timeout of its own. Callers should still prefer
// passing a context with its own deadline; this is only a backstop.
const defaultTimeout = 30 * time.Second

// ErrNotFound is returned (wrapped) when GitLab responds 404 to a project,
// sha, or path lookup. Callers can match it with errors.Is to map to an HTTP
// 404/400.
var ErrNotFound = errors.New("gitlab: not found")

// ErrUnauthorized is returned (wrapped) when GitLab responds 401 or 403,
// distinguishing an auth/permission failure from a plain not-found.
var ErrUnauthorized = errors.New("gitlab: unauthorized")

// Client is a minimal GitLab REST API client scoped to the operations this
// package needs. It holds the instance base URL, the PAT (if any), and the
// http.Client used to make requests. Zero value is not usable; construct
// with New.
type Client struct {
	// baseURL is the GitLab instance root, e.g. "https://gitlab.example.com"
	// -- no "/api/v4" suffix and no trailing slash (New trims one if given).
	baseURL    string
	token      string
	httpClient *http.Client
}

// New builds a Client against the GitLab instance at baseURL (its root, NOT
// including "/api/v4" -- that is appended internally). token may be empty,
// in which case requests are made unauthenticated (works only for public
// projects, subject to GitLab's stricter unauthenticated rate limits). If
// httpClient is nil, a client with defaultTimeout is used.
func New(baseURL, token string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		token:      token,
		httpClient: httpClient,
	}
}

// apiBase returns this instance's API root: baseURL + "/api/v4".
func (c *Client) apiBase() string {
	return c.baseURL + "/api/v4"
}

// encodeProjectPath URL-encodes a GitLab project path
// ("group/subgroup/project") into the single percent-encoded path segment
// GitLab's API expects as ":id" (i.e. "/" becomes "%2F"), per
// https://docs.gitlab.com/ee/api/rest/#namespaced-paths.
func encodeProjectPath(project string) string {
	return url.PathEscape(project)
}

// encodeFilePath URL-encodes a repo-relative file path into the single
// percent-encoded path segment GitLab's Repository Files API expects as
// ":file_path" (i.e. "/" becomes "%2F"), the same escaping as a project
// path.
func encodeFilePath(path string) string {
	return url.PathEscape(path)
}

// newRequest builds a GET request against the API path (which must start
// with "/" and already have its query string, if any), setting the
// PRIVATE-TOKEN auth header when a token is configured.
func (c *Client) newRequest(ctx context.Context, apiPath string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiBase()+apiPath, nil)
	if err != nil {
		return nil, fmt.Errorf("gitlab: build request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("PRIVATE-TOKEN", c.token)
	}
	return req, nil
}

// do executes req and returns the raw response body on 2xx. On non-2xx it
// drains and discards the body (small, bounded reads only) and returns a
// wrapped error: ErrNotFound for 404, ErrUnauthorized for 401/403, or a
// generic status error otherwise. The token is never included in returned
// errors (it is only ever sent as a header, never embedded in req.URL).
func (c *Client) do(req *http.Request) ([]byte, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Context cancellation/timeout surfaces here; preserve it via %w so
		// errors.Is(err, context.Canceled) etc. still works for callers.
		return nil, fmt.Errorf("gitlab: request %s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20)) // 10MiB cap
	if err != nil {
		return nil, fmt.Errorf("gitlab: read response body for %s: %w", req.URL.Path, err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}

	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil, fmt.Errorf("gitlab: %s %s: %w", req.Method, req.URL.Path, ErrNotFound)
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, fmt.Errorf("gitlab: %s %s: status %d: %w", req.Method, req.URL.Path, resp.StatusCode, ErrUnauthorized)
	default:
		return nil, fmt.Errorf("gitlab: %s %s: unexpected status %d", req.Method, req.URL.Path, resp.StatusCode)
	}
}

// workflowsDir is the fixed directory ListWorkflows looks under, matching
// the GitHub-Actions-shaped workflow layout Option A (T-M6-03) assumes even
// for GitLab-hosted repos.
const workflowsDir = ".github/workflows"

// treeEntry mirrors the fields this package needs from GitLab's Repository
// Tree API JSON response entries.
type treeEntry struct {
	Name string `json:"name"`
	Type string `json:"type"` // "blob" (file) or "tree" (directory)
	Path string `json:"path"` // repo-relative, e.g. ".github/workflows/ci.yml"
}

// ListWorkflows lists workflow file paths (*.yml, *.yaml) directly under
// .github/workflows/ in project at sha, via the Repository Tree API
// (GET /projects/:id/repository/tree). Returns repo-relative paths, e.g.
// ".github/workflows/ci.yml". Returns ErrNotFound (wrapped) if the
// project/sha has no such directory.
func (c *Client) ListWorkflows(ctx context.Context, project, sha string) ([]string, error) {
	if project == "" {
		return nil, fmt.Errorf("gitlab: ListWorkflows: project must not be empty")
	}
	if sha == "" {
		return nil, fmt.Errorf("gitlab: ListWorkflows: sha must not be empty")
	}

	apiPath := fmt.Sprintf("/projects/%s/repository/tree?path=%s&ref=%s&per_page=100",
		encodeProjectPath(project), url.QueryEscape(workflowsDir), url.QueryEscape(sha))
	req, err := c.newRequest(ctx, apiPath)
	if err != nil {
		return nil, err
	}

	body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("gitlab: ListWorkflows(%s, %s): %w", project, sha, err)
	}

	var entries []treeEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("gitlab: ListWorkflows(%s, %s): decode response: %w", project, sha, err)
	}

	var paths []string
	for _, e := range entries {
		if e.Type != "blob" {
			continue
		}
		if strings.HasSuffix(e.Name, ".yml") || strings.HasSuffix(e.Name, ".yaml") {
			paths = append(paths, e.Path)
		}
	}
	return paths, nil
}

// FetchWorkflow returns the raw bytes of the workflow file at path (e.g.
// ".github/workflows/ci.yml") in project at sha, via the Repository Files
// API's raw endpoint (GET /projects/:id/repository/files/:file_path/raw),
// which returns the file content directly with no JSON/base64 envelope.
func (c *Client) FetchWorkflow(ctx context.Context, project, sha, path string) ([]byte, error) {
	if project == "" {
		return nil, fmt.Errorf("gitlab: FetchWorkflow: project must not be empty")
	}
	if sha == "" {
		return nil, fmt.Errorf("gitlab: FetchWorkflow: sha must not be empty")
	}
	if path == "" {
		return nil, fmt.Errorf("gitlab: FetchWorkflow: path must not be empty")
	}

	apiPath := fmt.Sprintf("/projects/%s/repository/files/%s/raw?ref=%s",
		encodeProjectPath(project), encodeFilePath(path), url.QueryEscape(sha))
	req, err := c.newRequest(ctx, apiPath)
	if err != nil {
		return nil, err
	}

	body, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("gitlab: FetchWorkflow(%s, %s, %s): %w", project, sha, path, err)
	}
	return body, nil
}

// host returns baseURL's host[:port] component, e.g. "gitlab.example.com",
// for building a clone URL. Falls back to the raw baseURL (unlikely to be
// hit -- New's caller is expected to pass a well-formed URL).
func (c *Client) host() string {
	u, err := url.Parse(c.baseURL)
	if err != nil || u.Host == "" {
		return c.baseURL
	}
	return u.Host
}

// CloneURL returns an HTTPS clone URL for project ("group[/subgroup]/name")
// against this client's GitLab instance, with the client's token embedded as
// an oauth2 credential, e.g.
// "https://oauth2:<token>@gitlab.example.com/group/name.git", for use by the
// runner (see T-M1-04/T-M1-08's GitHub analog). If the client has no token,
// the plain (token-less) clone URL is returned, which only works for public
// projects.
//
// SECURITY: the returned string contains a live secret. Never log it, never
// return it from a REST endpoint, never write it to inventory.yaml or any
// other committed file. Use Redact (or your own equivalent) before putting a
// clone URL string into a log line or error message.
func (c *Client) CloneURL(project string) string {
	host := c.host()
	if c.token == "" {
		return fmt.Sprintf("https://%s/%s.git", host, project)
	}
	return fmt.Sprintf("https://oauth2:%s@%s/%s.git", c.token, host, project)
}

// Redact returns cloneURL with any embedded oauth2 credential replaced by a
// fixed placeholder, safe to include in logs or error messages. Non-tokenized
// URLs (or strings that aren't a clone URL at all) are returned unchanged.
func Redact(cloneURL string) string {
	const marker = "oauth2:"
	i := strings.Index(cloneURL, marker)
	if i < 0 {
		return cloneURL
	}
	rest := cloneURL[i+len(marker):]
	at := strings.Index(rest, "@")
	if at < 0 {
		return cloneURL
	}
	return cloneURL[:i] + "oauth2:***REDACTED***" + rest[at:]
}
