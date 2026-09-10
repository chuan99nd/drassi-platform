// Commit status reporting (T-M2-03): posts a run's outcome back to GitHub
// via the Statuses API so a webhook-triggered commit shows a check that
// transitions pending -> success/failure. This file only ADDS to the
// github package; it does not modify Client's existing methods in
// github.go (see that file's doc comment, which already reserves this as
// out-of-scope for T-M1-03 and forward-references T-M2-03).
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"unicode/utf8"
)

// DefaultStatusContext is the GitHub status "context" used when
// NewCommitStatusReporter is given an empty contextName. It is the label
// shown next to the check in GitHub's PR/commit UI.
const DefaultStatusContext = "drassi"

// maxStatusDescriptionRunes is GitHub's documented limit on the `description`
// field of a commit status; longer values are rejected by the API.
const maxStatusDescriptionRunes = 140

// validCommitStatusStates are the values GitHub's Statuses API accepts for
// `state`. ReportStatus passes its state argument straight through to the
// request body once validated here — no further translation.
var validCommitStatusStates = map[string]bool{
	"pending": true,
	"success": true,
	"failure": true,
	"error":   true,
}

// CommitStatusReporter posts commit statuses to GitHub
// (POST /repos/{owner}/{repo}/statuses/{sha}), reusing an existing *Client's
// token/auth (T-M1-03) rather than a second HTTP client or token path.
//
// It is best-effort by contract: ReportStatus logs a failure itself (never
// including the token — see (*Client).do, which already keeps the token out
// of error messages) and also returns the error so a caller that wants to
// notice can, but callers implementing T-M2-03's run-finalize hook are
// expected to log-and-ignore rather than fail a run over a GitHub API
// hiccup.
type CommitStatusReporter struct {
	client  *Client
	context string
}

// NewCommitStatusReporter builds a CommitStatusReporter backed by client.
// contextName is the GitHub status "context" (e.g. "drassi"); if empty,
// DefaultStatusContext is used. The same contextName must be reused across
// a check's pending and terminal posts for a given commit, or GitHub shows
// two separate check rows instead of one that transitions.
func NewCommitStatusReporter(client *Client, contextName string) *CommitStatusReporter {
	if contextName == "" {
		contextName = DefaultStatusContext
	}
	return &CommitStatusReporter{client: client, context: contextName}
}

// commitStatusRequestBody is the JSON body for
// POST /repos/{owner}/{repo}/statuses/{sha}.
type commitStatusRequestBody struct {
	State       string `json:"state"`
	Context     string `json:"context"`
	Description string `json:"description"`
}

// ReportStatus posts a commit status for sha in repo ("owner/name"). state
// must be one of "pending", "success", "failure", "error" — GitHub's own
// status states — and is passed straight through as the API's `state`
// field, with no further mapping. description is a short, human-readable
// summary (truncated to GitHub's 140-character limit if longer); it must
// never contain a secret or token.
//
// Success is HTTP 201; any other outcome (network error, non-2xx response,
// an invalid state/repo/sha) returns a wrapped error. This method itself
// logs a failure (without the token) so it is visible even if a caller
// discards the error, which is the expected best-effort usage from the
// dispatch run-finalize hook (T-M2-03) and the webhook "pending" post
// (T-M2-01): never fail a run or block a webhook response over this.
func (r *CommitStatusReporter) ReportStatus(ctx context.Context, repo, sha, state, description string) error {
	if !validCommitStatusStates[state] {
		err := fmt.Errorf("github: ReportStatus(%s, %s): invalid state %q, want one of pending|success|failure|error", repo, sha, state)
		log.Printf("%v", err)
		return err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		log.Printf("github: ReportStatus: %v", err)
		return err
	}
	if sha == "" {
		err := fmt.Errorf("github: ReportStatus(%s): sha must not be empty", repo)
		log.Printf("%v", err)
		return err
	}

	description = truncateDescription(description)
	payload, err := json.Marshal(commitStatusRequestBody{
		State:       state,
		Context:     r.context,
		Description: description,
	})
	if err != nil {
		err = fmt.Errorf("github: ReportStatus(%s, %s): encode request body: %w", repo, sha, err)
		log.Printf("%v", err)
		return err
	}

	path := fmt.Sprintf("/repos/%s/%s/statuses/%s", owner, name, sha)
	req, err := r.newStatusRequest(ctx, path, payload)
	if err != nil {
		err = fmt.Errorf("github: ReportStatus(%s, %s): %w", repo, sha, err)
		log.Printf("%v", err)
		return err
	}

	if _, err := r.client.do(req); err != nil {
		err = fmt.Errorf("github: ReportStatus(%s, %s, state=%s, context=%s): %w", repo, sha, state, r.context, err)
		log.Printf("github: commit status post failed (best-effort, run/dispatch not affected): %v", err)
		return err
	}
	return nil
}

// newStatusRequest builds the POST request for path with a JSON body,
// mirroring (*Client).newRequest's auth/header pattern (same Bearer token,
// API version, JSON Accept type) since newRequest itself has no body
// parameter. Built here rather than added to newRequest so github.go's
// existing methods are untouched (T-M2-03 scope: additive only).
func (r *CommitStatusReporter) newStatusRequest(ctx context.Context, path string, payload []byte) (*http.Request, error) {
	c := r.client
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base()+path, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("github: build request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", apiVersion)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	return req, nil
}

// truncateDescription trims s to GitHub's 140-character (rune) limit on a
// commit status description, leaving it unchanged if already short enough.
func truncateDescription(s string) string {
	if utf8.RuneCountInString(s) <= maxStatusDescriptionRunes {
		return s
	}
	runes := []rune(s)
	return string(runes[:maxStatusDescriptionRunes])
}
