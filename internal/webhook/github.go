// Package webhook implements the GitHub webhook receiver (T-M2-01) and the
// event -> plan mapping/filtering that turns a verified push delivery into
// zero or more planner.Plan calls (T-M2-02).
//
// Everything for GitHub lives in this file by design (see the task's
// parallel-safety scope rule): a sibling agent owns internal/webhook/gitlab.go
// and internal/dispatch/service.go / internal/github/status.go concurrently,
// so this package does not touch those files, internal/config, or
// cmd/drassi-server/main.go. RegisterGitHubWebhook is the only exported entry
// point the orchestrator wires in; it reads DRASSI_GH_WEBHOOK_SECRET /
// DRASSI_GH_ALLOWED_REPO from env itself and builds GitHubWebhookDeps.
//
// Flow: verify HMAC on the raw body -> route on X-GitHub-Event (push only,
// everything else 202-ignored) -> parse the push payload -> same-repo gate
// (no forks, allow-listed repo only -- the thing that makes host mode safe,
// see ../../../tasks/CONVENTIONS.md "Security posture") -> for each workflow
// file at the pushed sha, check its `on:` includes "push" and apply
// branches/branches-ignore/paths/paths-ignore filtering with act's
// pkg/workflowpattern -- only surviving workflows are handed to the planner.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/nektos/act/pkg/model"
	"github.com/nektos/act/pkg/workflowpattern"

	"github.com/chuan99nd/drassi-platform/internal/planner"
)

// maxGitHubWebhookBody caps how much of a webhook request body we will read
// into memory before verifying its signature (GitHub push payloads are
// usually well under 1 MiB; this is a generous backstop against abuse).
const maxGitHubWebhookBody = 5 << 20 // 5 MiB

// --- Push event wire types (JSON-tagged subset of GitHub's push payload) ---

// GitHubRepo is the subset of GitHub's push-event `repository` object this
// package needs for the same-repo security gate and for the fetch calls.
type GitHubRepo struct {
	FullName      string `json:"full_name"`
	Fork          bool   `json:"fork"`
	DefaultBranch string `json:"default_branch"`
	CloneURL      string `json:"clone_url"`
}

// GitHubPusher is the subset of the push-event `pusher` object used as the
// run's triggered_by actor.
type GitHubPusher struct {
	Name string `json:"name"`
}

// GitHubCommit is one entry of `commits[]` (and the shape of `head_commit`):
// only the fields needed for changed-file path filtering and for run
// bookkeeping are kept; the rest of the payload is preserved verbatim in the
// raw body forwarded to the planner as event_payload_json / event_json.
type GitHubCommit struct {
	ID       string   `json:"id"`
	Message  string   `json:"message"`
	Added    []string `json:"added"`
	Removed  []string `json:"removed"`
	Modified []string `json:"modified"`
}

// PushEvent is the typed subset of GitHub's `push` webhook payload this
// package parses. The full raw body (not this struct) is what gets forwarded
// to the planner as EventPayload / event_json, so any field this struct
// doesn't model is still preserved for the runner.
type PushEvent struct {
	Ref        string         `json:"ref"`
	After      string         `json:"after"`
	Repository GitHubRepo     `json:"repository"`
	Pusher     GitHubPusher   `json:"pusher"`
	HeadCommit *GitHubCommit  `json:"head_commit"`
	Commits    []GitHubCommit `json:"commits"`
}

// --- Deps interfaces (shared contract with concurrently-developed pieces) ---

// CommitStatusReporter posts a commit status to GitHub. state is one of
// "pending", "success", "failure", "error"; repo is "owner/name". A
// concurrent agent supplies the real implementation (backed by
// internal/github); it is optional here (nil-safe).
type CommitStatusReporter interface {
	ReportStatus(ctx context.Context, repo, sha, state, description string) error
}

// GitHubFetcher is the subset of *github.Client this package needs to
// discover and read workflow files at a given commit. *github.Client
// (internal/github) satisfies this structurally.
type GitHubFetcher interface {
	ListWorkflows(ctx context.Context, repo, sha string) ([]string, error)
	FetchWorkflow(ctx context.Context, repo, sha, path string) ([]byte, error)
}

// Notifier wakes the dispatcher so a connected runner reacts to newly
// enqueued jobs immediately instead of waiting for its next poll.
//
// NOTE: declared in gitlab.go (T-M2-04, same package, identical shape) --
// not redeclared here to avoid a duplicate-identifier build error, since
// both webhook paths need exactly the same one-method interface.

// GitHubWebhookDeps carries everything RegisterGitHubWebhook's handler needs.
// The orchestrator constructs one from env (DRASSI_GH_WEBHOOK_SECRET,
// DRASSI_GH_ALLOWED_REPO) plus the shared GitHub client / planner / dispatch
// notifier / commit-status reporter and passes it to RegisterGitHubWebhook.
type GitHubWebhookDeps struct {
	// Secret is the GitHub webhook signing secret. Empty means the endpoint
	// is unconfigured and every request is rejected with 401.
	Secret string
	// AllowRepo is the single trusted "owner/name" repo the same-repo gate
	// allows (see CONVENTIONS.md "Security posture": never fork PRs).
	AllowRepo string

	GitHub  GitHubFetcher
	Planner planner.Planner

	// Notifier is told about newly-planned jobs. Optional (nil-safe).
	Notifier Notifier
	// CommitStatus reports a "pending" status once a run is created.
	// Optional (nil-safe) -- T-M2-03 supplies the real implementation.
	CommitStatus CommitStatusReporter
}

// --- Signature verification ---

// Verify reports whether sig (the literal X-Hub-Signature-256 header value,
// "sha256=<hex>") is the correct HMAC-SHA256 of body under secret, using a
// constant-time comparison (crypto/hmac + hmac.Equal -- never "=="). Returns
// false for a missing/malformed header, a non-hex digest, or an empty secret.
func Verify(sig string, body []byte, secret []byte) bool {
	if len(secret) == 0 {
		return false
	}
	const prefix = "sha256="
	if !strings.HasPrefix(sig, prefix) {
		return false
	}
	sigBytes, err := hex.DecodeString(sig[len(prefix):])
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	expected := mac.Sum(nil)

	return hmac.Equal(sigBytes, expected)
}

// --- Registration ---

// RegisterGitHubWebhook registers POST /webhooks/github on r. The
// orchestrator mounts r as (or wires this onto) the top-level chi router --
// this route is intentionally not nested under /api, mirroring GitHub's own
// convention of a top-level webhook endpoint distinct from the FE REST API.
func RegisterGitHubWebhook(r chi.Router, deps GitHubWebhookDeps) {
	r.Post("/webhooks/github", githubWebhookHandler(deps))
}

func githubWebhookHandler(deps GitHubWebhookDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		delivery := req.Header.Get("X-GitHub-Delivery")

		// Read the raw body ONCE, before any parsing -- HMAC must be
		// verified against the exact bytes GitHub sent, not a re-marshaled
		// struct.
		req.Body = http.MaxBytesReader(w, req.Body, maxGitHubWebhookBody)
		body, err := io.ReadAll(req.Body)
		if err != nil {
			log.Printf("webhook/github: delivery=%s read body: %v", delivery, err)
			writeGitHubWebhookJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}

		sig := req.Header.Get("X-Hub-Signature-256")
		if !Verify(sig, body, []byte(deps.Secret)) {
			log.Printf("webhook/github: delivery=%s invalid or missing signature", delivery)
			writeGitHubWebhookJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid signature"})
			return
		}

		event := req.Header.Get("X-GitHub-Event")
		if event != "push" {
			log.Printf("webhook/github: delivery=%s event=%q ignored (not push)", delivery, event)
			writeGitHubWebhookJSON(w, http.StatusAccepted, map[string]string{"ignored": fmt.Sprintf("event %q not handled", event)})
			return
		}

		var push PushEvent
		if err := json.Unmarshal(body, &push); err != nil {
			log.Printf("webhook/github: delivery=%s invalid push payload: %v", delivery, err)
			writeGitHubWebhookJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid push payload"})
			return
		}

		if reason, gated := githubGateReason(push, deps.AllowRepo); gated {
			log.Printf("webhook/github: delivery=%s repo=%s ref=%s ignored: %s", delivery, push.Repository.FullName, push.Ref, reason)
			writeGitHubWebhookJSON(w, http.StatusAccepted, map[string]string{"ignored": reason})
			return
		}

		if push.After == "" {
			log.Printf("webhook/github: delivery=%s missing after-sha", delivery)
			writeGitHubWebhookJSON(w, http.StatusBadRequest, map[string]string{"error": "missing after sha"})
			return
		}

		ctx := req.Context()
		repo := push.Repository.FullName
		sha := push.After
		branch := shortBranch(push.Ref)
		files := changedFiles(push)

		log.Printf("webhook/github: delivery=%s event=push repo=%s ref=%s sha=%s pusher=%s accepted",
			delivery, repo, push.Ref, shortGitSHA(sha), push.Pusher.Name)

		workflowPaths, err := deps.GitHub.ListWorkflows(ctx, repo, sha)
		if err != nil {
			log.Printf("webhook/github: delivery=%s list workflows for %s@%s: %v", delivery, repo, shortGitSHA(sha), err)
			writeGitHubWebhookJSON(w, http.StatusAccepted, map[string]string{"ignored": "failed to list workflows"})
			return
		}

		runIDs := make([]string, 0, len(workflowPaths))
		for _, path := range workflowPaths {
			runID, ok := planGitHubPushWorkflow(ctx, deps, delivery, repo, sha, branch, files, path, push, body)
			if ok {
				runIDs = append(runIDs, runID)
			}
		}

		if len(runIDs) == 0 {
			writeGitHubWebhookJSON(w, http.StatusAccepted, map[string]string{"ignored": "no workflow matched this push"})
			return
		}

		writeGitHubWebhookJSON(w, http.StatusAccepted, map[string]any{"run_ids": runIDs})
	}
}

// planGitHubPushWorkflow fetches one workflow file, decides (via its `on:`
// config) whether this push should trigger it, and if so calls the planner.
// Returns the created run id and true on success; false (with no run
// created) if the workflow was skipped or failed to fetch/parse/plan -- each
// case is logged with its reason so a single bad workflow file doesn't fail
// the whole delivery.
func planGitHubPushWorkflow(
	ctx context.Context,
	deps GitHubWebhookDeps,
	delivery, repo, sha, branch string,
	files []string,
	path string,
	push PushEvent,
	rawBody []byte,
) (runID string, ok bool) {
	yamlBytes, err := deps.GitHub.FetchWorkflow(ctx, repo, sha, path)
	if err != nil {
		log.Printf("webhook/github: delivery=%s fetch workflow %s: %v", delivery, path, err)
		return "", false
	}

	wf, err := model.ReadWorkflow(bytes.NewReader(yamlBytes), false)
	if err != nil {
		log.Printf("webhook/github: delivery=%s parse workflow %s: %v", delivery, path, err)
		return "", false
	}

	if !hasGitHubEvent(wf.On(), "push") {
		log.Printf("webhook/github: delivery=%s workflow=%s skipped: on: does not include push", delivery, path)
		return "", false
	}

	if passes, reason := passesFilters(wf.OnEvent("push"), branch, files); !passes {
		log.Printf("webhook/github: delivery=%s workflow=%s skipped: %s", delivery, path, reason)
		return "", false
	}

	result, err := deps.Planner.Plan(ctx, planner.PlanInput{
		Repo:         repo,
		Ref:          push.Ref,
		SHA:          sha,
		EventName:    "push",
		EventPayload: rawBody,
		TriggeredBy:  push.Pusher.Name,
		WorkflowYAML: yamlBytes,
	})
	if err != nil {
		log.Printf("webhook/github: delivery=%s plan workflow %s: %v", delivery, path, err)
		return "", false
	}

	if deps.Notifier != nil {
		deps.Notifier.Notify()
	}
	if deps.CommitStatus != nil {
		if err := deps.CommitStatus.ReportStatus(ctx, repo, sha, "pending", "Drassi: run queued"); err != nil {
			log.Printf("webhook/github: delivery=%s report commit status for %s: %v", delivery, path, err)
		}
	}

	return result.RunID.String(), true
}

// --- Same-repo security gate ---

// githubGateReason reports whether push must be ignored (fork-sourced, or
// not the single allow-listed repo) and, if so, why. A push event's own
// commits always originate in the repo itself (a fork cannot push to it), so
// the gate is exactly fork==false AND full_name matches allowRepo.
func githubGateReason(push PushEvent, allowRepo string) (reason string, gated bool) {
	if push.Repository.Fork {
		return "repository is a fork", true
	}
	if allowRepo == "" {
		return "no allow-listed repo configured", true
	}
	if push.Repository.FullName != allowRepo {
		return fmt.Sprintf("repo %q is not the allow-listed repo", push.Repository.FullName), true
	}
	return "", false
}

// --- event_json / trigger evaluation helpers (T-M2-02) ---

// BuildEventJSON returns the github.event.* document the runner should see
// for this delivery. For push, GitHub's github.event is the raw webhook body
// itself, so the minimum correct behavior -- and what this returns -- is the
// verbatim, already-signature-verified rawPayload. eventName is accepted for
// symmetry with other event types even though push is the only one M2
// handles; an empty/invalid payload is rejected so callers never persist
// unparseable event_json.
func BuildEventJSON(eventName string, rawPayload []byte) ([]byte, error) {
	if len(rawPayload) == 0 {
		return nil, fmt.Errorf("webhook: BuildEventJSON(%s): empty payload", eventName)
	}
	if !json.Valid(rawPayload) {
		return nil, fmt.Errorf("webhook: BuildEventJSON(%s): payload is not valid JSON", eventName)
	}
	return rawPayload, nil
}

// changedFiles returns the de-duplicated union of added+modified+removed
// paths across push.Commits and push.HeadCommit.
func changedFiles(push PushEvent) []string {
	seen := make(map[string]struct{})
	var out []string

	add := func(paths []string) {
		for _, p := range paths {
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}

	for _, c := range push.Commits {
		add(c.Added)
		add(c.Modified)
		add(c.Removed)
	}
	if push.HeadCommit != nil {
		add(push.HeadCommit.Added)
		add(push.HeadCommit.Modified)
		add(push.HeadCommit.Removed)
	}

	return out
}

// shortBranch strips the "refs/heads/" prefix from ref, returning "" for a
// tag ref (refs/tags/...) or anything else that isn't a branch ref.
func shortBranch(ref string) string {
	const prefix = "refs/heads/"
	if strings.HasPrefix(ref, prefix) {
		return ref[len(prefix):]
	}
	return ""
}

// shortGitSHA truncates sha to 7 hex chars for log lines, matching git's
// short-sha convention. Never used for anything security-sensitive.
func shortGitSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

// hasGitHubEvent reports whether eventName is present in the workflow's
// `on:` event list (as returned by act's Workflow.On(), which already
// normalizes the scalar/sequence/mapping forms of `on:`).
func hasGitHubEvent(events []string, eventName string) bool {
	for _, e := range events {
		if e == eventName {
			return true
		}
	}
	return false
}

// passesFilters applies on.push branches/branches-ignore/paths/paths-ignore
// filtering (act's pkg/workflowpattern) and reports whether the workflow
// should run, plus a human-readable reason when it should not.
//
// on is the raw value of Workflow.OnEvent("push"): nil when `on:` is a bare
// scalar/list (`on: push` / `on: [push]`) -- no per-event config, so no
// filtering applies -- or a map[string]interface{} when `on:` is a mapping
// (`on: {push: {...}}`).
//
// Precedence (documented per the task's "reject illegal combinations is out
// of scope" note): branches/paths use Skip (skip when NO input matches any
// positive pattern); branches-ignore/paths-ignore use Filter (skip when
// EVERY input matches an ignore pattern). If both a positive and its
// -ignore form are present, the workflow must survive both checks -- either
// one saying skip drops the workflow. branches/branches-ignore only apply to
// branch refs (branch == ""); a tag push (or other non-branch ref) skips a
// workflow that declares `branches` (GitHub itself does not run
// branches-only workflows for tag pushes) and is unaffected by
// branches-ignore.
func passesFilters(on interface{}, branch string, files []string) (ok bool, reason string) {
	m, isMap := on.(map[string]interface{})
	if !isMap {
		// Bare `on: push` / `on: [push]` (or push has no config at all) --
		// nothing to filter on.
		return true, ""
	}

	tw := &workflowpattern.EmptyTraceWriter{}

	branches := toGitHubPatternList(m["branches"])
	branchesIgnore := toGitHubPatternList(m["branches-ignore"])
	if len(branches) > 0 {
		if branch == "" {
			return false, "ref is not a branch but workflow restricts on.push.branches"
		}
		patterns, err := workflowpattern.CompilePatterns(branches...)
		if err != nil {
			return false, fmt.Sprintf("invalid on.push.branches pattern: %v", err)
		}
		if workflowpattern.Skip(patterns, []string{branch}, tw) {
			return false, fmt.Sprintf("branch %q does not match on.push.branches", branch)
		}
	}
	if len(branchesIgnore) > 0 && branch != "" {
		patterns, err := workflowpattern.CompilePatterns(branchesIgnore...)
		if err != nil {
			return false, fmt.Sprintf("invalid on.push.branches-ignore pattern: %v", err)
		}
		if workflowpattern.Filter(patterns, []string{branch}, tw) {
			return false, fmt.Sprintf("branch %q excluded by on.push.branches-ignore", branch)
		}
	}

	paths := toGitHubPatternList(m["paths"])
	pathsIgnore := toGitHubPatternList(m["paths-ignore"])
	if len(paths) > 0 {
		patterns, err := workflowpattern.CompilePatterns(paths...)
		if err != nil {
			return false, fmt.Sprintf("invalid on.push.paths pattern: %v", err)
		}
		if workflowpattern.Skip(patterns, files, tw) {
			return false, "no changed file matches on.push.paths"
		}
	}
	if len(pathsIgnore) > 0 {
		patterns, err := workflowpattern.CompilePatterns(pathsIgnore...)
		if err != nil {
			return false, fmt.Sprintf("invalid on.push.paths-ignore pattern: %v", err)
		}
		if workflowpattern.Filter(patterns, files, tw) {
			return false, "all changed files excluded by on.push.paths-ignore"
		}
	}

	return true, ""
}

// toGitHubPatternList normalizes a decoded YAML value for branches/paths
// (and their -ignore forms) -- which act's model leaves untyped -- into a
// []string. Handles the scalar form (a bare string) and the list form
// ([]interface{} of strings, as produced by yaml.v3 decoding into
// interface{}); anything else (including a nil/absent key) yields nil.
func toGitHubPatternList(v interface{}) []string {
	switch val := v.(type) {
	case nil:
		return nil
	case string:
		return []string{val}
	case []string:
		return val
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// --- wire helpers ---

func writeGitHubWebhookJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("webhook/github: encode response: %v", err)
	}
}
