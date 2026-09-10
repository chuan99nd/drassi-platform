// This file implements T-M2-04: the GitLab push webhook receiver, mirroring
// the GitHub webhook path (T-M2-01) but adapted to GitLab's auth model and
// payload shape.
//
// MVP scope follows Option A from T-M6-03: the repo is hosted on GitLab, but
// its workflow files are still GitHub-Actions-shaped
// (.github/workflows/*.yml), so the existing act-based planner
// (internal/planner) is reused completely unchanged -- only the webhook
// ingress, auth, and clone/token adapter differ from the GitHub path. Native
// .gitlab-ci.yml parsing is out of scope (that is the larger Option B in
// T-M6-03).
//
// GitLab->GHA event-context mapping is intentionally minimal for MVP: the
// raw GitLab push payload is passed straight through as PlanInput's
// EventPayload, so `${{ github.event.* }}` expressions a workflow may use
// will mostly be absent/unpopulated for GitLab-sourced runs. Only
// repo/ref/sha and the "push" event name are guaranteed. Full mapping is
// out of scope (see the task's "Out of scope" section).
//
// All dependencies the handler needs (fetching the workflow YAML, planning,
// waking the dispatcher) are expressed as small interfaces on
// GitLabWebhookDeps so tests can fake them without touching the network or a
// real database, mirroring internal/api's RunsDeps pattern (T-M1-05).
package webhook

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/chuan99nd/drassi-platform/internal/gitlab"
	"github.com/chuan99nd/drassi-platform/internal/planner"
)

// gitlabMaxBodyBytes bounds how much of a webhook delivery body this handler
// will read, guarding against an oversized/malicious payload. GitLab push
// payloads (commit lists etc.) can be sizable but are never anywhere near
// this large in practice.
const gitlabMaxBodyBytes = 10 << 20 // 10MiB

// GitLabFetcher is the subset of *gitlab.Client the webhook handler needs to
// fetch the workflow YAML that should be planned for a push. It exists so
// tests can fake GitLab without hitting the network; *gitlab.Client
// (internal/gitlab) satisfies it structurally.
type GitLabFetcher interface {
	ListWorkflows(ctx context.Context, project, sha string) ([]string, error)
	FetchWorkflow(ctx context.Context, project, sha, path string) ([]byte, error)
}

// Notifier is told about a newly-planned run so a connected runner can react
// immediately instead of waiting for its next poll. It is optional
// (GitLabWebhookDeps.Notifier may be left nil) -- the handler no-ops when it
// is unset.
type Notifier interface {
	Notify()
}

// GitLabWebhookDeps carries everything RegisterGitLabWebhook's handler
// needs. Construct one per process (or per test) and pass it to
// RegisterGitLabWebhook.
type GitLabWebhookDeps struct {
	// Secret is the expected value of the X-Gitlab-Token header (GitLab's
	// webhook auth: a shared secret compared for equality, not an HMAC
	// signature). Configure via DRASSI_GITLAB_WEBHOOK_SECRET. A delivery
	// whose header doesn't match (including when Secret is itself empty,
	// i.e. unconfigured) is rejected with 401.
	Secret string

	// AllowProject is the single allowlisted project path_with_namespace
	// (e.g. "group/subgroup/name") this webhook will act on -- the
	// CONVENTIONS.md host-mode security gate: MVP only ever plans/executes
	// for a repo we control, never an arbitrary inbound project. Deliveries
	// for any other project are acknowledged (202) but ignored.
	AllowProject string

	// GitLab fetches the workflow YAML to plan, from the GitLab project at
	// the pushed commit sha.
	GitLab GitLabFetcher

	// Planner turns the fetched workflow YAML + push context into a
	// persisted run plus queued jobs. Reused unchanged from the GitHub path
	// (internal/planner is source-host-agnostic).
	Planner planner.Planner

	// Notifier wakes the dispatcher after a run has been planned. Optional;
	// nil-safe.
	Notifier Notifier

	// WorkflowPath, when set, is the single workflow file fetched directly
	// via GitLab.FetchWorkflow (e.g. ".github/workflows/ci.yml" -- the MVP
	// default fetch target). When empty, the handler instead calls
	// GitLab.ListWorkflows and plans the first workflow found under
	// .github/workflows/.
	WorkflowPath string
}

// gitlabPushProject mirrors the fields this handler needs from a GitLab push
// webhook's "project" object.
type gitlabPushProject struct {
	PathWithNamespace string `json:"path_with_namespace"`
}

// gitlabPushEvent mirrors the fields this handler needs from a GitLab push
// webhook JSON body. See
// https://docs.gitlab.com/ee/user/project/integrations/webhook_events.html#push-events.
type gitlabPushEvent struct {
	ObjectKind   string            `json:"object_kind"`
	Ref          string            `json:"ref"`
	CheckoutSHA  string            `json:"checkout_sha"`
	UserUsername string            `json:"user_username"`
	Project      gitlabPushProject `json:"project"`
}

type gitlabWebhookResponse struct {
	Status string `json:"status"`
	RunID  string `json:"run_id,omitempty"`
}

type gitlabWebhookErrorResponse struct {
	Error string `json:"error"`
}

func gitlabWriteJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("webhook: encode gitlab webhook response: %v", err)
	}
}

func gitlabWriteError(w http.ResponseWriter, status int, msg string) {
	gitlabWriteJSON(w, status, gitlabWebhookErrorResponse{Error: msg})
}

// gitlabInternalError logs the real error (which may reference internal
// detail, e.g. an upstream GitLab failure) and writes a generic 500 body, so
// no internal detail or tokenized clone URL ever reaches the client.
func gitlabInternalError(w http.ResponseWriter, context string, err error) {
	log.Printf("webhook: gitlab: %s: %v", context, err)
	gitlabWriteError(w, http.StatusInternalServerError, "internal error")
}

func gitlabIgnored(w http.ResponseWriter) {
	gitlabWriteJSON(w, http.StatusAccepted, gitlabWebhookResponse{Status: "ignored"})
}

// RegisterGitLabWebhook adds POST /webhooks/gitlab to r. Mount r at "/api"
// (e.g. via r.Route("/api", func(r chi.Router) { webhook.RegisterGitLabWebhook(r, deps) }));
// RegisterGitLabWebhook itself only ever registers a relative path.
func RegisterGitLabWebhook(r chi.Router, deps GitLabWebhookDeps) {
	r.Post("/webhooks/gitlab", gitlabWebhookHandler(deps))
}

func gitlabWebhookHandler(deps GitLabWebhookDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		// --- 1. Auth: X-Gitlab-Token is a shared secret, compared for
		// equality in constant time -- NOT an HMAC signature (that's the
		// GitHub path's X-Hub-Signature-256). An empty configured Secret is
		// treated as "misconfigured, reject everything" rather than as an
		// accidentally-open endpoint.
		got := req.Header.Get("X-Gitlab-Token")
		if deps.Secret == "" || subtle.ConstantTimeCompare([]byte(got), []byte(deps.Secret)) != 1 {
			gitlabWriteError(w, http.StatusUnauthorized, "invalid or missing X-Gitlab-Token")
			return
		}

		body, err := io.ReadAll(io.LimitReader(req.Body, gitlabMaxBodyBytes))
		if err != nil {
			gitlabWriteError(w, http.StatusBadRequest, "failed to read request body")
			return
		}

		var evt gitlabPushEvent
		if err := json.Unmarshal(body, &evt); err != nil {
			gitlabWriteError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		// --- 2. Only push events. GitLab also sends X-Gitlab-Event: "Push
		// Hook" for these, but object_kind in the body is the authoritative
		// signal and is what we gate on (some proxies/relays may not
		// preserve the header). Anything else (tag pushes, merge requests,
		// pipeline hooks, etc.) is acknowledged but ignored so GitLab won't
		// retry the delivery.
		if evt.ObjectKind != "push" {
			gitlabIgnored(w)
			return
		}

		// --- 3. SECURITY GATE (CONVENTIONS.md host-mode posture): only ever
		// plan/execute for the one allowlisted project we control. No
		// fork/arbitrary-project execution in host mode. Gated deliveries
		// are acknowledged (202), never 4xx, so GitLab won't retry them.
		if deps.AllowProject == "" || evt.Project.PathWithNamespace != deps.AllowProject {
			gitlabIgnored(w)
			return
		}

		if evt.CheckoutSHA == "" || evt.Ref == "" {
			gitlabWriteError(w, http.StatusBadRequest, "push event missing ref or checkout_sha")
			return
		}

		ctx := req.Context()
		project := evt.Project.PathWithNamespace

		// --- 4. Fetch the workflow YAML at checkout_sha. When
		// WorkflowPath is configured, fetch exactly that file; otherwise
		// discover workflows under .github/workflows/ and plan the first
		// one found (MVP: one workflow per push).
		var workflowYAML []byte
		if deps.WorkflowPath != "" {
			yamlBytes, err := deps.GitLab.FetchWorkflow(ctx, project, evt.CheckoutSHA, deps.WorkflowPath)
			if err != nil {
				gitlabHandleFetchError(w, "fetch workflow", err)
				return
			}
			workflowYAML = yamlBytes
		} else {
			paths, err := deps.GitLab.ListWorkflows(ctx, project, evt.CheckoutSHA)
			if err != nil {
				gitlabHandleFetchError(w, "list workflows", err)
				return
			}
			if len(paths) == 0 {
				gitlabWriteError(w, http.StatusBadRequest, "no workflow files found under .github/workflows")
				return
			}
			yamlBytes, err := deps.GitLab.FetchWorkflow(ctx, project, evt.CheckoutSHA, paths[0])
			if err != nil {
				gitlabHandleFetchError(w, "fetch workflow", err)
				return
			}
			workflowYAML = yamlBytes
		}

		// --- 5. Plan. Reuses the run/dispatch/runner pipeline exactly as
		// the GitHub path does -- planner.PlanInput is source-host-agnostic.
		result, err := deps.Planner.Plan(ctx, planner.PlanInput{
			Repo:         project,
			Ref:          evt.Ref,
			SHA:          evt.CheckoutSHA,
			EventName:    "push",
			EventPayload: body,
			TriggeredBy:  evt.UserUsername,
			WorkflowYAML: workflowYAML,
		})
		if err != nil {
			if errors.Is(err, planner.ErrInvalidWorkflow) {
				gitlabWriteError(w, http.StatusBadRequest, err.Error())
				return
			}
			gitlabInternalError(w, "plan workflow", err)
			return
		}

		if deps.Notifier != nil {
			deps.Notifier.Notify()
		}

		gitlabWriteJSON(w, http.StatusAccepted, gitlabWebhookResponse{
			Status: "accepted",
			RunID:  result.RunID.String(),
		})
	}
}

// gitlabHandleFetchError maps a GitLab fetch error to the documented HTTP
// status: gitlab.ErrNotFound is bad input (400, generic message -- never
// embeds a token or clone URL), everything else is an unexpected upstream
// failure (500, generic body; the real error is logged server-side only).
func gitlabHandleFetchError(w http.ResponseWriter, context string, err error) {
	if errors.Is(err, gitlab.ErrNotFound) {
		gitlabWriteError(w, http.StatusBadRequest, "workflow file or ref not found in GitLab project")
		return
	}
	gitlabInternalError(w, context, err)
}
