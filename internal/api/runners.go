// Package api implements the drassi-server REST API: a chi sub-router mounted
// at /api by cmd/drassi-server. Handlers translate internal/store domain
// types into response DTOs (never leaking store struct tags) and always
// write application/json.
package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/chuan99nd/drassi-platform/internal/store"
)

// runnerResponse is the JSON DTO for GET /api/runners. Field names are
// snake_case to match the wire contract in
// tasks/M0/T-M0-07-runners-api-and-fe-skeleton.md and the FE Runner type in
// web/src/types.ts. Only a subset of store.Runner's columns are exposed in
// M0 (capacity/version/created_at/auth_token are omitted).
type runnerResponse struct {
	ID            string   `json:"id"`
	Name          string   `json:"name"`
	Labels        []string `json:"labels"`
	Mode          string   `json:"mode"`
	Status        string   `json:"status"`
	LastHeartbeat *string  `json:"last_heartbeat"` // ISO-8601 UTC, or null
}

func toRunnerResponse(r store.Runner) runnerResponse {
	resp := runnerResponse{
		ID:     r.ID.String(),
		Name:   r.Name,
		Labels: r.Labels,
		Mode:   r.Mode,
		Status: r.Status,
	}
	// Never emit a null labels array: the FE type expects string[].
	if resp.Labels == nil {
		resp.Labels = []string{}
	}
	if r.LastHeartbeat != nil {
		s := r.LastHeartbeat.UTC().Format(time.RFC3339)
		resp.LastHeartbeat = &s
	}
	return resp
}

// NewRouter builds the /api sub-router. Mount it on the parent chi router
// with r.Mount("/api", api.NewRouter(runnerStore)).
func NewRouter(runnerStore store.RunnerStore) chi.Router {
	r := chi.NewRouter()
	r.Get("/runners", listRunnersHandler(runnerStore))
	return r
}

func listRunnersHandler(runnerStore store.RunnerStore) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		runners, err := runnerStore.List(req.Context())
		if err != nil {
			log.Printf("api: list runners: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal error"}`))
			return
		}

		// Never emit `null` for an empty list.
		resp := make([]runnerResponse, 0, len(runners))
		for _, r := range runners {
			resp = append(resp, toRunnerResponse(r))
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("api: encode runners response: %v", err)
		}
	}
}
