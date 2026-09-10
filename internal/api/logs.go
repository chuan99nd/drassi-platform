// T-M1-07: SSE live log tail + full log fetch REST endpoints, layered
// directly on top of internal/logstore (persistence + pub/sub) and
// internal/store.JobStore (job existence / terminal status). This file is
// self-contained within package api: all helpers are prefixed `logs` to
// avoid clashing with other handlers registered on the same router (see
// runners.go).
//
// SSE framing chosen (documented per T-M1-07's "pick one and document it"):
//   - The event id is the persisted chunk's `seq` (int64, decimal).
//   - The payload is the chunk's raw UTF-8 bytes, NOT base64-encoded: each
//     `\n`-delimited line of the chunk is emitted as its own `data:` field
//     per the SSE spec, so multi-line chunks reassemble correctly on the
//     client (EventSource joins multiple `data:` lines with "\n"). A single
//     trailing empty line produced by a trailing "\n" in the chunk is
//     dropped (it's a split artifact, not a real blank line); genuine blank
//     lines elsewhere in the chunk are preserved as empty `data:` fields.
//   - Every log event carries `event: log`. A final `event: end` (JSON
//     `{"status": "<job status>"}`) is sent, with no `id:` line, once the
//     job has reached a terminal status and no further chunks remain.
//
// Resume semantics: `Last-Event-ID` (set automatically by EventSource on
// reconnect) takes precedence over `?from_seq=`; resume is EXCLUSIVE
// (replay seq > Last-Event-ID, since that seq was already delivered before
// the disconnect). An initial `?from_seq=N` is INCLUSIVE (replay seq >= N).
// Both reduce to a single "effective start seq" fed to ReplayFrom.
//
// Replay -> live bridge (no gap/dupe): Subscribe() is called BEFORE
// ReplayFrom(), so any chunk published while replay is in flight lands in
// the subscriber's buffered channel instead of being missed. Chunks read
// from the live channel with seq <= the last seq already delivered (from
// replay, or from an earlier live chunk) are dropped as duplicates of the
// overlap window.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/store"
)

// logsSSEKeepAlive is the interval between idle-connection keep-alive SSE
// comments, and also the cadence at which the handler re-checks whether the
// job has reached a terminal status (to decide whether to close the
// stream). Var (not const) so tests could shorten it, though the tests in
// logs_test.go avoid needing to by using an already-terminal job.
var logsSSEKeepAlive = 15 * time.Second

// logsTerminalJobStatuses mirrors the terminal states of the jobs.status
// state machine (see internal/store/job_store.go's jobTransitions: any
// status absent from that map's keys has no outgoing transitions).
var logsTerminalJobStatuses = map[string]bool{
	"success":   true,
	"failure":   true,
	"skipped":   true,
	"cancelled": true,
}

// LogsDeps are the dependencies of the log endpoints registered by
// RegisterLogs.
type LogsDeps struct {
	// LogStore provides persisted chunk replay (ReplayFrom) and live
	// fan-out (Subscribe) — see internal/logstore (T-M1-06).
	LogStore *logstore.Store
	// JobStore resolves job ids (404 on unknown) and terminal status, to
	// know when the SSE stream should close.
	JobStore store.JobStore
}

// RegisterLogs mounts the log-reading routes on r:
//
//	GET /jobs/{id}/logs      SSE live tail (see package doc for framing)
//	GET /jobs/{id}/logs.txt  full concatenated log, text/plain
//
// The caller (cmd/drassi-server) is responsible for mounting r such that
// these resolve to /api/jobs/{id}/logs[.txt].
func RegisterLogs(r chi.Router, deps LogsDeps) {
	r.Get("/jobs/{id}/logs", logsSSEHandler(deps))
	r.Get("/jobs/{id}/logs.txt", logsTxtHandler(deps))
}

// logsParseJobID extracts and validates the {id} URL param, writing a 400
// and returning ok==false if it is not a UUID.
func logsParseJobID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	raw := chi.URLParam(r, "id")
	id, err := uuid.Parse(raw)
	if err != nil {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return uuid.UUID{}, false
	}
	return id, true
}

// logsGetJob fetches the job or writes 404/500 and returns ok==false.
func logsGetJob(ctx context.Context, w http.ResponseWriter, jobStore store.JobStore, id uuid.UUID) (*store.Job, bool) {
	job, err := jobStore.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "job not found", http.StatusNotFound)
			return nil, false
		}
		log.Printf("api: logs: get job %s: %v", id, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return nil, false
	}
	return job, true
}

// logsEffectiveStartSeq computes the inclusive starting seq for ReplayFrom
// from the request: Last-Event-ID (exclusive: seq > id, so start = id+1)
// takes precedence over ?from_seq= (inclusive: start = from_seq, default
// 0). Returns ok==false (400 already written) on a malformed value.
func logsEffectiveStartSeq(w http.ResponseWriter, r *http.Request) (int64, bool) {
	if lastEventID := r.Header.Get("Last-Event-ID"); lastEventID != "" {
		seq, err := strconv.ParseInt(strings.TrimSpace(lastEventID), 10, 64)
		if err != nil {
			http.Error(w, "invalid Last-Event-ID", http.StatusBadRequest)
			return 0, false
		}
		return seq + 1, true
	}
	if fromSeq := r.URL.Query().Get("from_seq"); fromSeq != "" {
		seq, err := strconv.ParseInt(strings.TrimSpace(fromSeq), 10, 64)
		if err != nil {
			http.Error(w, "invalid from_seq", http.StatusBadRequest)
			return 0, false
		}
		return seq, true
	}
	return 0, true
}

// logsWriteChunkEvent frames one persisted/live chunk as an SSE `log`
// event per the package doc's framing, and flushes.
func logsWriteChunkEvent(w http.ResponseWriter, f http.Flusher, c logstore.Chunk) error {
	lines := strings.Split(string(c.Data), "\n")
	// Drop the single trailing "" produced by a trailing "\n" in the
	// chunk data; it is a Split artifact, not a real blank line.
	if len(lines) > 1 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if _, err := fmt.Fprintf(w, "id: %d\n", c.Seq); err != nil {
		return err
	}
	if _, err := fmt.Fprint(w, "event: log\n"); err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprint(w, "\n"); err != nil {
		return err
	}
	f.Flush()
	return nil
}

// logsWriteComment writes an SSE comment line (used for the initial
// connect ack and periodic keep-alives) and flushes.
func logsWriteComment(w http.ResponseWriter, f http.Flusher, text string) error {
	if _, err := fmt.Fprintf(w, ": %s\n\n", text); err != nil {
		return err
	}
	f.Flush()
	return nil
}

// logsWriteEnd writes the terminal `end` event (no `id:` line — see
// package doc) and flushes.
func logsWriteEnd(w http.ResponseWriter, f http.Flusher, status string) error {
	payload, err := json.Marshal(struct {
		Status string `json:"status"`
	}{Status: status})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprint(w, "event: end\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return err
	}
	f.Flush()
	return nil
}

// logsSSEHandler implements GET /jobs/{id}/logs. See the package doc for
// framing, resume semantics, and the replay->live bridge strategy.
func logsSSEHandler(deps LogsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		jobID, ok := logsParseJobID(w, r)
		if !ok {
			return
		}
		// 404 unknown job BEFORE upgrading to the event stream.
		job, ok := logsGetJob(ctx, w, deps.JobStore, jobID)
		if !ok {
			return
		}
		startSeq, ok := logsEffectiveStartSeq(w, r)
		if !ok {
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx etc.)
		w.WriteHeader(http.StatusOK)
		if err := logsWriteComment(w, flusher, "connected"); err != nil {
			return
		}

		// Subscribe BEFORE replay so chunks published mid-replay are
		// buffered, not lost (see package doc's replay->live bridge note).
		sub, cancel := deps.LogStore.Subscribe(jobID)
		defer cancel()

		lastSeq := startSeq - 1 // nothing delivered yet

		chunks, err := deps.LogStore.ReplayFrom(ctx, jobID, startSeq)
		if err != nil {
			log.Printf("api: logs: replay job %s from %d: %v", jobID, startSeq, err)
			return
		}
		for _, c := range chunks {
			if err := logsWriteChunkEvent(w, flusher, c); err != nil {
				return
			}
			lastSeq = c.Seq
		}

		// If the job is already terminal, catch any stragglers appended
		// since ReplayFrom (via the subscriber buffer and one more
		// authoritative replay), then end the stream — no need to wait
		// for the keep-alive tick.
		if done, err := logsMaybeFinish(ctx, w, flusher, deps, jobID, job.Status, sub, &lastSeq); done || err != nil {
			return
		}

		ticker := time.NewTicker(logsSSEKeepAlive)
		defer ticker.Stop()

		for {
			select {
			case c, ok := <-sub:
				if !ok {
					// Dropped for falling too far behind (see
					// logstore.Store.Publish's drop policy). Resync once
					// from Postgres, then end this connection; the client
					// (EventSource) will reconnect with Last-Event-ID and
					// get a fresh subscription.
					if resynced, rerr := deps.LogStore.ReplayFrom(ctx, jobID, lastSeq+1); rerr == nil {
						for _, rc := range resynced {
							if werr := logsWriteChunkEvent(w, flusher, rc); werr != nil {
								return
							}
							lastSeq = rc.Seq
						}
					} else {
						log.Printf("api: logs: resync job %s from %d: %v", jobID, lastSeq+1, rerr)
					}
					return
				}
				if c.Seq <= lastSeq {
					continue // overlap with what replay (or an earlier live chunk) already sent
				}
				if err := logsWriteChunkEvent(w, flusher, c); err != nil {
					return
				}
				lastSeq = c.Seq

			case <-ticker.C:
				if err := logsWriteComment(w, flusher, "keep-alive"); err != nil {
					return
				}
				status, err := logsCurrentJobStatus(ctx, deps.JobStore, jobID)
				if err != nil {
					log.Printf("api: logs: poll job %s status: %v", jobID, err)
					continue
				}
				if done, err := logsMaybeFinish(ctx, w, flusher, deps, jobID, status, sub, &lastSeq); done || err != nil {
					return
				}

			case <-ctx.Done():
				return
			}
		}
	}
}

// logsCurrentJobStatus re-fetches the job's status (used on each
// keep-alive tick to notice a job that became terminal after the stream
// started).
func logsCurrentJobStatus(ctx context.Context, jobStore store.JobStore, jobID uuid.UUID) (string, error) {
	job, err := jobStore.GetJob(ctx, jobID)
	if err != nil {
		return "", err
	}
	return job.Status, nil
}

// logsMaybeFinish checks whether status is terminal; if so it drains any
// chunks still sitting in sub's buffer plus one authoritative ReplayFrom
// (covers the race between a chunk being published and this check), writes
// the `end` event, and returns done=true. If status is not terminal it is
// a no-op returning done=false.
func logsMaybeFinish(
	ctx context.Context,
	w http.ResponseWriter,
	f http.Flusher,
	deps LogsDeps,
	jobID uuid.UUID,
	status string,
	sub <-chan logstore.Chunk,
	lastSeq *int64,
) (done bool, err error) {
	if !logsTerminalJobStatuses[status] {
		return false, nil
	}

	// Drain whatever is already buffered (non-blocking).
drain:
	for {
		select {
		case c, ok := <-sub:
			if !ok {
				break drain
			}
			if c.Seq > *lastSeq {
				if werr := logsWriteChunkEvent(w, f, c); werr != nil {
					return true, werr
				}
				*lastSeq = c.Seq
			}
		default:
			break drain
		}
	}

	// Authoritative catch-up straight from Postgres in case anything was
	// published+persisted after our last check but missed the buffer drain
	// above (e.g. arrived between the terminal-status read and now).
	stragglers, rerr := deps.LogStore.ReplayFrom(ctx, jobID, *lastSeq+1)
	if rerr != nil {
		log.Printf("api: logs: final replay job %s from %d: %v", jobID, *lastSeq+1, rerr)
	} else {
		for _, c := range stragglers {
			if werr := logsWriteChunkEvent(w, f, c); werr != nil {
				return true, werr
			}
			*lastSeq = c.Seq
		}
	}

	if werr := logsWriteEnd(w, f, status); werr != nil {
		return true, werr
	}
	return true, nil
}

// logsTxtHandler implements GET /jobs/{id}/logs.txt: a one-shot snapshot of
// the full concatenated log in seq order, written chunk-by-chunk (not
// buffered into one big string) so a large log doesn't require holding the
// whole thing in memory beyond what ReplayFrom itself already materializes.
func logsTxtHandler(deps LogsDeps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		jobID, ok := logsParseJobID(w, r)
		if !ok {
			return
		}
		if _, ok := logsGetJob(ctx, w, deps.JobStore, jobID); !ok {
			return
		}

		chunks, err := deps.LogStore.ReplayFrom(ctx, jobID, 0)
		if err != nil {
			log.Printf("api: logs.txt: replay job %s: %v", jobID, err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		for _, c := range chunks {
			if _, err := w.Write(c.Data); err != nil {
				log.Printf("api: logs.txt: write job %s: %v", jobID, err)
				return
			}
		}
	}
}
