package api

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/store"
)

// sseEvent is a parsed SSE frame (id/event optional, data joined with \n).
type sseEvent struct {
	id    string
	event string
	data  string
}

// parseSSE reads an SSE stream body (as produced by logsSSEHandler) into
// discrete events, splitting on blank lines and reassembling multi-line
// `data:` fields with "\n" per the SSE spec. Comment lines (starting with
// ':') are ignored.
func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()

	var events []sseEvent
	cur := sseEvent{}
	var dataLines []string
	flush := func() {
		if cur.id != "" || cur.event != "" || len(dataLines) > 0 {
			cur.data = strings.Join(dataLines, "\n")
			events = append(events, cur)
		}
		cur = sseEvent{}
		dataLines = nil
	}

	scanner := bufio.NewScanner(strings.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, ":"):
			// comment (connected / keep-alive) — ignore
		case strings.HasPrefix(line, "id: "):
			cur.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			cur.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
		default:
			t.Fatalf("parseSSE: unexpected line %q", line)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("parseSSE: scan: %v", err)
	}
	flush()
	return events
}

// setupLogsTest seeds one workflow_run + one already-terminal job (so the
// SSE handler emits `end` right after replay instead of blocking on a live
// tail, keeping this a fast, self-contained unit test), plus optionally
// some log_chunks for it. Returns a router with RegisterLogs mounted at the
// root (test requests use bare "/jobs/{id}/logs..." paths — the /api prefix
// is applied by the orchestrator's mount, not exercised here).
func setupLogsTest(t *testing.T) (r chi.Router, deps LogsDeps, jobID uuid.UUID, leaseID uuid.UUID) {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping api/logs integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	runStore := store.NewRunStore(pool)
	jobStore := store.NewJobStore(pool)
	logStore := logstore.New(pool)

	runID := uuid.New()
	run := &store.Run{
		ID:               runID,
		Repo:             "octo/example",
		Ref:              "refs/heads/main",
		SHA:              "deadbeef",
		Event:            "push",
		EventPayloadJSON: []byte(`{}`),
		TriggeredBy:      "octocat",
		Status:           "running",
	}
	if err := runStore.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		if _, err := pool.Exec(cctx, `DELETE FROM workflow_runs WHERE id = $1`, runID); err != nil {
			t.Logf("cleanup: delete run %s: %v", runID, err)
		}
	})

	jobID = uuid.New()
	leaseID = uuid.New()
	runnerID := uuid.New()
	job := &store.Job{
		ID:        jobID,
		RunID:     runID,
		JobIDYAML: "build",
		Name:      "build",
		Needs:     []string{},
		Status:    "queued",
	}
	if err := jobStore.CreateJob(ctx, job); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	deadline := time.Now().UTC().Add(time.Minute)
	if err := jobStore.Lease(ctx, jobID, leaseID, runnerID, deadline); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if err := jobStore.TransitionStatus(ctx, jobID, "leased", "running"); err != nil {
		t.Fatalf("TransitionStatus running: %v", err)
	}
	if err := jobStore.TransitionStatus(ctx, jobID, "running", "success"); err != nil {
		t.Fatalf("TransitionStatus success: %v", err)
	}
	if err := jobStore.SetResult(ctx, jobID, "success", time.Now().UTC()); err != nil {
		t.Fatalf("SetResult: %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		if _, err := pool.Exec(cctx, `DELETE FROM log_chunks WHERE lease_id = $1`, leaseID); err != nil {
			t.Logf("cleanup: delete log_chunks for lease %s: %v", leaseID, err)
		}
	})

	deps = LogsDeps{LogStore: logStore, JobStore: jobStore}
	r = chi.NewRouter()
	RegisterLogs(r, deps)
	return r, deps, jobID, leaseID
}

// seedChunk appends one log_chunks row directly (mirroring the append
// idempotency test in internal/store/run_job_step_store_test.go), then
// Publishes it so a concurrent live subscriber would also see it — not
// exercised by these tests (the job is already terminal so the handler
// never reaches the live-subscribe branch with pending data), but keeps
// the seeding path faithful to how internal/dispatch really appends.
func seedChunk(t *testing.T, deps LogsDeps, jobRowID, leaseID uuid.UUID, seq int64, data string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := logstore.Chunk{
		JobRowID: jobRowID,
		LeaseID:  leaseID,
		StepID:   "",
		Seq:      seq,
		Ts:       time.Now().UTC(),
		Data:     []byte(data),
	}
	inserted, err := deps.LogStore.Append(ctx, c)
	if err != nil {
		t.Fatalf("seedChunk: Append seq %d: %v", seq, err)
	}
	if !inserted {
		t.Fatalf("seedChunk: seq %d was a duplicate insert", seq)
	}
}

// TestLogsSSE_ReplayOrderAndEnd seeds a handful of ordered chunks for an
// already-terminal job, GETs the SSE endpoint from the beginning, and
// asserts the events arrive in order with the right `id:` seq, followed by
// a terminal `end` event.
func TestLogsSSE_ReplayOrderAndEnd(t *testing.T) {
	r, deps, jobID, leaseID := setupLogsTest(t)

	seedChunk(t, deps, jobID, leaseID, 0, "hello\n")
	seedChunk(t, deps, jobID, leaseID, 1, "multi\nline\nchunk\n")
	seedChunk(t, deps, jobID, leaseID, 2, "world\n")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s/logs", jobID), nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	events := parseSSE(t, rec.Body.String())

	var logEvents []sseEvent
	var endEvent *sseEvent
	for i := range events {
		switch events[i].event {
		case "log":
			logEvents = append(logEvents, events[i])
		case "end":
			e := events[i]
			endEvent = &e
		default:
			t.Fatalf("unexpected event type %q", events[i].event)
		}
	}

	wantData := []string{"hello", "multi\nline\nchunk", "world"}
	if len(logEvents) != len(wantData) {
		t.Fatalf("got %d log events, want %d: %+v", len(logEvents), len(wantData), logEvents)
	}
	for i, ev := range logEvents {
		if ev.id != strconv.Itoa(i) {
			t.Fatalf("log event %d: id = %q, want %q", i, ev.id, strconv.Itoa(i))
		}
		if ev.data != wantData[i] {
			t.Fatalf("log event %d: data = %q, want %q", i, ev.data, wantData[i])
		}
	}

	if endEvent == nil {
		t.Fatalf("expected a terminal `end` event, got none; events: %+v", events)
	}
	if !strings.Contains(endEvent.data, `"status":"success"`) {
		t.Fatalf("end event data = %q, want status success", endEvent.data)
	}
}

// TestLogsSSE_ResumeFromSeq verifies both resume mechanisms have no gaps
// and no duplicates against the union of a "first half" and "resumed
// second half" request, matching the DoD's disconnect/reconnect scenario.
func TestLogsSSE_ResumeFromSeq(t *testing.T) {
	r, deps, jobID, leaseID := setupLogsTest(t)

	total := []string{"a", "b", "c", "d", "e"}
	for i, d := range total {
		seedChunk(t, deps, jobID, leaseID, int64(i), d)
	}

	// Simulate: client saw seq 0..2 (cut at k=2), then reconnects with
	// Last-Event-ID: 2, expecting seq > 2 (exclusive) i.e. {3, 4}.
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s/logs", jobID), nil)
	req.Header.Set("Last-Event-ID", "2")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	resumed := parseSSE(t, rec.Body.String())

	var resumedSeqs []string
	for _, ev := range resumed {
		if ev.event == "log" {
			resumedSeqs = append(resumedSeqs, ev.data)
		}
	}
	wantResumed := []string{"d", "e"}
	if len(resumedSeqs) != len(wantResumed) {
		t.Fatalf("Last-Event-ID resume: got %v, want %v", resumedSeqs, wantResumed)
	}
	for i := range wantResumed {
		if resumedSeqs[i] != wantResumed[i] {
			t.Fatalf("Last-Event-ID resume[%d] = %q, want %q", i, resumedSeqs[i], wantResumed[i])
		}
	}

	// ?from_seq=3 is INCLUSIVE, so it must also yield {3 -> "d", 4 -> "e"}
	// — the same union member as Last-Event-ID:2, proving the two paths
	// agree on the same effective start and produce identical, gap-free,
	// dup-free coverage of the tail.
	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s/logs?from_seq=3", jobID), nil)
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)
	fromSeqEvents := parseSSE(t, rec2.Body.String())

	var fromSeqData []string
	for _, ev := range fromSeqEvents {
		if ev.event == "log" {
			fromSeqData = append(fromSeqData, ev.data)
		}
	}
	if len(fromSeqData) != len(wantResumed) {
		t.Fatalf("from_seq=3: got %v, want %v", fromSeqData, wantResumed)
	}
	for i := range wantResumed {
		if fromSeqData[i] != wantResumed[i] {
			t.Fatalf("from_seq=3[%d] = %q, want %q", i, fromSeqData[i], wantResumed[i])
		}
	}

	// Full union check: "first half" (from_seq=0, cut conceptually at
	// k=2 by only taking events with seq<=2) + "second half" (Last-Event-ID:2)
	// must equal the full ordered sequence exactly once each.
	req3 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s/logs?from_seq=0", jobID), nil)
	rec3 := httptest.NewRecorder()
	r.ServeHTTP(rec3, req3)
	firstHalfEvents := parseSSE(t, rec3.Body.String())

	var union []string
	for _, ev := range firstHalfEvents {
		if ev.event == "log" {
			seq, err := strconv.Atoi(ev.id)
			if err != nil {
				t.Fatalf("bad id %q: %v", ev.id, err)
			}
			if seq <= 2 { // pretend the connection was cut right after seq 2
				union = append(union, ev.data)
			}
		}
	}
	union = append(union, resumedSeqs...)

	if len(union) != len(total) {
		t.Fatalf("union = %v (len %d), want %v (len %d) — gap or duplicate", union, len(union), total, len(total))
	}
	for i := range total {
		if union[i] != total[i] {
			t.Fatalf("union[%d] = %q, want %q — gap or duplicate at this position", i, union[i], total[i])
		}
	}
}

// TestLogsTxt verifies GET /jobs/{id}/logs.txt returns the full
// concatenated log in seq order as text/plain.
func TestLogsTxt(t *testing.T) {
	r, deps, jobID, leaseID := setupLogsTest(t)

	seedChunk(t, deps, jobID, leaseID, 0, "hello ")
	seedChunk(t, deps, jobID, leaseID, 1, "world\n")

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s/logs.txt", jobID), nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if got, want := rec.Body.String(), "hello world\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// TestLogsUnknownJob404 verifies both endpoints 404 on an unknown job id.
func TestLogsUnknownJob404(t *testing.T) {
	r, _, _, _ := setupLogsTest(t)

	unknown := uuid.New()

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s/logs", unknown), nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("SSE: status = %d, want 404", rec.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/jobs/%s/logs.txt", unknown), nil)
	rec2 := httptest.NewRecorder()
	r.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("logs.txt: status = %d, want 404", rec2.Code)
	}
}

// TestLogsInvalidJobID400 verifies a non-UUID {id} is a 400, not a 500.
func TestLogsInvalidJobID400(t *testing.T) {
	r, _, _, _ := setupLogsTest(t)

	req := httptest.NewRequest(http.MethodGet, "/jobs/not-a-uuid/logs", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}
