package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestRunnerStore_RoundTrip exercises Create -> List -> UpdateHeartbeat ->
// SetStatus -> MarkStaleOffline against a real Postgres instance. It is
// skipped unless DATABASE_URL is set (see migrations/ + `make dev-up`).
func TestRunnerStore_RoundTrip(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping RunnerStore integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	// Registered before the row-cleanup below so it runs after it: t.Cleanup
	// callbacks run in LIFO order, and the row must be deleted while the
	// pool is still open.
	t.Cleanup(pool.Close)

	rs := NewRunnerStore(pool)

	id := uuid.New()
	r := &Runner{
		ID:       id,
		Name:     "test-runner-" + id.String()[:8],
		Labels:   []string{"self-hosted", "linux"},
		Mode:     "host",
		Capacity: 2,
		Version:  "test",
		Status:   "offline",
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM runners WHERE id = $1`, id); err != nil {
			t.Logf("cleanup: failed to delete test runner %s: %v", id, err)
		}
	})

	if err := rs.Create(ctx, r); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if r.CreatedAt.IsZero() {
		t.Fatalf("Create: expected CreatedAt to be populated")
	}

	runners, err := rs.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var found *Runner
	for i := range runners {
		if runners[i].ID == id {
			found = &runners[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("List: expected to find runner %s", id)
	}
	if found.Name != r.Name || found.Mode != "host" || found.Capacity != 2 || found.Status != "offline" {
		t.Fatalf("List: unexpected runner fields: %+v", found)
	}

	heartbeatAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := rs.UpdateHeartbeat(ctx, id, heartbeatAt); err != nil {
		t.Fatalf("UpdateHeartbeat: %v", err)
	}

	if err := rs.SetStatus(ctx, id, "online"); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}

	runners, err = rs.List(ctx)
	if err != nil {
		t.Fatalf("List (after update): %v", err)
	}
	found = nil
	for i := range runners {
		if runners[i].ID == id {
			found = &runners[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("List (after update): expected to find runner %s", id)
	}
	if found.Status != "online" {
		t.Fatalf("expected status 'online', got %q", found.Status)
	}
	if found.LastHeartbeat == nil {
		t.Fatalf("expected LastHeartbeat to be set")
	}
	if !found.LastHeartbeat.Equal(heartbeatAt) {
		t.Fatalf("expected LastHeartbeat %v, got %v", heartbeatAt, *found.LastHeartbeat)
	}

	// MarkStaleOffline should flip our runner (heartbeat in the past) to
	// offline when the cutoff is in the future, and report 1 row changed.
	cutoff := heartbeatAt.Add(1 * time.Minute)
	n, err := rs.MarkStaleOffline(ctx, cutoff)
	if err != nil {
		t.Fatalf("MarkStaleOffline: %v", err)
	}
	if n != 1 {
		t.Fatalf("MarkStaleOffline: expected 1 row affected, got %d", n)
	}

	runners, err = rs.List(ctx)
	if err != nil {
		t.Fatalf("List (after MarkStaleOffline): %v", err)
	}
	found = nil
	for i := range runners {
		if runners[i].ID == id {
			found = &runners[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("List (after MarkStaleOffline): expected to find runner %s", id)
	}
	if found.Status != "offline" {
		t.Fatalf("expected status 'offline' after MarkStaleOffline, got %q", found.Status)
	}

	// A second call should now be a no-op (status already offline).
	n, err = rs.MarkStaleOffline(ctx, cutoff)
	if err != nil {
		t.Fatalf("MarkStaleOffline (second call): %v", err)
	}
	if n != 0 {
		t.Fatalf("MarkStaleOffline (second call): expected 0 rows affected, got %d", n)
	}
}
