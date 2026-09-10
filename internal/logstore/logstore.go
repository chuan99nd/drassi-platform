// Package logstore appends ordered runner log chunks to the log_chunks table
// (idempotently, keyed by (lease_id, seq)) and fans them out to any
// in-process live subscribers so the SSE tail endpoint (T-M1-07) can stream
// logs as they arrive. Pub/sub is in-process only for the single-binary MVP
// — there is no cross-process fan-out (Redis/NATS); see T-M1-06's "out of
// scope" note.
package logstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// subscriberBuffer is the channel capacity given to each Subscribe call.
// Delivery to subscribers must never block the ingest path (StreamLogs): if
// a subscriber falls behind by more than this many chunks, Publish drops it
// (closes its channel and removes it from the registry) rather than
// stalling the publisher. A dropped subscriber is expected to notice its
// channel closed, re-Subscribe, and catch up via ReplayFrom.
const subscriberBuffer = 256

// Chunk is one ordered log blob for a job.
type Chunk struct {
	JobRowID uuid.UUID
	LeaseID  uuid.UUID
	StepID   string // "" for job-level output
	Seq      int64  // monotonic per lease
	Ts       time.Time
	Data     []byte
}

// Store persists log chunks to Postgres and fans them out to live
// subscribers, keyed by the orchestrator's job row id (not lease_id, which
// changes across re-leases of the same job).
type Store struct {
	pool *pgxpool.Pool

	mu   sync.Mutex
	subs map[uuid.UUID]map[chan Chunk]struct{} // jobRowID -> live subscriber channels
}

// New constructs a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{
		pool: pool,
		subs: make(map[uuid.UUID]map[chan Chunk]struct{}),
	}
}

// Append persists c into log_chunks idempotently: a duplicate (lease_id,
// seq) — e.g. a runner retrying a chunk it already sent — is a no-op.
// inserted reports whether this call actually created a new row; callers
// must only Publish when inserted is true, so a retried duplicate is never
// re-delivered to subscribers.
func (s *Store) Append(ctx context.Context, c Chunk) (inserted bool, err error) {
	const q = `
		INSERT INTO log_chunks (lease_id, step_id, seq, ts, data)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (lease_id, seq) DO NOTHING`

	tag, err := s.pool.Exec(ctx, q, c.LeaseID, c.StepID, c.Seq, c.Ts.UTC(), c.Data)
	if err != nil {
		return false, fmt.Errorf("logstore: append: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Publish notifies live subscribers of jobRowID==c.JobRowID. Callers should
// only Publish after a successful *new* Append (inserted==true). Delivery is
// best-effort and non-blocking: a subscriber whose buffer is full is
// considered too slow, and is dropped (channel closed, removed from the
// registry) rather than stalling this call.
func (s *Store) Publish(c Chunk) {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, ok := s.subs[c.JobRowID]
	if !ok {
		return
	}
	for ch := range set {
		select {
		case ch <- c:
		default:
			delete(set, ch)
			close(ch)
		}
	}
	if len(set) == 0 {
		delete(s.subs, c.JobRowID)
	}
}

// Subscribe registers a live listener for jobRowID's chunks. The returned
// cancel func unsubscribes and closes the channel; it is safe to call
// multiple times. The channel may also be closed unilaterally by Publish if
// the subscriber falls behind (see Publish's drop policy) — callers should
// treat a closed channel the same as an explicit cancel.
func (s *Store) Subscribe(jobRowID uuid.UUID) (<-chan Chunk, func()) {
	ch := make(chan Chunk, subscriberBuffer)

	s.mu.Lock()
	set, ok := s.subs[jobRowID]
	if !ok {
		set = make(map[chan Chunk]struct{})
		s.subs[jobRowID] = set
	}
	set[ch] = struct{}{}
	s.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if set, ok := s.subs[jobRowID]; ok {
				if _, present := set[ch]; present {
					delete(set, ch)
					close(ch)
				}
				if len(set) == 0 {
					delete(s.subs, jobRowID)
				}
			}
		})
	}
	return ch, cancel
}

// ReplayFrom reads persisted chunks for jobRowID with seq >= fromSeq, in
// ascending seq order. It resolves jobRowID to lease_id via a join against
// jobs (log_chunks itself is keyed by lease_id, not job_row_id) — this is
// jobs' *current* lease_id, i.e. chunks logged under an earlier, since
// re-leased lease_id for the same job are not returned. Used by T-M1-07 for
// SSE catch-up before switching to a live Subscribe feed.
func (s *Store) ReplayFrom(ctx context.Context, jobRowID uuid.UUID, fromSeq int64) ([]Chunk, error) {
	const q = `
		SELECT lc.lease_id, lc.step_id, lc.seq, lc.ts, lc.data
		FROM log_chunks lc
		JOIN jobs j ON j.lease_id = lc.lease_id
		WHERE j.id = $1 AND lc.seq >= $2
		ORDER BY lc.seq`

	rows, err := s.pool.Query(ctx, q, jobRowID, fromSeq)
	if err != nil {
		return nil, fmt.Errorf("logstore: replay from: %w", err)
	}
	defer rows.Close()

	var out []Chunk
	for rows.Next() {
		c := Chunk{JobRowID: jobRowID}
		if err := rows.Scan(&c.LeaseID, &c.StepID, &c.Seq, &c.Ts, &c.Data); err != nil {
			return nil, fmt.Errorf("logstore: replay from: scan: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("logstore: replay from: %w", err)
	}
	return out, nil
}
