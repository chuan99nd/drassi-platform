package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Runner is the domain type for a registered runner (see runners table,
// migrations/000001_create_runners.up.sql). Mode and Status are validated
// strings; the proto RunnerMode <-> mode-column mapping is the caller's
// responsibility (T-M0-05).
type Runner struct {
	ID            uuid.UUID
	Name          string
	Labels        []string
	Mode          string // "host"|"docker"|"k8s"
	Capacity      int
	Version       string
	Status        string // "online"|"draining"|"offline"
	LastHeartbeat *time.Time
	CreatedAt     time.Time
	// AuthToken is the opaque bearer credential minted at registration
	// (migrations/000002_add_runner_auth_token). Never logged.
	AuthToken string
}

// RunnerStore persists and queries runners.
type RunnerStore interface {
	// Create inserts r. The caller must set r.ID and r.AuthToken before calling.
	Create(ctx context.Context, r *Runner) error
	// UpdateHeartbeat sets last_heartbeat for the runner identified by id.
	UpdateHeartbeat(ctx context.Context, id uuid.UUID, at time.Time) error
	// SetStatus sets status for the runner identified by id.
	SetStatus(ctx context.Context, id uuid.UUID, status string) error
	// List returns all runners ordered by created_at.
	List(ctx context.Context) ([]Runner, error)
	// MarkStaleOffline sets status='offline' for every runner whose
	// last_heartbeat is before cutoff and whose status isn't already
	// 'offline'. It returns the number of rows changed.
	MarkStaleOffline(ctx context.Context, cutoff time.Time) (int, error)
	// GetByID fetches a runner by id, including its auth_token, so the caller
	// can validate a presented Bearer token. Returns pgx.ErrNoRows (wrapped)
	// when the runner does not exist.
	GetByID(ctx context.Context, id uuid.UUID) (*Runner, error)
}

// pgxRunnerStore is the pgx/v5 implementation of RunnerStore.
type pgxRunnerStore struct {
	pool *pgxpool.Pool
}

// NewRunnerStore constructs a RunnerStore backed by pool.
func NewRunnerStore(pool *pgxpool.Pool) RunnerStore {
	return &pgxRunnerStore{pool: pool}
}

func (s *pgxRunnerStore) Create(ctx context.Context, r *Runner) error {
	const q = `
		INSERT INTO runners (id, name, labels, mode, capacity, version, status, last_heartbeat, auth_token)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING created_at`

	err := s.pool.QueryRow(ctx, q,
		r.ID, r.Name, r.Labels, r.Mode, r.Capacity, r.Version, r.Status, r.LastHeartbeat, r.AuthToken,
	).Scan(&r.CreatedAt)
	if err != nil {
		return fmt.Errorf("store: create runner: %w", err)
	}
	return nil
}

func (s *pgxRunnerStore) UpdateHeartbeat(ctx context.Context, id uuid.UUID, at time.Time) error {
	const q = `UPDATE runners SET last_heartbeat = $2 WHERE id = $1`

	tag, err := s.pool.Exec(ctx, q, id, at.UTC())
	if err != nil {
		return fmt.Errorf("store: update heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: update heartbeat: %w", pgx.ErrNoRows)
	}
	return nil
}

func (s *pgxRunnerStore) SetStatus(ctx context.Context, id uuid.UUID, status string) error {
	const q = `UPDATE runners SET status = $2 WHERE id = $1`

	tag, err := s.pool.Exec(ctx, q, id, status)
	if err != nil {
		return fmt.Errorf("store: set status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: set status: %w", pgx.ErrNoRows)
	}
	return nil
}

func (s *pgxRunnerStore) List(ctx context.Context) ([]Runner, error) {
	const q = `
		SELECT id, name, labels, mode, capacity, version, status, last_heartbeat, created_at
		FROM runners
		ORDER BY created_at`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list runners: %w", err)
	}
	defer rows.Close()

	var out []Runner
	for rows.Next() {
		var r Runner
		if err := rows.Scan(
			&r.ID, &r.Name, &r.Labels, &r.Mode, &r.Capacity, &r.Version, &r.Status, &r.LastHeartbeat, &r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("store: list runners: scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list runners: %w", err)
	}
	return out, nil
}

func (s *pgxRunnerStore) MarkStaleOffline(ctx context.Context, cutoff time.Time) (int, error) {
	const q = `
		UPDATE runners
		SET status = 'offline'
		WHERE last_heartbeat < $1 AND status != 'offline'`

	tag, err := s.pool.Exec(ctx, q, cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: mark stale offline: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *pgxRunnerStore) GetByID(ctx context.Context, id uuid.UUID) (*Runner, error) {
	const q = `
		SELECT id, name, labels, mode, capacity, version, status, last_heartbeat, created_at, auth_token
		FROM runners
		WHERE id = $1`

	var r Runner
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&r.ID, &r.Name, &r.Labels, &r.Mode, &r.Capacity, &r.Version, &r.Status, &r.LastHeartbeat, &r.CreatedAt, &r.AuthToken,
	)
	if err != nil {
		return nil, fmt.Errorf("store: get runner by id: %w", err)
	}
	return &r, nil
}
