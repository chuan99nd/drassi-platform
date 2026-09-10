// Package config loads drassi-server runtime configuration from environment
// variables into a single Config struct. No package-level globals: callers
// obtain a Config via Load and thread it through explicitly.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration for drassi-server.
type Config struct {
	// HTTPAddr is the listen address for the chi REST server (DRASSI_HTTP_ADDR).
	HTTPAddr string
	// GRPCAddr is the listen address for the gRPC dispatch server (DRASSI_GRPC_ADDR).
	GRPCAddr string
	// DBDSN is the Postgres connection string (DRASSI_DB_DSN). It is parsed and
	// stored here but not dialed yet — store bootstrap lands in T-M0-04.
	DBDSN string
	// HeartbeatIntervalSeconds is the cadence (seconds) the server tells
	// runners to heartbeat at (DRASSI_HEARTBEAT_INTERVAL_SECONDS). Returned
	// verbatim by RegisterRunner and Heartbeat so there is one source of
	// truth for the interval.
	HeartbeatIntervalSeconds int
	// HeartbeatMissLimit is how many consecutive missed intervals the reaper
	// tolerates before flipping a runner to 'offline'
	// (DRASSI_HEARTBEAT_MISS_LIMIT). Stale cutoff = now -
	// MissLimit*IntervalSeconds.
	HeartbeatMissLimit int
	// GitHubToken is the PAT used by internal/github to call the GitHub REST
	// API (DRASSI_GITHUB_TOKEN, falling back to GITHUB_TOKEN). May be empty:
	// the github package then falls back to unauthenticated requests, which
	// work for public repos but are rate-limited. Never log this value.
	GitHubToken string
}

const (
	defaultHTTPAddr                 = ":8080"
	defaultGRPCAddr                 = ":9090"
	defaultDBDSN                    = "postgres://drassi:drassi@localhost:5432/drassi?sslmode=disable"
	defaultHeartbeatIntervalSeconds = 10
	defaultHeartbeatMissLimit       = 3
)

// Load builds a Config from DRASSI_* environment variables, falling back to
// sane defaults when unset. It returns an error if any value is malformed.
func Load() (Config, error) {
	heartbeatInterval, err := getEnvInt("DRASSI_HEARTBEAT_INTERVAL_SECONDS", defaultHeartbeatIntervalSeconds)
	if err != nil {
		return Config{}, err
	}
	missLimit, err := getEnvInt("DRASSI_HEARTBEAT_MISS_LIMIT", defaultHeartbeatMissLimit)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		HTTPAddr:                 getEnv("DRASSI_HTTP_ADDR", defaultHTTPAddr),
		GRPCAddr:                 getEnv("DRASSI_GRPC_ADDR", defaultGRPCAddr),
		DBDSN:                    getEnv("DRASSI_DB_DSN", defaultDBDSN),
		HeartbeatIntervalSeconds: heartbeatInterval,
		HeartbeatMissLimit:       missLimit,
		GitHubToken:              getEnv("DRASSI_GITHUB_TOKEN", getEnv("GITHUB_TOKEN", "")),
	}

	if cfg.HTTPAddr == "" {
		return Config{}, fmt.Errorf("config: DRASSI_HTTP_ADDR must not be empty")
	}
	if cfg.GRPCAddr == "" {
		return Config{}, fmt.Errorf("config: DRASSI_GRPC_ADDR must not be empty")
	}
	if cfg.DBDSN == "" {
		return Config{}, fmt.Errorf("config: DRASSI_DB_DSN must not be empty")
	}
	if cfg.HeartbeatIntervalSeconds <= 0 {
		return Config{}, fmt.Errorf("config: DRASSI_HEARTBEAT_INTERVAL_SECONDS must be > 0")
	}
	if cfg.HeartbeatMissLimit <= 0 {
		return Config{}, fmt.Errorf("config: DRASSI_HEARTBEAT_MISS_LIMIT must be > 0")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s must be an integer: %w", key, err)
	}
	return n, nil
}
