#!/usr/bin/env bash
# T-M1-12 — end-to-end verification of the Drassi north star:
# real drassi-server + real drassi-runner + real Postgres, one job flowing
# dispatcher -> runner -> host exec (act, WITHOUT_DOCKER) -> live logs -> DB.
#
# It seeds the run directly in the DB (the GitHub fetch->plan path is unit-tested
# in T-M1-05), then proves the full distributed EXECUTION path: the runner leases
# the job over gRPC, clones a real public repo, runs the step on the host, ships
# logs/status, and the orchestrator persists success. Also hits the HTTP surface
# (/healthz, /api/runs, /api/jobs/{id}/logs.txt).
#
# Usage: DATABASE_URL=... ./scripts/e2e.sh   (assumes docker compose postgres up)
set -euo pipefail

PLAT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_REPO="$(cd "$PLAT/../drassi-runner" && pwd)"
export PATH="$PATH:$(go env GOPATH)/bin"
export DRASSI_DB_DSN="${DRASSI_DB_DSN:-postgres://drassi:drassi@localhost:5432/drassi?sslmode=disable}"
export DATABASE_URL="${DATABASE_URL:-$DRASSI_DB_DSN}"
PSQL=(docker compose -f "$PLAT/docker-compose.yml" exec -T postgres psql -U drassi -d drassi -qtA)

RUNID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
JOBID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
SENTINEL="hello-drassi-$$"
LOGDIR="$(mktemp -d)"
SRV_PID=""; RNR_PID=""

cleanup() {
  [ -n "$RNR_PID" ] && kill "$RNR_PID" 2>/dev/null || true
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true
  # subshell kill above doesn't always reap the built binary child; sweep by path.
  pkill -f "$LOGDIR/drassi-server" 2>/dev/null || true
  pkill -f "$LOGDIR/drassi-runner" 2>/dev/null || true
  "${PSQL[@]}" -c "DELETE FROM workflow_runs WHERE id='$RUNID';" >/dev/null 2>&1 || true
  echo "--- server log tail ---"; tail -n 20 "$LOGDIR/server.log" 2>/dev/null || true
  echo "--- runner log tail ---"; tail -n 25 "$LOGDIR/runner.log" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> migrate + build"
migrate -path "$PLAT/migrations" -database "$DATABASE_URL" up >/dev/null 2>&1 || true
( cd "$PLAT" && go build -o "$LOGDIR/drassi-server" ./cmd/drassi-server )
( cd "$RUN_REPO" && go build -tags WITHOUT_DOCKER -o "$LOGDIR/drassi-runner" ./cmd/drassi-runner )

echo "==> resolve octocat/Hello-World master sha"
SHA="$(git ls-remote https://github.com/octocat/Hello-World.git refs/heads/master | cut -f1)"
[ -n "$SHA" ] || { echo "FAIL: could not resolve sha (network?)"; exit 1; }
echo "    sha=$SHA"

echo "==> seed run+job ($RUNID / $JOBID)"
"${PSQL[@]}" >/dev/null <<SQL
INSERT INTO workflow_runs (id, repo, ref, sha, event, triggered_by, status, workflow_yaml)
VALUES ('$RUNID','octocat/Hello-World','master','$SHA','manual','e2e','queued',
'name: e2e
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo $SENTINEL
');
INSERT INTO jobs (id, run_id, job_id_yaml, name, needs, if_expr, status)
VALUES ('$JOBID','$RUNID','build','build','{}','','queued');
SQL

echo "==> start server"
( cd "$PLAT" && "$LOGDIR/drassi-server" >"$LOGDIR/server.log" 2>&1 ) &
SRV_PID=$!
for i in $(seq 1 30); do curl -sf localhost:8080/healthz >/dev/null 2>&1 && break; sleep 0.3; done
curl -sf localhost:8080/healthz >/dev/null || { echo "FAIL: server /healthz not up"; exit 1; }
echo "    healthz ok"

echo "==> start runner (capacity 1)"
( cd "$RUN_REPO" && DRASSI_SERVER_ADDR="localhost:9090" DRASSI_RUNNER_NAME="e2e-runner" \
    DRASSI_RUNNER_LABELS="self-hosted,linux" DRASSI_RUNNER_CAPACITY=1 \
    "$LOGDIR/drassi-runner" >"$LOGDIR/runner.log" 2>&1 ) &
RNR_PID=$!

echo "==> wait for job to reach a terminal state (<=60s)"
STATUS=""
for i in $(seq 1 120); do
  STATUS="$("${PSQL[@]}" -c "SELECT status FROM jobs WHERE id='$JOBID';" | tr -d '[:space:]')"
  case "$STATUS" in success|failure|cancelled|skipped) break;; esac
  sleep 0.5
done
echo "    job status=$STATUS"

RUNSTATUS="$("${PSQL[@]}" -c "SELECT status FROM workflow_runs WHERE id='$RUNID';" | tr -d '[:space:]')"
LEASE="$("${PSQL[@]}" -c "SELECT lease_id FROM jobs WHERE id='$JOBID';" | tr -d '[:space:]')"
LOGHITS="$("${PSQL[@]}" -c "SELECT count(*) FROM log_chunks WHERE lease_id='$LEASE' AND convert_from(data,'UTF8') LIKE '%$SENTINEL%';" | tr -d '[:space:]')"
echo "    run status=$RUNSTATUS  lease=$LEASE  sentinel-log-chunks=$LOGHITS"

echo "==> HTTP surface checks"
echo "    GET /api/runs -> $(curl -s localhost:8080/api/runs | head -c 200)"
echo "    GET /api/jobs/$JOBID/logs.txt (first 200 chars):"
curl -s "localhost:8080/api/jobs/$JOBID/logs.txt" | head -c 200; echo

PASS=1
[ "$STATUS" = "success" ] || { echo "FAIL: job status != success"; PASS=0; }
[ "$RUNSTATUS" = "success" ] || { echo "FAIL: run status != success"; PASS=0; }
[ "${LOGHITS:-0}" -ge 1 ] || { echo "FAIL: sentinel '$SENTINEL' not found in shipped logs"; PASS=0; }

if [ "$PASS" = 1 ]; then echo "E2E PASS ✅"; else echo "E2E FAIL ❌"; exit 1; fi
