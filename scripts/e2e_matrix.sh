#!/usr/bin/env bash
# M4 E2E: one matrix job "build" with two cells (os=linux, os=mac) executes as
# two job rows, each interpolating its own ${{ matrix.os }} on the runner.
# (Planner expansion is unit-tested; here we seed the two expanded cells and
# prove the dispatcher->runner->rc.Matrix path runs both with distinct values.)
set -euo pipefail

PLAT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_REPO="$(cd "$PLAT/../drassi-runner" && pwd)"
export PATH="$PATH:$(go env GOPATH)/bin"
export DRASSI_DB_DSN="${DRASSI_DB_DSN:-postgres://drassi:drassi@localhost:5432/drassi?sslmode=disable}"
export DATABASE_URL="${DATABASE_URL:-$DRASSI_DB_DSN}"
PSQL=(docker compose -f "$PLAT/docker-compose.yml" exec -T postgres psql -U drassi -d drassi -qtA)

RUNID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
J1="$(uuidgen | tr '[:upper:]' '[:lower:]')"; J2="$(uuidgen | tr '[:upper:]' '[:lower:]')"
LOGDIR="$(mktemp -d)"; SRV_PID=""; RNR_PID=""
cleanup() {
  [ -n "$RNR_PID" ] && kill "$RNR_PID" 2>/dev/null || true
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true
  pkill -f "$LOGDIR/drassi-server" 2>/dev/null || true
  pkill -f "$LOGDIR/drassi-runner" 2>/dev/null || true
  "${PSQL[@]}" -c "DELETE FROM workflow_runs WHERE id='$RUNID';" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> build + migrate"
migrate -path "$PLAT/migrations" -database "$DATABASE_URL" up >/dev/null 2>&1 || true
( cd "$PLAT" && go build -o "$LOGDIR/drassi-server" ./cmd/drassi-server )
( cd "$RUN_REPO" && go build -tags WITHOUT_DOCKER -o "$LOGDIR/drassi-runner" ./cmd/drassi-runner )
SHA="$(git ls-remote https://github.com/octocat/Hello-World.git refs/heads/master | cut -f1)"

echo "==> seed matrix run (build: os=[linux,mac])"
"${PSQL[@]}" >/dev/null <<SQL
INSERT INTO workflow_runs (id,repo,ref,sha,event,triggered_by,status,workflow_yaml)
VALUES ('$RUNID','octocat/Hello-World','master','$SHA','manual','e2e','queued',
'name: matrix-demo
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [linux, mac]
    steps:
      - run: echo "matrix-cell:\${{ matrix.os }}"
');
INSERT INTO jobs (id,run_id,job_id_yaml,name,matrix_cell_json,needs,if_expr,status)
VALUES ('$J1','$RUNID','build','build (linux)','{"os":"linux"}','{}','','queued'),
       ('$J2','$RUNID','build','build (mac)','{"os":"mac"}','{}','','queued');
SQL

echo "==> start server + runner (capacity 2)"
( cd "$PLAT" && "$LOGDIR/drassi-server" >"$LOGDIR/server.log" 2>&1 ) & SRV_PID=$!
for i in $(seq 1 30); do curl -sf localhost:8080/healthz >/dev/null 2>&1 && break; sleep 0.3; done
( cd "$RUN_REPO" && DRASSI_SERVER_ADDR="localhost:9090" DRASSI_RUNNER_NAME="e2e-matrix" \
    DRASSI_RUNNER_LABELS="self-hosted,linux" DRASSI_RUNNER_CAPACITY=2 \
    "$LOGDIR/drassi-runner" >"$LOGDIR/runner.log" 2>&1 ) & RNR_PID=$!

echo "==> wait for both cells terminal (<=60s)"
for i in $(seq 1 120); do
  S1="$("${PSQL[@]}" -c "SELECT status FROM jobs WHERE id='$J1';" | tr -d '[:space:]')"
  S2="$("${PSQL[@]}" -c "SELECT status FROM jobs WHERE id='$J2';" | tr -d '[:space:]')"
  case "$S1" in success|failure|cancelled|skipped) case "$S2" in success|failure|cancelled|skipped) break;; esac;; esac
  sleep 0.5
done
RS="$("${PSQL[@]}" -c "SELECT status FROM workflow_runs WHERE id='$RUNID';" | tr -d '[:space:]')"
echo "    cell1(linux)=$S1 cell2(mac)=$S2 run=$RS"
L1="$(curl -s "localhost:8080/api/jobs/$J1/logs.txt")"
L2="$(curl -s "localhost:8080/api/jobs/$J2/logs.txt")"
echo "    cell1 log: $(echo "$L1" | grep -o 'matrix-cell:[a-z]*' | head -1)"
echo "    cell2 log: $(echo "$L2" | grep -o 'matrix-cell:[a-z]*' | head -1)"
echo "    API matrix field (run detail):" && curl -s "localhost:8080/api/runs/$RUNID" | grep -o '"matrix":{[^}]*}' | head -2

PASS=1
[ "$S1" = success ] && [ "$S2" = success ] && [ "$RS" = success ] || { echo "FAIL: not all success"; PASS=0; }
echo "$L1" | grep -q "matrix-cell:linux" || { echo "FAIL: cell1 missing matrix-cell:linux"; PASS=0; }
echo "$L2" | grep -q "matrix-cell:mac"   || { echo "FAIL: cell2 missing matrix-cell:mac"; PASS=0; }
if [ "$PASS" = 1 ]; then echo "M4 MATRIX E2E PASS ✅ (two cells ran with distinct matrix.os values)"; else echo "M4 MATRIX E2E FAIL ❌"; exit 1; fi
