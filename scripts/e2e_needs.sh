#!/usr/bin/env bash
# M3 E2E: a 2-job DAG (produce -> consume) proving cross-runner needs+outputs.
# produce sets a job output; consume `needs: [produce]` and echoes
# ${{ needs.produce.outputs.msg }}. Verifies: consume is not leased until
# produce is terminal (gating), the JobLease for consume carries produce's
# outputs, the runner injects them, and consume's step sees the value.
set -euo pipefail

PLAT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_REPO="$(cd "$PLAT/../drassi-runner" && pwd)"
export PATH="$PATH:$(go env GOPATH)/bin"
export DRASSI_DB_DSN="${DRASSI_DB_DSN:-postgres://drassi:drassi@localhost:5432/drassi?sslmode=disable}"
export DATABASE_URL="${DATABASE_URL:-$DRASSI_DB_DSN}"
PSQL=(docker compose -f "$PLAT/docker-compose.yml" exec -T postgres psql -U drassi -d drassi -qtA)

RUNID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
J_PROD="$(uuidgen | tr '[:upper:]' '[:lower:]')"
J_CONS="$(uuidgen | tr '[:upper:]' '[:lower:]')"
MSG="msg-from-A-$$"
LOGDIR="$(mktemp -d)"; SRV_PID=""; RNR_PID=""

cleanup() {
  [ -n "$RNR_PID" ] && kill "$RNR_PID" 2>/dev/null || true
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null || true
  pkill -f "$LOGDIR/drassi-server" 2>/dev/null || true
  pkill -f "$LOGDIR/drassi-runner" 2>/dev/null || true
  "${PSQL[@]}" -c "DELETE FROM workflow_runs WHERE id='$RUNID';" >/dev/null 2>&1 || true
  echo "--- server log tail ---"; tail -n 15 "$LOGDIR/server.log" 2>/dev/null || true
}
trap cleanup EXIT

echo "==> migrate + build"
migrate -path "$PLAT/migrations" -database "$DATABASE_URL" up >/dev/null 2>&1 || true
( cd "$PLAT" && go build -o "$LOGDIR/drassi-server" ./cmd/drassi-server )
( cd "$RUN_REPO" && go build -tags WITHOUT_DOCKER -o "$LOGDIR/drassi-runner" ./cmd/drassi-runner )

SHA="$(git ls-remote https://github.com/octocat/Hello-World.git refs/heads/master | cut -f1)"

echo "==> seed 2-job DAG (produce -> consume)"
"${PSQL[@]}" >/dev/null <<SQL
INSERT INTO workflow_runs (id,repo,ref,sha,event,triggered_by,status,workflow_yaml)
VALUES ('$RUNID','octocat/Hello-World','master','$SHA','manual','e2e','queued',
'name: needs-demo
on: push
jobs:
  produce:
    runs-on: ubuntu-latest
    outputs:
      msg: \${{ steps.s.outputs.msg }}
    steps:
      - id: s
        run: echo "msg=$MSG" >> \$GITHUB_OUTPUT
  consume:
    runs-on: ubuntu-latest
    needs: [produce]
    steps:
      - run: echo "consume-got:\${{ needs.produce.outputs.msg }}"
');
INSERT INTO jobs (id,run_id,job_id_yaml,name,needs,if_expr,status)
VALUES ('$J_PROD','$RUNID','produce','produce','{}','','queued'),
       ('$J_CONS','$RUNID','consume','consume','{produce}','','queued');
SQL

echo "==> start server + runner (capacity 2)"
( cd "$PLAT" && "$LOGDIR/drassi-server" >"$LOGDIR/server.log" 2>&1 ) & SRV_PID=$!
for i in $(seq 1 30); do curl -sf localhost:8080/healthz >/dev/null 2>&1 && break; sleep 0.3; done
( cd "$RUN_REPO" && DRASSI_SERVER_ADDR="localhost:9090" DRASSI_RUNNER_NAME="e2e-needs" \
    DRASSI_RUNNER_LABELS="self-hosted,linux" DRASSI_RUNNER_CAPACITY=2 \
    "$LOGDIR/drassi-runner" >"$LOGDIR/runner.log" 2>&1 ) & RNR_PID=$!

echo "==> wait for both jobs terminal (<=60s)"
for i in $(seq 1 120); do
  PS="$("${PSQL[@]}" -c "SELECT status FROM jobs WHERE id='$J_PROD';" | tr -d '[:space:]')"
  CS="$("${PSQL[@]}" -c "SELECT status FROM jobs WHERE id='$J_CONS';" | tr -d '[:space:]')"
  case "$CS" in success|failure|cancelled|skipped) break;; esac
  sleep 0.5
done
RS="$("${PSQL[@]}" -c "SELECT status FROM workflow_runs WHERE id='$RUNID';" | tr -d '[:space:]')"
echo "    produce=$PS consume=$CS run=$RS"

echo "==> consume job log:"
curl -s "localhost:8080/api/jobs/$J_CONS/logs.txt" | grep -i 'consume-got' || echo "(no consume-got line)"
CONSUME_LOG="$(curl -s "localhost:8080/api/jobs/$J_CONS/logs.txt")"

PASS=1
[ "$PS" = "success" ] || { echo "FAIL: produce != success"; PASS=0; }
[ "$CS" = "success" ] || { echo "FAIL: consume != success"; PASS=0; }
[ "$RS" = "success" ] || { echo "FAIL: run != success"; PASS=0; }
echo "$CONSUME_LOG" | grep -q "consume-got:$MSG" || { echo "FAIL: consume did not see produce's output '$MSG'"; PASS=0; }

if [ "$PASS" = 1 ]; then echo "M3 NEEDS E2E PASS ✅ (cross-job output '$MSG' flowed produce->consume)"; else echo "M3 NEEDS E2E FAIL ❌"; exit 1; fi
