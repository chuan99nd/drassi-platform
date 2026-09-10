# Development quickstart

This covers local dev for both repos (`drassi-platform` and `drassi-runner`, checked out as
siblings). See `../CONVENTIONS.md` (workspace) for stack decisions and repo layout.

## Prerequisites

- Go 1.26+
- Docker + the `docker compose` v2 plugin (Postgres + MinIO run in containers)
- [`buf`](https://buf.build) v1.72 (proto codegen) — `$(go env GOPATH)/bin` must be on `PATH`
  (or let `make proto` handle that for you)
- `protoc-gen-go` v1.36 and `protoc-gen-go-grpc` v1.6:
  ```sh
  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6
  ```
- The `golang-migrate` CLI (with the `postgres` build tag):
  ```sh
  go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
  ```
  (or `brew install golang-migrate`)

## 1. Configure environment

```sh
cd ~/ghq/github.com/chuan99nd/drassi-platform
cp .env.example .env
# edit .env if you need non-default ports/creds; defaults work out of the box
```

`drassi-runner` reads the same `DRASSI_*` variables (`DRASSI_SERVER_ADDR`,
`DRASSI_RUNNER_NAME`, `DRASSI_RUNNER_LABELS`, `DRASSI_RUNNER_CAPACITY`,
`DRASSI_RUNNER_VERSION`) — copy the relevant lines into a `.env` in `drassi-runner/` too, or
export them in your shell before `make run` there.

## 2. Bring up infra (Postgres + MinIO)

```sh
make dev-up      # docker compose up -d postgres minio; waits for postgres healthy
docker compose ps  # both should show as running/healthy
```

MinIO is provisioned now for M5 (artifacts/cache) and is not consumed by anything in M0/M1.

## 3. Apply migrations

```sh
make migrate-up   # migrate -path migrations -database "$DATABASE_URL" up
```

Reverse the most recent migration with `make migrate-down` (reverts exactly one step; run the
`migrate ... down` command directly with no count to tear down everything).

## 4. Generate the proto contract

```sh
make proto        # buf generate; regenerates pkg/dispatchpb from proto/dispatch/v1
```

## 5. Build and run the server

```sh
make build        # go build ./...
make run          # go run ./cmd/drassi-server (loads .env if present)
```

The server listens on `DRASSI_HTTP_ADDR` (default `:8080`, chi REST API) and
`DRASSI_GRPC_ADDR` (default `:9090`, gRPC dispatch service).

## 6. Build and run the runner

In the sibling `drassi-runner` checkout:

```sh
cd ~/ghq/github.com/chuan99nd/drassi-runner
make build        # go build -tags WITHOUT_DOCKER ./...
make run          # go run -tags WITHOUT_DOCKER ./cmd/drassi-runner
```

The runner dials `DRASSI_SERVER_ADDR` (default `localhost:9090`) to register and heartbeat.

## 7. Frontend (FE)

The React/Vite SPA lives under `web/`. It is scaffolded but not yet wired up as of M0 — see
`web/` and the relevant task (T-M0-07) for its own dev-server instructions once available.

## Tearing down

```sh
make dev-down     # docker compose down; named volumes (postgres/minio data) persist
# docker compose down -v   # wipes volumes too — use when you want a clean slate
```

## Security note (MVP host mode)

The runner executes workflow steps directly as the OS user with no sandbox. Only point it at
trusted repos, on a dedicated, disposable, least-privilege host — see `../CONVENTIONS.md`
"Security posture" for details.
