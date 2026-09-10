SHELL := /bin/bash

# Load .env if present so DATABASE_URL / DRASSI_DB_DSN etc. are available to targets below.
ifneq (,$(wildcard .env))
include .env
export
endif

# buf + protoc-gen-go / protoc-gen-go-grpc are installed via `go install` into $(go env
# GOPATH)/bin, which is not necessarily on PATH — prepend it for the proto target.
GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

DATABASE_URL ?= postgres://drassi:drassi@localhost:5432/drassi?sslmode=disable

.PHONY: build run proto migrate-up migrate-down dev-up dev-down

build:
	go build ./...

run:
	go run ./cmd/drassi-server

# T-M0-01's canonical `proto` target: regenerate pkg/dispatchpb from proto/dispatch/v1.
proto:
	buf generate

# golang-migrate CLI against migrations/ + $DATABASE_URL (falls back to the default DSN above
# if neither .env nor the shell environment sets it). Install the CLI with:
#   go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

# Reverts exactly one migration step. Use `migrate -path migrations -database "$(DATABASE_URL)" down`
# (no count) to tear down everything.
migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down 1

# Brings up postgres + minio and waits for postgres to report healthy.
dev-up:
	docker compose up -d postgres minio
	@echo "Waiting for postgres to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' $$(docker compose ps -q postgres) 2>/dev/null)" = "healthy" ]; do \
		sleep 1; \
	done
	@echo "postgres is healthy."

# Brings up local GitLab CE too. Slow (several minutes) — polls until healthy.
dev-up-gitlab:
	docker compose up -d postgres minio gitlab
	@echo "Waiting for gitlab to become healthy (this can take several minutes)..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' $$(docker compose ps -q gitlab) 2>/dev/null)" = "healthy" ]; do \
		sleep 5; \
	done
	@echo "gitlab is healthy at http://localhost:8929"

# Stops the stack but keeps volumes (data persists). Run `docker compose down -v` manually to
# wipe postgres/minio data.
dev-down:
	docker compose down
