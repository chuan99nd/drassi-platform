-- T-M1-01: durable schema for a workflow run: the run itself, its jobs (with
-- status + lease bookkeeping), per-job outputs, per-step status, and
-- append-only log chunks. See tasks/M1/T-M1-01-data-model.md for the
-- authoritative column list and state machine.

CREATE TABLE workflow_runs (
  id                 uuid        PRIMARY KEY,
  repo               text        NOT NULL,               -- owner/name
  ref                text        NOT NULL,                -- branch/tag/ref requested
  sha                text        NOT NULL,                -- resolved commit sha
  event              text        NOT NULL,                -- push|pull_request|manual
  event_payload_json jsonb       NOT NULL DEFAULT '{}',   -- {} allowed for manual
  triggered_by       text        NOT NULL,                -- actor login / user id
  status             text        NOT NULL,                -- queued|running|success|failure|cancelled
  created_at         timestamptz NOT NULL DEFAULT now()
);

-- NOTE: intentionally NO UNIQUE(run_id, job_id_yaml) below — matrix expansion
-- (T-M4-01) creates multiple jobs rows sharing one job_id_yaml (one per
-- matrix cell), so a uniqueness constraint on that pair must not be added.
CREATE TABLE jobs (
  id                uuid        PRIMARY KEY,
  run_id            uuid        NOT NULL REFERENCES workflow_runs (id) ON DELETE CASCADE,
  job_id_yaml       text        NOT NULL,                -- key in workflow.jobs
  name              text        NOT NULL,                -- job.Name or job_id_yaml
  matrix_cell_json  jsonb,                                -- null/{} for MVP; matrix deferred to T-M4-01
  needs             text[]      NOT NULL DEFAULT '{}',   -- upstream job_id_yaml values from job.Needs()
  if_expr           text,                                 -- nullable raw job-level if: string (empty/null = always)
  status            text        NOT NULL,                -- queued|leased|running|success|failure|skipped|cancelled
  runner_id         uuid,                                 -- nullable; set on lease
  lease_id          uuid,                                 -- nullable; new value per lease
  lease_deadline    timestamptz,                          -- nullable
  result            text,                                 -- nullable; terminal result string
  started_at        timestamptz,                          -- nullable
  finished_at       timestamptz                           -- nullable
);

CREATE INDEX jobs_run_id_idx ON jobs (run_id);
CREATE INDEX jobs_status_idx ON jobs (status);
CREATE INDEX jobs_lease_deadline_leased_idx ON jobs (lease_deadline) WHERE status = 'leased';
CREATE INDEX jobs_lease_id_idx ON jobs (lease_id);

CREATE TABLE job_outputs (
  job_row_id uuid NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  key        text NOT NULL,
  value      text NOT NULL,
  PRIMARY KEY (job_row_id, key)
);

CREATE TABLE steps (
  id          uuid        PRIMARY KEY,
  job_row_id  uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  step_index  int         NOT NULL,               -- 0-based order within job
  name        text        NOT NULL,
  status      text        NOT NULL,               -- queued|running|success|failure|skipped|cancelled
  started_at  timestamptz,
  finished_at timestamptz,
  UNIQUE (job_row_id, step_index)
);

CREATE INDEX steps_job_row_id_idx ON steps (job_row_id);

CREATE TABLE log_chunks (
  lease_id uuid        NOT NULL,               -- which lease produced it
  step_id  text        NOT NULL,               -- proto step_id ("" allowed for job-level)
  seq      bigint      NOT NULL,               -- monotonic per lease
  ts       timestamptz NOT NULL,
  data     bytea       NOT NULL,
  PRIMARY KEY (lease_id, seq)                   -- makes duplicate-seq append idempotent; also covers the lease_id,seq index
);
