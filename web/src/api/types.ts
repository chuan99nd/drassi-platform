// FE mirror of the run/job REST DTOs owned by T-M1-05 (trigger + read) and
// T-M1-07 (SSE log tail). Field names are snake_case to match the JSON wire
// shape exactly, matching the convention already used by ../types.ts
// (Runner, from T-M0-07).
//
// Contract (as coded against for T-M1-11 — confirm against the live server;
// if a field is missing/renamed once T-M1-05 / T-M1-07 land, this is the file
// to fix):
//   POST /api/runs        body {repo, ref, workflow_path} -> {run_id}      (201)
//   GET  /api/runs        -> RunSummary[]
//   GET  /api/runs/{id}   -> RunDetail   (RunSummary & {jobs: JobSummary[]})
//   GET  /api/jobs/{id}   -> JobDetail   (JobSummary & {steps: StepInfo[]})
//   GET  /api/jobs/{id}/logs      -> SSE, event `id:`=seq, `data:`=log line
//   GET  /api/jobs/{id}/logs.txt -> full historical log (plain text)

export type Status =
  | "queued"
  | "leased"
  | "running"
  | "success"
  | "failure"
  | "skipped"
  | "cancelled";

// A run's status rolls up its jobs' statuses; jobs and runs share the same
// vocabulary (see CONVENTIONS.md "act reuse cheatsheet" / Phase mapping).
export type RunStatus = Status;
export type JobStatus = Status;

const TERMINAL_STATUSES: ReadonlySet<Status> = new Set([
  "success",
  "failure",
  "skipped",
  "cancelled",
]);

// Used to decide whether react-query should keep polling (list/detail) and
// whether the log view should (re)open its EventSource.
export function isTerminalStatus(status: Status): boolean {
  return TERMINAL_STATUSES.has(status);
}

export type RunEvent = "push" | "pull_request" | "manual" | (string & {});

export interface RunSummary {
  id: string;
  repo: string;
  ref: string;
  sha: string;
  event: RunEvent;
  status: RunStatus;
  created_at: string; // RFC3339 UTC
}

export interface JobSummary {
  id: string; // jobs row id — the :id used by GET/POST /api/jobs/{id}...
  // job_id_yaml is the workflow-file job key (e.g. "build"). Multiple
  // JobSummary rows share the same job_id_yaml when the job is matrixed —
  // one row per cell (T-M4-01 planner expansion) — so the FE groups on this
  // field, not `id`, to reassemble a matrixed job's cells (T-M4-02).
  job_id_yaml: string;
  name: string;
  status: JobStatus;
  needs: string[];
  // matrix is the decoded matrix_cell_json for this row's cell (T-M4-02),
  // or null/undefined for a non-matrix job or before the platform serializes
  // this field. Values are whatever JSON the cell held (string/number/bool),
  // matching the runner's decodeMatrix contract — the FE only displays them,
  // it never re-derives or coerces them.
  matrix?: Record<string, unknown> | null;
}

export interface RunDetail extends RunSummary {
  jobs: JobSummary[];
}

export interface StepInfo {
  step_index: number;
  name: string;
  status: JobStatus;
  started_at: string | null;
  finished_at: string | null;
}

export interface JobDetail extends JobSummary {
  steps: StepInfo[];
}

export interface CreateRunRequest {
  repo: string;
  ref: string;
  workflow_path: string;
}

export interface CreateRunResponse {
  run_id: string;
}
