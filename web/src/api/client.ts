import type {
  CreateRunRequest,
  CreateRunResponse,
  JobDetail,
  RunDetail,
  RunSummary,
} from "./types";

// The Vite dev server proxies /api/* to the Go backend (see vite.config.ts),
// so these are always same-origin requests — no CORS setup needed. Mirrors
// the pattern in ../api.ts (fetchRunners, T-M0-07).
async function asJson<T>(res: Response, label: string): Promise<T> {
  if (!res.ok) {
    throw new Error(`${label} failed: ${res.status} ${res.statusText}`);
  }
  return (await res.json()) as T;
}

export async function createRun(
  input: CreateRunRequest,
): Promise<CreateRunResponse> {
  const res = await fetch("/api/runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(input),
  });
  return asJson<CreateRunResponse>(res, "POST /api/runs");
}

export async function fetchRuns(): Promise<RunSummary[]> {
  const res = await fetch("/api/runs");
  // Backend wraps the list: {"runs":[...]} (T-M1-05).
  const body = await asJson<{ runs: RunSummary[] }>(res, "GET /api/runs");
  return body.runs ?? [];
}

export async function fetchRun(id: string): Promise<RunDetail> {
  const res = await fetch(`/api/runs/${encodeURIComponent(id)}`);
  return asJson<RunDetail>(res, `GET /api/runs/${id}`);
}

export async function fetchJob(id: string): Promise<JobDetail> {
  const res = await fetch(`/api/jobs/${encodeURIComponent(id)}`);
  return asJson<JobDetail>(res, `GET /api/jobs/${id}`);
}

// Path for the live SSE tail (opened directly via `new EventSource(...)` in
// the log view, not through this client) and the full-log download link.
export function jobLogsStreamUrl(id: string): string {
  return `/api/jobs/${encodeURIComponent(id)}/logs`;
}

export function jobLogsFullUrl(id: string): string {
  return `/api/jobs/${encodeURIComponent(id)}/logs.txt`;
}
