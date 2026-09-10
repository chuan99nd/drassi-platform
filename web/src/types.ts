// Runner is the FE mirror of the REST DTO returned by GET /api/runners
// (see internal/api/runners.go in the Go backend and
// tasks/M0/T-M0-07-runners-api-and-fe-skeleton.md). Field names are
// snake_case to match the JSON wire shape exactly — no re-mapping layer.
export type RunnerStatus = "online" | "draining" | "offline";

export interface Runner {
  id: string;
  name: string;
  labels: string[];
  mode: string; // "host" | "docker" | "k8s"
  status: RunnerStatus;
  last_heartbeat: string | null; // ISO-8601 UTC, or null if never heartbeated
}
