import type { Runner } from "./types";

// The Vite dev server proxies /api/* to the Go backend (see vite.config.ts),
// so this is always a same-origin request — no CORS setup needed.
export async function fetchRunners(): Promise<Runner[]> {
  const res = await fetch("/api/runners");
  if (!res.ok) {
    throw new Error(`GET /api/runners failed: ${res.status} ${res.statusText}`);
  }
  return (await res.json()) as Runner[];
}
