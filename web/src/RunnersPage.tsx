import type { CSSProperties } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchRunners } from "./api";
import { relativeTime } from "./relativeTime";
import { StatusBadge } from "./StatusBadge";

export function RunnersPage() {
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["runners"],
    queryFn: fetchRunners,
    refetchInterval: 3000,
    // Status dashboards should keep polling even when the tab isn't focused
    // (e.g. a wallboard, or a background tab) rather than pausing, which is
    // react-query's default for interval refetches.
    refetchIntervalInBackground: true,
  });

  return (
    <div style={{ padding: 24, fontFamily: "system-ui, sans-serif" }}>
      <h1 style={{ fontSize: 20, marginBottom: 12 }}>Runners</h1>

      {isLoading && <p>Loading…</p>}
      {isError && (
        <p style={{ color: "#b91c1c" }}>
          Failed to load runners: {(error as Error).message}
        </p>
      )}

      {data && (
        <table style={{ borderCollapse: "collapse", width: "100%" }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "2px solid #e5e7eb" }}>
              <th style={cellStyle}>Status</th>
              <th style={cellStyle}>Name</th>
              <th style={cellStyle}>Labels</th>
              <th style={cellStyle}>Mode</th>
              <th style={cellStyle}>Last heartbeat</th>
            </tr>
          </thead>
          <tbody>
            {data.length === 0 && (
              <tr>
                <td style={cellStyle} colSpan={5}>
                  No runners registered yet.
                </td>
              </tr>
            )}
            {data.map((runner) => (
              <tr key={runner.id} style={{ borderBottom: "1px solid #f1f5f9" }}>
                <td style={cellStyle}>
                  <StatusBadge status={runner.status} />
                </td>
                <td style={cellStyle}>{runner.name}</td>
                <td style={cellStyle}>{runner.labels.join(", ")}</td>
                <td style={cellStyle}>{runner.mode}</td>
                <td style={cellStyle} title={runner.last_heartbeat ?? undefined}>
                  {relativeTime(runner.last_heartbeat)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

const cellStyle: CSSProperties = { padding: "8px 12px" };
