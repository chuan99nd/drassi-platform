import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router";
import { fetchRuns } from "../api/client";
import { isTerminalStatus } from "../api/types";
import { StatusBadge } from "../components/StatusBadge";
import { relativeTime } from "../relativeTime";
import { bodyRow, cell, errorText, headerRow, mutedText, page, table } from "../styles";

// Auto-refresh interval while any run is still in flight. Stops polling once
// every visible run has reached a terminal status (per T-M1-11 impl notes).
const POLL_MS = 2500;

export function RunsListPage() {
  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["runs"],
    queryFn: fetchRuns,
    refetchInterval: (query) => {
      const runs = query.state.data;
      if (!runs || runs.length === 0) return POLL_MS;
      return runs.some((run) => !isTerminalStatus(run.status)) ? POLL_MS : false;
    },
    refetchIntervalInBackground: true,
  });

  return (
    <div style={page}>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: 12,
        }}
      >
        <h1 style={{ fontSize: 20, margin: 0 }}>Runs</h1>
        <Link to="/runs/new">
          <button type="button">New run</button>
        </Link>
      </div>

      {isLoading && <p>Loading…</p>}
      {isError && (
        <p style={errorText}>Failed to load runs: {(error as Error).message}</p>
      )}

      {data && (
        <table style={table}>
          <thead>
            <tr style={headerRow}>
              <th style={cell}>Status</th>
              <th style={cell}>Repo</th>
              <th style={cell}>Ref</th>
              <th style={cell}>SHA</th>
              <th style={cell}>Event</th>
              <th style={cell}>Created</th>
            </tr>
          </thead>
          <tbody>
            {data.length === 0 && (
              <tr>
                <td style={cell} colSpan={6}>
                  No runs yet.{" "}
                  <Link to="/runs/new">Trigger the first one</Link>.
                </td>
              </tr>
            )}
            {data.map((run) => (
              <tr key={run.id} style={bodyRow}>
                <td style={cell}>
                  <StatusBadge status={run.status} />
                </td>
                <td style={cell}>
                  <Link to={`/runs/${run.id}`}>{run.repo}</Link>
                </td>
                <td style={cell}>{run.ref}</td>
                <td style={{ ...cell, ...mutedText, fontFamily: "monospace" }}>
                  {run.sha ? run.sha.slice(0, 7) : "—"}
                </td>
                <td style={cell}>{run.event}</td>
                <td style={cell} title={run.created_at}>
                  {relativeTime(run.created_at)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
