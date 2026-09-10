import { Fragment } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router";
import { fetchRun } from "../api/client";
import { isTerminalStatus, type JobSummary } from "../api/types";
import { StatusBadge } from "../components/StatusBadge";
import { relativeTime } from "../relativeTime";
import {
  bodyRow,
  cell,
  cellIndent,
  errorText,
  groupHeaderRow,
  headerRow,
  mutedText,
  page,
  table,
} from "../styles";

const POLL_MS = 2000;

// T-M4-02: jobs sharing job_id_yaml are the cells of one matrixed workflow
// job (one `jobs` row per cell — see T-M4-01's planner expansion). Grouping
// is a pure client-side transform over the existing run-detail jobs list;
// Map preserves first-seen order, so groups render in the same order the API
// returned their first cell.
interface JobGroup {
  jobIdYaml: string;
  name: string;
  needs: string[];
  cells: JobSummary[];
}

function groupJobs(jobs: JobSummary[]): JobGroup[] {
  const groups = new Map<string, JobGroup>();
  for (const job of jobs) {
    const key = job.job_id_yaml || job.id;
    const existing = groups.get(key);
    if (existing) {
      existing.cells.push(job);
    } else {
      groups.set(key, {
        jobIdYaml: key,
        name: job.name,
        needs: job.needs,
        cells: [job],
      });
    }
  }
  return Array.from(groups.values());
}

// Labels one matrix cell by its cell values, e.g. "build (linux, 1.22)".
// Falls back to the bare job name when there's no matrix (shouldn't be
// called in that case, but stay safe rather than throwing on an unexpected
// shape from the API).
function cellLabel(job: JobSummary): string {
  if (!job.matrix) return job.name;
  const values = Object.values(job.matrix).map(String);
  return values.length > 0 ? `${job.name} (${values.join(", ")})` : job.name;
}

export function RunDetailPage() {
  const { id } = useParams<{ id: string }>();

  const { data, isLoading, isError, error } = useQuery({
    queryKey: ["run", id],
    queryFn: () => fetchRun(id!),
    enabled: Boolean(id),
    refetchInterval: (query) => {
      const run = query.state.data;
      if (!run) return POLL_MS;
      return isTerminalStatus(run.status) ? false : POLL_MS;
    },
    refetchIntervalInBackground: true,
  });

  return (
    <div style={page}>
      <p>
        <Link to="/">&larr; All runs</Link>
      </p>

      {isLoading && <p>Loading…</p>}
      {isError && (
        <p style={errorText}>Failed to load run: {(error as Error).message}</p>
      )}

      {data && (
        <>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 12,
              marginBottom: 4,
            }}
          >
            <StatusBadge status={data.status} />
            <h1 style={{ fontSize: 20, margin: 0 }}>{data.repo}</h1>
          </div>
          <p style={mutedText}>
            ref <code>{data.ref}</code> · sha{" "}
            <code>{data.sha ? data.sha.slice(0, 12) : "—"}</code> · {data.event}{" "}
            · triggered {relativeTime(data.created_at)}
          </p>

          <h2 style={{ fontSize: 16, marginTop: 24 }}>Jobs</h2>
          <table style={table}>
            <thead>
              <tr style={headerRow}>
                <th style={cell}>Status</th>
                <th style={cell}>Job</th>
                <th style={cell}>Needs</th>
                <th style={cell}>Logs</th>
              </tr>
            </thead>
            <tbody>
              {data.jobs.length === 0 && (
                <tr>
                  <td style={cell} colSpan={4}>
                    No jobs planned for this run yet.
                  </td>
                </tr>
              )}
              {groupJobs(data.jobs).map((group) => {
                // A group is "matrixed" for display purposes once it has
                // more than one cell, or its single cell explicitly carries
                // a non-null matrix (e.g. a 1x1 matrix). Anything else
                // renders exactly as a plain job did before this change.
                const isMatrixed =
                  group.cells.length > 1 ||
                  Boolean(group.cells[0]?.matrix);

                if (!isMatrixed) {
                  const job = group.cells[0];
                  return (
                    <tr key={job.id} style={bodyRow}>
                      <td style={cell}>
                        <StatusBadge status={job.status} />
                      </td>
                      <td style={cell}>{job.name}</td>
                      <td style={{ ...cell, ...mutedText }}>
                        {job.needs.length > 0 ? job.needs.join(", ") : "—"}
                      </td>
                      <td style={cell}>
                        <Link to={`/jobs/${job.id}`} state={{ runId: data.id }}>
                          View logs
                        </Link>
                      </td>
                    </tr>
                  );
                }

                return (
                  <Fragment key={group.jobIdYaml}>
                    <tr style={groupHeaderRow}>
                      <td style={cell} />
                      <td style={cell}>
                        <strong>{group.name}</strong>{" "}
                        <span style={mutedText}>
                          ({group.cells.length} matrix cells)
                        </span>
                      </td>
                      <td style={{ ...cell, ...mutedText }}>
                        {group.needs.length > 0 ? group.needs.join(", ") : "—"}
                      </td>
                      <td style={cell} />
                    </tr>
                    {group.cells.map((job) => (
                      <tr key={job.id} style={bodyRow}>
                        <td style={cell}>
                          <StatusBadge status={job.status} />
                        </td>
                        <td style={{ ...cell, ...cellIndent }}>
                          {cellLabel(job)}
                        </td>
                        <td style={cell} />
                        <td style={cell}>
                          <Link to={`/jobs/${job.id}`} state={{ runId: data.id }}>
                            View logs
                          </Link>
                        </td>
                      </tr>
                    ))}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
