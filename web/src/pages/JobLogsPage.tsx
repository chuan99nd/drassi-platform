import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useLocation, useParams } from "react-router";
import { fetchJob, jobLogsFullUrl, jobLogsStreamUrl } from "../api/client";
import { isTerminalStatus } from "../api/types";
import { StatusBadge } from "../components/StatusBadge";
import { errorText, mutedText, page } from "../styles";

const POLL_MS = 2000;
// If the viewer's scroll position is within this many px of the bottom,
// treat them as "stuck to bottom" and keep auto-scrolling on new lines.
const STICK_THRESHOLD_PX = 32;

interface LogLine {
  seq: number;
  text: string;
}

type ConnectionState = "connecting" | "open" | "reconnecting" | "closed";

export function JobLogsPage() {
  const { id } = useParams<{ id: string }>();
  const location = useLocation();
  const runId = (location.state as { runId?: string } | null)?.runId;

  const [lines, setLines] = useState<LogLine[]>([]);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const seenSeq = useRef(new Set<number>());
  const nextSyntheticSeq = useRef(0);
  const logContainerRef = useRef<HTMLPreElement>(null);
  const stickToBottom = useRef(true);

  const { data: job, isLoading, isError, error } = useQuery({
    queryKey: ["job", id],
    queryFn: () => fetchJob(id!),
    enabled: Boolean(id),
    refetchInterval: (query) => {
      const j = query.state.data;
      if (!j) return POLL_MS;
      return isTerminalStatus(j.status) ? false : POLL_MS;
    },
    refetchIntervalInBackground: true,
  });

  const jobStatus = job?.status;
  const jobIsTerminal = jobStatus !== undefined && isTerminalStatus(jobStatus);

  // Open the live tail via the native EventSource (not react-query): it's a
  // long-lived stream, not a request/response resource. The browser handles
  // reconnect + resending `Last-Event-ID` on its own as long as the server
  // sets `id:` per event (T-M1-07); we just dedupe by seq and render in seq
  // order so a reconnect never duplicates or reorders already-shown lines.
  useEffect(() => {
    if (!id || jobIsTerminal) return;

    const source = new EventSource(jobLogsStreamUrl(id));
    setConnection("connecting");

    source.onopen = () => setConnection("open");

    // The backend emits NAMED SSE events: `event: log` per line and a final
    // `event: end` when the job is terminal (T-M1-07). onmessage only catches
    // unnamed events, so listen for "log" explicitly (keep onmessage as a
    // fallback in case framing changes).
    const onLog = (event: MessageEvent<string>) => {
      const parsedSeq = event.lastEventId ? Number(event.lastEventId) : NaN;
      const seq = Number.isFinite(parsedSeq)
        ? parsedSeq
        : nextSyntheticSeq.current++;
      if (seenSeq.current.has(seq)) return;
      seenSeq.current.add(seq);
      setLines((prev) => [...prev, { seq, text: event.data }]);
    };
    source.addEventListener("log", onLog as EventListener);
    source.onmessage = onLog;
    source.addEventListener("end", () => {
      setConnection("closed");
      source.close();
    });

    // EventSource fires onerror both for transient network hiccups (it then
    // retries automatically) and for a terminal failure; either way there's
    // nothing more to do here than reflect the state — the browser owns the
    // retry loop, honoring Last-Event-ID.
    source.onerror = () => setConnection("reconnecting");

    return () => source.close();
  }, [id, jobIsTerminal]);

  useEffect(() => {
    const el = logContainerRef.current;
    if (el && stickToBottom.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [lines]);

  function handleScroll() {
    const el = logContainerRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    stickToBottom.current = distanceFromBottom < STICK_THRESHOLD_PX;
  }

  const orderedLines = [...lines].sort((a, b) => a.seq - b.seq);

  return (
    <div style={page}>
      <p>
        <Link to={runId ? `/runs/${runId}` : "/"}>&larr; Back to run</Link>
      </p>

      {isLoading && <p>Loading…</p>}
      {isError && (
        <p style={errorText}>Failed to load job: {(error as Error).message}</p>
      )}

      {job && (
        <>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 12,
              marginBottom: 4,
            }}
          >
            <StatusBadge status={job.status} />
            <h1 style={{ fontSize: 20, margin: 0 }}>{job.name}</h1>
          </div>
          {job.needs.length > 0 && (
            <p style={mutedText}>needs: {job.needs.join(", ")}</p>
          )}

          {job.steps.length > 0 && (
            <ul style={{ listStyle: "none", padding: 0, display: "flex", gap: 8, flexWrap: "wrap" }}>
              {job.steps.map((step) => (
                <li
                  key={step.step_index}
                  style={{ display: "flex", alignItems: "center", gap: 6 }}
                >
                  <StatusBadge status={step.status} />
                  <span style={mutedText}>{step.name}</span>
                </li>
              ))}
            </ul>
          )}

          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "baseline",
              marginTop: 16,
            }}
          >
            <h2 style={{ fontSize: 16, margin: 0 }}>
              Logs{" "}
              <span style={{ ...mutedText, fontWeight: 400, fontSize: 12 }}>
                ({connectionLabel(connection, jobIsTerminal)})
              </span>
            </h2>
            {id && (
              <a href={jobLogsFullUrl(id)} target="_blank" rel="noreferrer">
                Full log
              </a>
            )}
          </div>

          <pre
            ref={logContainerRef}
            onScroll={handleScroll}
            style={{
              background: "#0b1020",
              color: "#e5e7eb",
              padding: 12,
              borderRadius: 6,
              height: 420,
              overflowY: "auto",
              fontSize: 12.5,
              lineHeight: 1.5,
              whiteSpace: "pre-wrap",
              wordBreak: "break-all",
            }}
          >
            {orderedLines.length === 0
              ? "(no log lines yet)"
              : orderedLines.map((line) => line.text).join("\n")}
          </pre>
        </>
      )}
    </div>
  );
}

function connectionLabel(state: ConnectionState, terminal: boolean): string {
  if (terminal || state === "closed") return "closed — job finished";
  if (state === "open") return "live";
  if (state === "reconnecting") return "reconnecting…";
  return "connecting…";
}
