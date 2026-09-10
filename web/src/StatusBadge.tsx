import type { RunnerStatus } from "./types";

// online = green, offline = grey, draining = amber (per
// tasks/M0/T-M0-07-runners-api-and-fe-skeleton.md).
const COLORS: Record<RunnerStatus, { bg: string; fg: string }> = {
  online: { bg: "#16a34a", fg: "#ffffff" },
  draining: { bg: "#d97706", fg: "#ffffff" },
  offline: { bg: "#9ca3af", fg: "#1f2937" },
};

export function StatusBadge({ status }: { status: RunnerStatus }) {
  const colors = COLORS[status] ?? COLORS.offline;
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 600,
        textTransform: "uppercase",
        letterSpacing: 0.4,
        backgroundColor: colors.bg,
        color: colors.fg,
      }}
    >
      {status}
    </span>
  );
}
