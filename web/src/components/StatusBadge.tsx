import type { CSSProperties } from "react";
import type { Status } from "../api/types";

// Badge for run/job/step statuses (the shared "queued|leased|running|
// success|failure|skipped|cancelled" vocabulary — see api/types.ts). Sibling
// to ../StatusBadge.tsx, which covers the separate RunnerStatus vocabulary
// ("online|draining|offline") from T-M0-07 — kept as two small components
// rather than one generic one since the color mappings and status sets don't
// overlap.
const COLORS: Record<Status, { bg: string; fg: string }> = {
  queued: { bg: "#9ca3af", fg: "#1f2937" },
  leased: { bg: "#6366f1", fg: "#ffffff" },
  running: { bg: "#2563eb", fg: "#ffffff" },
  success: { bg: "#16a34a", fg: "#ffffff" },
  failure: { bg: "#dc2626", fg: "#ffffff" },
  skipped: { bg: "#d97706", fg: "#ffffff" },
  cancelled: { bg: "#4b5563", fg: "#ffffff" },
};

export function StatusBadge({ status }: { status: Status }) {
  const colors = COLORS[status] ?? { bg: "#9ca3af", fg: "#1f2937" };
  const style: CSSProperties = {
    display: "inline-block",
    padding: "2px 10px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: 600,
    textTransform: "uppercase",
    letterSpacing: 0.4,
    backgroundColor: colors.bg,
    color: colors.fg,
  };
  return <span style={style}>{status}</span>;
}
