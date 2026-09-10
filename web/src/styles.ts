import type { CSSProperties } from "react";

// Small set of shared inline-style objects so the run/job pages don't each
// redefine the same layout primitives. Kept intentionally tiny — no CSS
// framework per CONVENTIONS.md ("keep it minimal").
export const page: CSSProperties = {
  padding: 24,
  fontFamily: "system-ui, sans-serif",
  maxWidth: 1000,
  margin: "0 auto",
};

export const cell: CSSProperties = { padding: "8px 12px" };

export const errorText: CSSProperties = { color: "#b91c1c" };

export const mutedText: CSSProperties = { color: "#6b7280" };

export const table: CSSProperties = { borderCollapse: "collapse", width: "100%" };

export const headerRow: CSSProperties = {
  textAlign: "left",
  borderBottom: "2px solid #e5e7eb",
};

export const bodyRow: CSSProperties = { borderBottom: "1px solid #f1f5f9" };

// Matrix job group header row (T-M4-02): the parent row for a job that
// expanded into >1 matrix cell. No status badge of its own — each cell below
// carries its own status — so it's visually lighter than a normal bodyRow.
export const groupHeaderRow: CSSProperties = {
  borderBottom: "1px solid #f1f5f9",
  backgroundColor: "#f8fafc",
};

// Indents a matrix cell's row under its group header so the nesting reads
// clearly without a real tree widget.
export const cellIndent: CSSProperties = { paddingLeft: 28 };
