// Minimal relative-time formatter (no dependency needed for a handful of
// units). Returns e.g. "5s ago", "3m ago", "2h ago"; "never" for null.
export function relativeTime(iso: string | null): string {
  if (!iso) return "never";

  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "unknown";

  const deltaSeconds = Math.max(0, Math.round((Date.now() - then) / 1000));

  if (deltaSeconds < 5) return "just now";
  if (deltaSeconds < 60) return `${deltaSeconds}s ago`;

  const deltaMinutes = Math.round(deltaSeconds / 60);
  if (deltaMinutes < 60) return `${deltaMinutes}m ago`;

  const deltaHours = Math.round(deltaMinutes / 60);
  if (deltaHours < 24) return `${deltaHours}h ago`;

  const deltaDays = Math.round(deltaHours / 24);
  return `${deltaDays}d ago`;
}
