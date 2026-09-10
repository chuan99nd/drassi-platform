// Persistent host-mode security warning. Copy follows the "Security posture
// (MVP host mode)" section of CONVENTIONS.md: untrusted repo code runs as
// the runner OS user with NO sandbox in M1 — trusted repos only, never fork
// PRs. Real isolation (docker/k8s modes) lands in M6.
export function SecurityBanner() {
  return (
    <div
      role="alert"
      style={{
        background: "#7f1d1d",
        color: "#fff",
        padding: "8px 16px",
        fontSize: 13,
        textAlign: "center",
      }}
    >
      <strong>Host mode:</strong> triggered runs execute repository code
      directly on this host with no sandbox. Only use trusted repositories
      you control — never trigger runs for fork pull requests.
    </div>
  );
}
