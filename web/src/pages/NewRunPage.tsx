import { useState, type CSSProperties, type FormEvent } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "react-router";
import { createRun } from "../api/client";
import { errorText, page } from "../styles";

export function NewRunPage() {
  const [repo, setRepo] = useState("");
  const [ref, setRef] = useState("refs/heads/main");
  const [workflowPath, setWorkflowPath] = useState(
    ".github/workflows/ci.yml",
  );
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const mutation = useMutation({
    mutationFn: createRun,
    onSuccess: (data) => {
      // The new run won't show up in a stale runs-list cache until the next
      // poll; invalidate so the list page (if visited via back button) is
      // fresh immediately.
      void queryClient.invalidateQueries({ queryKey: ["runs"] });
      navigate(`/runs/${data.run_id}`);
    },
  });

  function handleSubmit(event: FormEvent) {
    event.preventDefault();
    mutation.mutate({ repo, ref, workflow_path: workflowPath });
  }

  return (
    <div style={page}>
      <h1 style={{ fontSize: 20, marginBottom: 12 }}>New run</h1>
      <form
        onSubmit={handleSubmit}
        style={{ display: "grid", gap: 12, maxWidth: 480 }}
      >
        <label style={fieldStyle}>
          Repo
          <input
            required
            value={repo}
            onChange={(e) => setRepo(e.target.value)}
            placeholder="owner/name"
            style={inputStyle}
          />
        </label>
        <label style={fieldStyle}>
          Ref
          <input
            required
            value={ref}
            onChange={(e) => setRef(e.target.value)}
            placeholder="refs/heads/main"
            style={inputStyle}
          />
        </label>
        <label style={fieldStyle}>
          Workflow path
          <input
            required
            value={workflowPath}
            onChange={(e) => setWorkflowPath(e.target.value)}
            placeholder=".github/workflows/ci.yml"
            style={inputStyle}
          />
        </label>
        <button
          type="submit"
          disabled={mutation.isPending}
          style={{ padding: "8px 16px", width: "fit-content" }}
        >
          {mutation.isPending ? "Starting…" : "Start run"}
        </button>
        {mutation.isError && (
          <p style={errorText}>
            Failed to start run: {(mutation.error as Error).message}
          </p>
        )}
      </form>
    </div>
  );
}

const fieldStyle: CSSProperties = {
  display: "grid",
  gap: 4,
  fontSize: 13,
  fontWeight: 600,
};
const inputStyle: CSSProperties = {
  padding: "6px 8px",
  fontSize: 14,
  fontWeight: 400,
  border: "1px solid #d1d5db",
  borderRadius: 4,
};
