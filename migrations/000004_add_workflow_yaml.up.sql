-- T-M1-04: the dispatcher's AcquireJob needs the verbatim workflow YAML to
-- build JobLease.workflow_yaml (the runner re-parses it to rebuild an
-- identical model.Workflow). The planner (T-M1-02) already receives the
-- fetched YAML in PlanInput but had no column to persist it in. Simplest
-- MVP choice (per tasks/M1/T-M1-04-dispatcher-queue.md): store it once on
-- the workflow_runs row at plan time rather than per-job (M1 has exactly
-- one workflow YAML per run) or re-fetching from GitHub at lease time.
ALTER TABLE workflow_runs
  ADD COLUMN workflow_yaml text NOT NULL DEFAULT '';
