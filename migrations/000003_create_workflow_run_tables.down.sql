-- Reverse of 000003_create_workflow_run_tables.up.sql, FK-safe (drop children before parents).
DROP TABLE IF EXISTS log_chunks;
DROP TABLE IF EXISTS steps;
DROP TABLE IF EXISTS job_outputs;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS workflow_runs;
