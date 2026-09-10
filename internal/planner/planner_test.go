package planner_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	actmodel "github.com/nektos/act/pkg/model"
	"gopkg.in/yaml.v3"

	"github.com/chuan99nd/drassi-platform/internal/planner"
	"github.com/chuan99nd/drassi-platform/internal/store"
)

const singleJobYAML = `
name: single
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
`

const needsTwoJobYAML = `
name: needs-two
on: push
jobs:
  A:
    runs-on: ubuntu-latest
    steps:
      - run: echo a
  B:
    needs: [A]
    if: success()
    runs-on: ubuntu-latest
    steps:
      - run: echo b
`

// matrix2x2YAML carries a 2x2 strategy.matrix (os x version) on a single
// job, expected to expand into 4 jobs rows (T-M4-01).
const matrix2x2YAML = `
name: matrix-2x2
on: push
jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        version: ["1.25", "1.26"]
    runs-on: ${{ matrix.os }}
    steps:
      - run: echo building
`

// matrixIncludeExcludeYAML carries an exclude that drops one of the 2x2
// cells and an include that adds one extra ad hoc cell, so the expected
// row count is 2*2 - 1 (exclude) + 1 (include) = 4, with the included cell
// unioning "extra" into whichever matching cell it targets (act's
// GetMatrixes semantics -- see workflow.go's commonKeysMatch2) or being
// appended as its own row if it matches nothing.
const matrixIncludeExcludeYAML = `
name: matrix-include-exclude
on: push
jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        version: ["1.25", "1.26"]
        exclude:
          - os: macos-latest
            version: "1.25"
        include:
          - os: windows-latest
            version: "1.26"
    runs-on: ${{ matrix.os }}
    steps:
      - run: echo building
`

// matrixInvalidExcludeYAML has an exclude key that does not match any
// declared matrix key -- act's GetMatrixes hard-fails this shape (unlike a
// non-matching include, which it silently drops), so this must surface as a
// plan-time ErrInvalidWorkflow with no run/job rows persisted.
const matrixInvalidExcludeYAML = `
name: matrix-invalid-exclude
on: push
jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
        exclude:
          - arch: arm64
    runs-on: ${{ matrix.os }}
    steps:
      - run: echo building
`

// bogus_top_level_key is not a valid top-level workflow property, so
// pkg/schema validation (invoked from model.ReadWorkflow) must reject this
// before it ever reaches the planning step.
const malformedYAML = `
name: malformed
on: push
bogus_top_level_key: true
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
`

// testEnv opens a real Postgres pool from DATABASE_URL, skipping the test if
// it is unset, and returns ready-to-use stores/planner plus a cleanup func.
func testEnv(t *testing.T) (p planner.Planner, pool *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping planner integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := store.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(pool.Close)

	runs := store.NewRunStore(pool)
	jobs := store.NewJobStore(pool)
	return planner.New(runs, jobs), pool
}

// cleanupRun deletes the workflow_runs row (and, via ON DELETE CASCADE, its
// jobs) so integration test runs don't accumulate rows.
func cleanupRun(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, `DELETE FROM workflow_runs WHERE id = $1`, runID); err != nil {
			t.Logf("cleanup: failed to delete test run %s: %v", runID, err)
		}
	})
}

func TestPlan_SingleJob(t *testing.T) {
	p, pool := testEnv(t)

	res, err := p.Plan(context.Background(), planner.PlanInput{
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "deadbeef",
		EventName:    "push",
		EventPayload: []byte(`{"ref":"refs/heads/main"}`),
		TriggeredBy:  "octocat",
		WorkflowYAML: []byte(singleJobYAML),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	cleanupRun(t, pool, res.RunID)

	if len(res.JobIDs) != 1 {
		t.Fatalf("expected exactly 1 job, got %d", len(res.JobIDs))
	}

	jobs := store.NewJobStore(pool)
	rows, err := jobs.ListJobsForRun(context.Background(), res.RunID)
	if err != nil {
		t.Fatalf("ListJobsForRun: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 job row, got %d", len(rows))
	}
	if rows[0].JobIDYAML != "build" || rows[0].Name != "build" {
		t.Fatalf("unexpected job row: %+v", rows[0])
	}
	if rows[0].Status != "queued" {
		t.Fatalf("expected status 'queued', got %q", rows[0].Status)
	}
	if len(rows[0].Needs) != 0 {
		t.Fatalf("expected no needs, got %v", rows[0].Needs)
	}
	if rows[0].IfExpr != nil {
		t.Fatalf("expected nil if_expr (no if: in yaml), got %q", *rows[0].IfExpr)
	}

	runs := store.NewRunStore(pool)
	gotRun, err := runs.GetRun(context.Background(), res.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if gotRun.Status != "queued" {
		t.Fatalf("expected run status 'queued', got %q", gotRun.Status)
	}
}

func TestPlan_NeedsTwoJob(t *testing.T) {
	p, pool := testEnv(t)

	res, err := p.Plan(context.Background(), planner.PlanInput{
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "cafebabe",
		EventName:    "push",
		EventPayload: []byte(`{}`),
		TriggeredBy:  "octocat",
		WorkflowYAML: []byte(needsTwoJobYAML),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	cleanupRun(t, pool, res.RunID)

	if len(res.JobIDs) != 2 {
		t.Fatalf("expected exactly 2 jobs, got %d", len(res.JobIDs))
	}

	jobs := store.NewJobStore(pool)
	rows, err := jobs.ListJobsForRun(context.Background(), res.RunID)
	if err != nil {
		t.Fatalf("ListJobsForRun: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 job rows, got %d", len(rows))
	}

	byYAMLID := map[string]*store.Job{}
	for _, r := range rows {
		byYAMLID[r.JobIDYAML] = r
	}

	a, ok := byYAMLID["A"]
	if !ok {
		t.Fatalf("missing job row for A: %+v", rows)
	}
	if len(a.Needs) != 0 {
		t.Fatalf("expected A.needs to be empty, got %v", a.Needs)
	}
	if a.IfExpr != nil {
		t.Fatalf("expected A.if_expr to be nil, got %q", *a.IfExpr)
	}

	b, ok := byYAMLID["B"]
	if !ok {
		t.Fatalf("missing job row for B: %+v", rows)
	}
	if len(b.Needs) != 1 || b.Needs[0] != "A" {
		t.Fatalf("expected B.needs == [A], got %v", b.Needs)
	}
	if b.IfExpr == nil || *b.IfExpr != "success()" {
		t.Fatalf("expected B.if_expr == %q, got %+v", "success()", b.IfExpr)
	}
}

// TestPlan_Matrix2x2 covers T-M4-01's core deliverable: a job with a 2x2
// strategy.matrix expands into exactly 4 jobs rows, each with a distinct
// matrix_cell_json, all sharing job_id_yaml and needs.
func TestPlan_Matrix2x2(t *testing.T) {
	p, pool := testEnv(t)

	res, err := p.Plan(context.Background(), planner.PlanInput{
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "deadbeef",
		EventName:    "push",
		EventPayload: []byte(`{"ref":"refs/heads/main"}`),
		TriggeredBy:  "octocat",
		WorkflowYAML: []byte(matrix2x2YAML),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	cleanupRun(t, pool, res.RunID)

	if len(res.JobIDs) != 4 {
		t.Fatalf("expected exactly 4 jobs (2x2 matrix), got %d", len(res.JobIDs))
	}

	jobs := store.NewJobStore(pool)
	rows, err := jobs.ListJobsForRun(context.Background(), res.RunID)
	if err != nil {
		t.Fatalf("ListJobsForRun: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 job rows, got %d", len(rows))
	}

	seenCellJSON := map[string]bool{}
	seenNames := map[string]bool{}
	for _, r := range rows {
		if r.JobIDYAML != "build" {
			t.Fatalf("expected every row's job_id_yaml == build, got %q", r.JobIDYAML)
		}
		if len(r.Needs) != 0 {
			t.Fatalf("expected no needs, got %v", r.Needs)
		}
		if len(r.MatrixCellJSON) == 0 {
			t.Fatalf("expected non-null matrix_cell_json for a real matrix cell, row=%+v", r)
		}
		if seenCellJSON[string(r.MatrixCellJSON)] {
			t.Fatalf("duplicate matrix_cell_json across rows: %s", r.MatrixCellJSON)
		}
		seenCellJSON[string(r.MatrixCellJSON)] = true

		if seenNames[r.Name] {
			t.Fatalf("duplicate display name across rows: %s", r.Name)
		}
		seenNames[r.Name] = true
		if r.Name == "build" || r.Status != "queued" {
			t.Fatalf("expected a per-cell display name != plain job name and status queued, got name=%q status=%q", r.Name, r.Status)
		}

		var cell map[string]string
		if err := json.Unmarshal(r.MatrixCellJSON, &cell); err != nil {
			t.Fatalf("unmarshal matrix_cell_json %s: %v", r.MatrixCellJSON, err)
		}
		if cell["os"] == "" || cell["version"] == "" {
			t.Fatalf("expected cell to carry both os and version keys, got %+v", cell)
		}
	}
	if len(seenCellJSON) != 4 {
		t.Fatalf("expected 4 distinct matrix_cell_json values, got %d", len(seenCellJSON))
	}
}

// TestPlan_NonMatrixJob_SingleRowNullCell covers the other half of T-M4-01's
// DoD: a plain (non-matrix) job still produces exactly 1 jobs row with
// matrix_cell_json null.
func TestPlan_NonMatrixJob_SingleRowNullCell(t *testing.T) {
	p, pool := testEnv(t)

	res, err := p.Plan(context.Background(), planner.PlanInput{
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "deadbeef",
		EventName:    "push",
		EventPayload: []byte(`{"ref":"refs/heads/main"}`),
		TriggeredBy:  "octocat",
		WorkflowYAML: []byte(singleJobYAML),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	cleanupRun(t, pool, res.RunID)

	jobs := store.NewJobStore(pool)
	rows, err := jobs.ListJobsForRun(context.Background(), res.RunID)
	if err != nil {
		t.Fatalf("ListJobsForRun: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 job row for a non-matrix job, got %d", len(rows))
	}
	if rows[0].MatrixCellJSON != nil {
		t.Fatalf("expected matrix_cell_json to be null for a non-matrix job, got %s", rows[0].MatrixCellJSON)
	}
	if rows[0].Name != "build" {
		t.Fatalf("expected plain job name %q for a non-matrix job, got %q", "build", rows[0].Name)
	}
}

// TestPlan_MatrixIncludeExclude exercises act's include/exclude handling
// (GetMatrixes applies both before returning cells to the planner): the
// 2x2 base matrix loses one cell to exclude and gains one ad hoc cell via
// include, netting 4 rows again but via a different combination than the
// plain 2x2 case, proving the planner defers entirely to GetMatrixes()
// rather than reimplementing any cross-product logic itself.
func TestPlan_MatrixIncludeExclude(t *testing.T) {
	p, pool := testEnv(t)

	res, err := p.Plan(context.Background(), planner.PlanInput{
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "deadbeef",
		EventName:    "push",
		EventPayload: []byte(`{"ref":"refs/heads/main"}`),
		TriggeredBy:  "octocat",
		WorkflowYAML: []byte(matrixIncludeExcludeYAML),
	})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	cleanupRun(t, pool, res.RunID)

	jobs := store.NewJobStore(pool)
	rows, err := jobs.ListJobsForRun(context.Background(), res.RunID)
	if err != nil {
		t.Fatalf("ListJobsForRun: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 job rows (2x2 - 1 exclude + 1 include), got %d", len(rows))
	}

	seenCellJSON := map[string]bool{}
	for _, r := range rows {
		if len(r.MatrixCellJSON) == 0 {
			t.Fatalf("expected non-null matrix_cell_json for a real matrix cell, row=%+v", r)
		}
		seenCellJSON[string(r.MatrixCellJSON)] = true

		var cell map[string]string
		if err := json.Unmarshal(r.MatrixCellJSON, &cell); err != nil {
			t.Fatalf("unmarshal matrix_cell_json %s: %v", r.MatrixCellJSON, err)
		}
		// The excluded cell (macos-latest, 1.25) must never appear.
		if cell["os"] == "macos-latest" && cell["version"] == "1.25" {
			t.Fatalf("excluded cell (macos-latest, 1.25) leaked into a persisted row: %+v", cell)
		}
	}
	if len(seenCellJSON) != 4 {
		t.Fatalf("expected 4 distinct matrix_cell_json values, got %d", len(seenCellJSON))
	}
}

// TestPlan_MatrixInvalidExclude_NoPartialWrites covers the task's "surface
// GetMatrixes() error as a plan-time failure ... do not create partial job
// rows" note: an exclude key that matches no declared matrix key is a hard
// failure in act's GetMatrixes, and it must abort the whole Plan call before
// any run/job row is persisted (same "no writes on failure" contract as a
// malformed workflow -- see TestPlan_MalformedYAML).
func TestPlan_MatrixInvalidExclude_NoPartialWrites(t *testing.T) {
	p, pool := testEnv(t)

	var before int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&before); err != nil {
		t.Fatalf("count workflow_runs (before): %v", err)
	}

	res, err := p.Plan(context.Background(), planner.PlanInput{
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "deadbeef",
		EventName:    "push",
		EventPayload: []byte(`{}`),
		TriggeredBy:  "octocat",
		WorkflowYAML: []byte(matrixInvalidExcludeYAML),
	})
	if err == nil {
		t.Fatalf("expected an error for an invalid matrix exclude key, got result %+v", res)
	}
	if !errors.Is(err, planner.ErrInvalidWorkflow) {
		t.Fatalf("expected err to wrap ErrInvalidWorkflow, got %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on error, got %+v", res)
	}

	var after int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&after); err != nil {
		t.Fatalf("count workflow_runs (after): %v", err)
	}
	if before != after {
		t.Fatalf("an invalid matrix must not persist any run rows: before=%d after=%d", before, after)
	}
}

func TestPlan_MalformedYAML(t *testing.T) {
	p, pool := testEnv(t)

	var before int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&before); err != nil {
		t.Fatalf("count workflow_runs (before): %v", err)
	}

	res, err := p.Plan(context.Background(), planner.PlanInput{
		Repo:         "octo/example",
		Ref:          "refs/heads/main",
		SHA:          "deadbeef",
		EventName:    "push",
		EventPayload: []byte(`{}`),
		TriggeredBy:  "octocat",
		WorkflowYAML: []byte(malformedYAML),
	})
	if err == nil {
		t.Fatalf("expected an error for malformed YAML, got result %+v", res)
	}
	if !errors.Is(err, planner.ErrInvalidWorkflow) {
		t.Fatalf("expected err to wrap ErrInvalidWorkflow, got %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on error, got %+v", res)
	}

	var after int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_runs`).Scan(&after); err != nil {
		t.Fatalf("count workflow_runs (after): %v", err)
	}
	if before != after {
		t.Fatalf("malformed workflow must not persist any run rows: before=%d after=%d", before, after)
	}
}

// TestOnDecodeNodeErrorOverride_NonFatal proves the planner package installs
// a non-fatal replacement for act's model.OnDecodeNodeError at init time.
// Upstream's default is log.Fatalf (pkg/model/workflow.go ~:750), which
// would os.Exit the whole process -- if that were still wired up, this test
// (and the rest of the binary) would never report a result at all. Importing
// "github.com/chuan99nd/drassi-platform/internal/planner" above runs its
// init(), which must have already replaced the hook by the time this test
// body executes.
func TestOnDecodeNodeErrorOverride_NonFatal(t *testing.T) {
	if actmodel.OnDecodeNodeError == nil {
		t.Fatal("model.OnDecodeNodeError is nil; planner package init did not run")
	}

	node := yaml.Node{Kind: yaml.ScalarNode, Value: "not-a-valid-decode-target"}
	var out struct{ Field int }

	done := make(chan struct{})
	go func() {
		defer close(done)
		actmodel.OnDecodeNodeError(node, out, errors.New("crafted decode error"))
	}()

	select {
	case <-done:
		// Returned normally: no log.Fatalf/os.Exit was reached.
	case <-time.After(5 * time.Second):
		t.Fatal("model.OnDecodeNodeError did not return; override may still be fatal")
	}
}
