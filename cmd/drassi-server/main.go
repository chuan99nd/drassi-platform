// Command drassi-server boots the drassi-platform orchestrator: a chi-based
// HTTP server exposing /healthz and the /api REST API (see internal/api), and
// a gRPC server exposing the (currently stubbed) dispatch.v1.Dispatch
// service. Both listeners run concurrently and shut down gracefully on
// SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/chuan99nd/drassi-platform/internal/api"
	"github.com/chuan99nd/drassi-platform/internal/config"
	"github.com/chuan99nd/drassi-platform/internal/dispatch"
	"github.com/chuan99nd/drassi-platform/internal/github"
	"github.com/chuan99nd/drassi-platform/internal/gitlab"
	"github.com/chuan99nd/drassi-platform/internal/logstore"
	"github.com/chuan99nd/drassi-platform/internal/planner"
	"github.com/chuan99nd/drassi-platform/internal/store"
	"github.com/chuan99nd/drassi-platform/internal/webhook"
	"github.com/chuan99nd/drassi-platform/pkg/dispatchpb"
)

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("drassi-server: %v", err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Dial Postgres on boot and fail fast if it's unreachable; the pool is
	// closed on shutdown below.
	dbPool, err := store.NewPool(ctx, cfg.DBDSN)
	if err != nil {
		return fmt.Errorf("connect db: %w", err)
	}
	defer dbPool.Close()
	log.Println("drassi-server: connected to postgres")

	runnerStore := store.NewRunnerStore(dbPool)
	jobStore := store.NewJobStore(dbPool)
	stepStore := store.NewStepStore(dbPool)
	runStore := store.NewRunStore(dbPool)
	jobOutputStore := store.NewJobOutputStore(dbPool)
	logStore := logstore.New(dbPool)

	dispatchService := dispatch.NewService(runnerStore, jobStore, stepStore, runStore, logStore, jobOutputStore, cfg)

	// REST dependencies (see internal/api). The dispatch service is the queue
	// notifier: POST /api/runs wakes any waiting AcquireJob stream via Notify().
	ghClient := github.New(cfg.GitHubToken, nil)
	plnr := planner.New(runStore, jobStore)

	// Commit-status reporting (T-M2-03): report success/failure back to GitHub
	// on run finalize (skipped for manual runs), and "pending" on webhook run
	// creation. One reporter shared by dispatch (finalize) + webhook (pending);
	// injected post-construction so NewService's signature is stable.
	ghReporter := github.NewCommitStatusReporter(ghClient, github.DefaultStatusContext)
	dispatchService.SetCommitStatusReporter(ghReporter)

	// Webhook config (T-M2-01/04). MVP knobs read straight from env; a real
	// GitLab base URL/token must be supplied by the operator (see CLAUDE.md).
	glClient := gitlab.New(os.Getenv("DRASSI_GITLAB_BASE_URL"), os.Getenv("DRASSI_GITLAB_TOKEN"), nil)
	httpServer := newHTTPServer(cfg.HTTPAddr, httpDeps{
		runners:     runnerStore,
		github:      ghClient,
		planner:     plnr,
		runs:        runStore,
		jobs:        jobStore,
		steps:       stepStore,
		logs:        logStore,
		notifier:    dispatchService,
		ghClient:    ghClient,
		ghReporter:  ghReporter,
		ghSecret:    os.Getenv("DRASSI_GH_WEBHOOK_SECRET"),
		ghAllowRepo: os.Getenv("DRASSI_GH_ALLOWED_REPO"),
		gitlab:      glClient,
		glSecret:    os.Getenv("DRASSI_GITLAB_WEBHOOK_SECRET"),
		glAllowProj: os.Getenv("DRASSI_GITLAB_ALLOWED_PROJECT"),
		glWorkflow:  os.Getenv("DRASSI_GITLAB_WORKFLOW_PATH"),
	})

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", cfg.GRPCAddr, err)
	}
	grpcServer := newGRPCServer(dispatchService)

	reaperCtx, stopReaper := context.WithCancel(context.Background())
	defer stopReaper()
	go dispatchService.RunReaper(reaperCtx)      // marks stale runners offline
	go dispatchService.StartJobReaper(reaperCtx) // requeues expired job leases

	errCh := make(chan error, 2)

	go func() {
		log.Printf("drassi-server: http listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
			return
		}
		errCh <- nil
	}()

	go func() {
		log.Printf("drassi-server: grpc listening on %s", cfg.GRPCAddr)
		if err := grpcServer.Serve(grpcListener); err != nil {
			errCh <- fmt.Errorf("grpc server: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		log.Println("drassi-server: shutdown signal received")
	case err := <-errCh:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("drassi-server: http shutdown error: %v", err)
	}
	grpcServer.GracefulStop()

	log.Println("drassi-server: shutdown complete")
	return nil
}

// httpDeps bundles everything the HTTP surface needs: the /api REST API plus
// the top-level /webhooks/* receivers (T-M2-01/03/04).
type httpDeps struct {
	runners  store.RunnerStore
	github   api.RunsGitHub
	planner  planner.Planner
	runs     store.RunStore
	jobs     store.JobStore
	steps    store.StepStore
	logs     *logstore.Store
	notifier api.RunsNotifier

	// Webhook wiring.
	ghClient    *github.Client // concrete: webhook fetcher needs ListWorkflows (api.RunsGitHub lacks it)
	ghReporter  *github.CommitStatusReporter
	ghSecret    string
	ghAllowRepo string
	gitlab      *gitlab.Client
	glSecret    string
	glAllowProj string
	glWorkflow  string
}

func newHTTPServer(addr string, d httpDeps) *http.Server {
	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	// REST API for the FE (web/), all mounted under /api. No CORS handling here
	// — the Vite dev server proxies /api to this server (web/vite.config.ts), so
	// browser requests are same-origin. Register runs + logs routes on the same
	// sub-router BEFORE mounting it (chi disallows adding routes post-mount).
	apiRouter := api.NewRouter(d.runners)     // GET /runners (T-M0-07)
	api.RegisterRuns(apiRouter, api.RunsDeps{ // POST /runs, GET /runs, /runs/{id}, /jobs/{id} (T-M1-05)
		GitHub:      d.github,
		Planner:     d.planner,
		Runs:        d.runs,
		Jobs:        d.jobs,
		Steps:       d.steps,
		Notifier:    d.notifier,
		TriggeredBy: "web",
	})
	api.RegisterLogs(apiRouter, api.LogsDeps{ // GET /jobs/{id}/logs (SSE), /logs.txt (T-M1-07)
		LogStore: d.logs,
		JobStore: d.jobs,
	})
	r.Mount("/api", apiRouter)

	// Webhook receivers live at the top level (/webhooks/*), not under /api —
	// they're inbound integrations, not the FE REST surface (T-M2-01/04).
	webhook.RegisterGitHubWebhook(r, webhook.GitHubWebhookDeps{
		Secret:       d.ghSecret,
		AllowRepo:    d.ghAllowRepo,
		GitHub:       d.ghClient,
		Planner:      d.planner,
		Notifier:     d.notifier,
		CommitStatus: d.ghReporter,
	})
	webhook.RegisterGitLabWebhook(r, webhook.GitLabWebhookDeps{
		Secret:       d.glSecret,
		AllowProject: d.glAllowProj,
		GitLab:       d.gitlab,
		Planner:      d.planner,
		Notifier:     d.notifier,
		WorkflowPath: d.glWorkflow,
	})

	return &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func newGRPCServer(svc dispatchpb.DispatchServer) *grpc.Server {
	s := grpc.NewServer()
	dispatchpb.RegisterDispatchServer(s, svc)
	reflection.Register(s)
	return s
}
