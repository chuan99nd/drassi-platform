# drassi-platform web (FE)

Minimal React + TypeScript + Vite SPA for the Drassi platform. M0 scope: a single
**Runners** page that polls `GET /api/runners` every 3s and shows each runner's
online/offline/draining status.

## Stack

- React 19 + TypeScript, built with Vite.
- [`@tanstack/react-query`](https://tanstack.com/query) for polling `/api/runners`
  (`refetchInterval: 3000`).
- [`react-router`](https://reactrouter.com/) for the app shell (Runners is currently
  the only route; runs/jobs/logs pages land in M1).
- No component library — plain inline styles, kept intentionally small.

## Prerequisites

- Node 20+ (developed against Node 24).
- The Go backend (`drassi-server`) running on `localhost:8080` — see the repo root
  README / `../DEVELOPMENT.md`. In dev, requests to `/api/*` are proxied there by
  Vite (see `vite.config.ts`); there is no CORS setup on either side because the
  browser only ever talks to the Vite origin.

## Getting started

```sh
npm install
npm run dev
```

Then open the printed `http://localhost:5173` URL. With the backend running and at
least one runner registered (see the root README's `T-M0-06` runner, or seed rows
directly in Postgres), the Runners table shows each runner's status badge (green =
online, grey = offline, amber = draining), name, labels, mode, and a relative
last-heartbeat time. The table refreshes automatically every 3 seconds.

## Build

```sh
npm run build   # tsc -b && vite build, output in dist/
npm run preview # serve the production build locally
```

The built SPA is **not** wired into the Go binary in M0 — embedding/production
serving is a later concern (see the task file for T-M0-07).

## Project layout

```
src/
  main.tsx        # React root: QueryClientProvider + BrowserRouter
  App.tsx         # route table (Runners is the default/only route)
  RunnersPage.tsx # the page: react-query poll + table
  StatusBadge.tsx # color-coded status pill
  api.ts          # fetchRunners() — GET /api/runners
  types.ts        # Runner / RunnerStatus TS types matching the REST DTO
  relativeTime.ts # tiny "Xs/Xm/Xh ago" formatter (no dependency needed)
```
