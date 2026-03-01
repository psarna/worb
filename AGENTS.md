# AGENTS.md — worb

## What is worb?

A local, single-binary Go server that's API-compatible with the W&B (Weights & Biases) Python client (`wandb`). It lets users track ML experiments locally without a cloud service. Data is stored in SQLite (default), DuckDB, or Turso.

## Project layout

```
main.go                              CLI entry point (cobra). Flags: --host, --port, --data, --db-engine
go.mod                               Module: github.com/sarna/worb, Go 1.24
tools.go                             Build tool dep (gqlgen)
Dockerfile                           Multi-stage: golang:1.24 → debian:bookworm-slim

internal/
  server/
    server.go    (328 LOC)           HTTP server, chi router, all route definitions, REST API handlers
    ui.go        (66 LOC)            HTML template rendering for web UI pages
  store/
    db.go        (350 LOC)           DB init, schema migration, engine switching (sqlite/duckdb/turso)
    runs.go      (347 LOC)           Project & Run CRUD (UpsertRun, ListRuns, GetRun, etc.)
    history.go   (354 LOC)           Metric history: insert, streaming reads (scalars, histograms)
    artifacts.go (61 LOC)            Artifact CRUD (create, commit)
  graphql/
    schema.graphqls                  GraphQL schema (wandb-compatible types & mutations)
    gqlgen.yml                       gqlgen config
    generated.go                     Auto-generated executor (DO NOT EDIT)
    models_gen.go                    Auto-generated Go types from schema (DO NOT EDIT)
    schema.resolvers.go (520 LOC)    Hand-written resolver implementations
    helpers.go   (75 LOC)            Type conversion helpers
    resolver.go  (9 LOC)             Resolver struct definition (holds *store.DB)
    context.go   (25 LOC)            Request context middleware (extracts host for upload URLs)
  filestream/
    handler.go   (135 LOC)           Handles wandb file_stream uploads (history, summary, events, logs)
  filestore/
    store.go                         File upload/download management for artifacts
  auth/
    auth.go                          Auth middleware (currently no-op, accepts all)

ui/
  embed.go                           go:embed for templates/ and static/
  templates/
    index.html   (38 LOC)            Projects listing page
    project.html (521 LOC)           Project dashboard: multi-run charts, run sidebar, category grouping
    run.html     (1018 LOC)          Run detail: charts, config, summary, logs, SQL console
  static/
    style.css                        Dark theme CSS
    worb.svg                         Logo

examples/                            Python scripts for testing with real wandb client
```

## Architecture & data flow

```
wandb Python client
  │
  ├─ GraphQL POST /graphql ──→ gqlgen ──→ schema.resolvers.go ──→ store (upsertRun, createArtifact, etc.)
  │
  └─ POST /files/{entity}/{project}/{run}/file_stream
       └─→ filestream/handler.go
            ├─ wandb-history.jsonl  → store.InsertHistory (metrics per step)
            ├─ wandb-summary.json   → store.UpsertRun (summary update)
            ├─ wandb-events.jsonl   → store.InsertSystemEvents
            └─ output.log           → store.InsertConsoleLogs

Web UI (templates + Chart.js + vanilla JS)
  │
  └─ GET /api/projects/{id}/history ──→ server.go ──→ store.StreamHistoryScalars
       (NDJSON streaming response of ProjectScalarPoint / HistogramPoint)
```

## Key route groups (server.go)

- `/graphql`, `/playground` — GraphQL API + explorer
- `/files/{entity}/{project}/{run}/file_stream` — wandb metric streaming endpoint
- `/files/upload/{token}` — artifact file upload/download
- `/api/v1/*` — wandb REST compatibility (settings, check, unapproved users)
- `/api/projects`, `/api/runs`, `/api/history`, `/api/query` — custom REST endpoints
- `/`, `/projects/{id}`, `/runs/{id}` — HTML UI pages
- `/static/*` — embedded CSS/SVG assets

## Key types

**store.DB** — wraps `*sql.DB` with Engine field ("sqlite"/"duckdb"/"turso")

**store.Run** — ID, ProjectID, Name, DisplayName, Config/Summary (json.RawMessage), State ("running"/"finished"/"crashed"), Tags, HistoryLineCount, timestamps

**store.Project** — ID, Entity, Name, CreatedAt

**store.ScalarPoint** — {Key, Step, Value, Index} — single metric data point

**store.ProjectScalarPoint** — ScalarPoint + RunID/RunName for multi-run views

**store.HistogramPoint** — {Key, Step, Bins, Values} — histogram data

**store.HistoryRow** — {RunID, Step, Data (raw JSON), Timestamp}

## Frontend

Vanilla JS + Chart.js v4 + chartjs-plugin-zoom + Hammer.js. All logic is inline in HTML templates.

Key UI features: NDJSON streaming for metric loading, multi-run comparison with color-coded lines, category grouping by metric prefix (train/, val/), synchronized zoom across charts, run visibility toggling, click-to-expand charts, SQL console with direct DB access.

## Database

SQLite by default (`~/.worb/worb.db`). Tables: `projects`, `runs`, `history`, `system_events`, `console_logs`, `artifacts`, `files`. Schema is in `store/db.go` initDB().

DuckDB and Turso support via `--db-engine` flag. Some SQL differences are handled with engine checks in store code.

## Build & run

```bash
go build .           # produces ./worb binary
./worb               # starts on localhost:8080, data in ~/.worb
./worb --port 9090 --db-engine duckdb
```

GraphQL codegen: `go run github.com/99designs/gqlgen generate` (config in `internal/graphql/gqlgen.yml`)

## Conventions

- No comments in code — just code
- Single-binary philosophy: all assets embedded via go:embed
- Store methods stream large results via callbacks rather than returning slices
- REST API streams NDJSON for large metric datasets
- The GraphQL schema mirrors wandb's API surface for client compatibility
- Entity is typically "local" for local usage
