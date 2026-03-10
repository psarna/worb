"""
Migration orchestration server for worb.

Exposes a REST API for scheduling, monitoring, and controlling migrations
from Weights & Biases to worb. Runs a background worker thread that
processes migration tasks with resume support and WAL backpressure.

Usage:
    uv run migration_server.py --worb-url http://localhost:8080
    uv run migration_server.py  # uses defaults from env vars / .env
"""

import json
import logging
import sys
import threading
import time
import traceback
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from state_db import StateDB
from wandb_client import (
    fetch_console_logs,
    fetch_scan_history,
    fetch_system_events,
    get_run_summary,
    make_wandb_api,
    retry,
)
from worb_client import (
    FILESTREAM_BATCH_SIZE,
    get_wal_stats,
    make_session,
    send_filestream,
    upsert_run_graphql,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("migration_server")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    worb_url: str = "http://localhost:8080"
    state_db: str = "~/.worb/migration.db"
    wal_lag_soft: int = 1_073_741_824   # 1 GB — start slowing down
    wal_lag_hard: int = 4_294_967_296   # 4 GB — full stop
    wal_lag_max_delay: float = 30.0     # max sleep seconds at hard limit
    max_errors: int = 3
    wandb_url: str = "https://api.wandb.ai"
    wandb_timeout: int = 300
    port: int = 9090
    host: str = "0.0.0.0"

    model_config = SettingsConfigDict(env_prefix="MIGRATE_")


settings = Settings()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class MigrateRequest(BaseModel):
    entity: str
    project: str | None = None


class ResetRequest(BaseModel):
    entity: str
    project: str
    run_id: str | None = None
    force: bool = False  # reset done runs too, not just errors


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

class Worker:
    def __init__(self, db: StateDB):
        self.db = db
        self._paused = threading.Event()
        self._stopped = threading.Event()
        self._state = "idle"  # idle | running | paused
        self._thread: threading.Thread | None = None

    @property
    def state(self) -> str:
        return self._state

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stopped.set()
        if self._thread:
            self._thread.join(timeout=10)

    def pause(self):
        self._paused.set()
        self._state = "paused"

    def resume(self):
        self._paused.clear()
        self._state = "running"

    def _wait_if_paused(self):
        while self._paused.is_set() and not self._stopped.is_set():
            self._state = "paused"
            time.sleep(1)

    def _wal_backpressure(self):
        """Sleep proportionally to WAL lag. No-op when lag < soft limit.

        Between soft and hard limits, delay scales linearly from 0 to
        wal_lag_max_delay seconds — same principle as worb's own WAL
        writer backpressure (wal.go:Append).
        """
        stats = get_wal_stats(settings.worb_url)
        if not stats:
            return
        lag = stats.get("lag_bytes", 0)
        if lag <= settings.wal_lag_soft:
            return
        if lag >= settings.wal_lag_hard:
            log.info("WAL lag %d bytes >= hard limit %d, sleeping %.0fs",
                     lag, settings.wal_lag_hard, settings.wal_lag_max_delay)
            self._interruptible_sleep(settings.wal_lag_max_delay)
            return
        # Linear interpolation between soft and hard
        fraction = (lag - settings.wal_lag_soft) / (settings.wal_lag_hard - settings.wal_lag_soft)
        delay = fraction * settings.wal_lag_max_delay
        log.info("WAL lag %d bytes (%.0f%% of range), sleeping %.1fs",
                 lag, fraction * 100, delay)
        self._interruptible_sleep(delay)

    def _interruptible_sleep(self, seconds: float):
        """Sleep in small increments so stop/pause remain responsive."""
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if self._stopped.is_set() or self._paused.is_set():
                return
            time.sleep(min(0.5, end - time.monotonic()))

    def _run(self):
        log.info("Worker started")
        while not self._stopped.is_set():
            self._wait_if_paused()
            if self._stopped.is_set():
                break

            self._state = "running"

            try:
                did_work = self._tick()
            except Exception:
                log.error("Worker tick error:\n%s", traceback.format_exc())
                did_work = False

            if not did_work:
                self._state = "idle"
                # Sleep in small increments so we can respond to stop/pause
                for _ in range(50):  # 5 seconds total
                    if self._stopped.is_set() or self._paused.is_set():
                        break
                    time.sleep(0.1)

        self._state = "idle"
        log.info("Worker stopped")

    def _tick(self) -> bool:
        """Do one unit of work. Returns True if work was done."""
        # 1. Discover runs for a pending project
        project = self.db.get_next_project(status="pending")
        if project:
            self._discover_project(project)
            return True

        # 2. WAL backpressure (proportional delay)
        self._wal_backpressure()

        # 3. Pick next run to migrate
        run = self.db.get_next_run(max_errors=settings.max_errors)
        if run:
            self._migrate_run(run)
            return True

        return False

    def _discover_project(self, project: dict):
        log.info("Discovering runs for project %s (id=%d)", project["project_name"], project["id"])
        self.db.update_project_status(project["id"], "discovering")

        try:
            # Get source info
            src = self.db.conn.execute(
                "SELECT * FROM migration_sources WHERE id = ?", (project["source_id"],)
            ).fetchone()
            wandb_url = src["wandb_url"]
            entity = src["entity"]

            api = make_wandb_api(
                wandb_url=wandb_url if wandb_url != "https://api.wandb.ai" else None,
                timeout=settings.wandb_timeout,
            )
            runs = retry(
                lambda: api.runs(f"{entity}/{project['project_name']}", order="+created_at"),
                "list runs",
            )
            run_list = list(runs)

            for r in run_list:
                self.db.upsert_migration_run(
                    project["id"],
                    wandb_run_id=r.id,
                    name=r.name,
                    state=r.state,
                )

            self.db.update_project_status(project["id"], "ready", total_runs=len(run_list))
            log.info("Project %s: %d runs discovered", project["project_name"], len(run_list))

        except Exception as e:
            log.error("Discovery failed for project %s: %s", project["project_name"], e)
            self.db.update_project_status(project["id"], "error", error=str(e))

    def _migrate_run(self, run: dict):
        run_id = run["id"]
        log.info(
            "Migrating run %s (%s) from %s/%s",
            run["wandb_run_id"], run["wandb_run_name"],
            run["entity"], run["project_name"],
        )
        self.db.update_run_status(run_id, "in_progress")

        try:
            wandb_url = run["wandb_url"]
            entity = run["entity"]
            project_name = run["project_name"]

            api = make_wandb_api(
                wandb_url=wandb_url if wandb_url != "https://api.wandb.ai" else None,
                timeout=settings.wandb_timeout,
            )
            wandb_run = retry(
                lambda: api.run(f"{entity}/{project_name}/{run['wandb_run_id']}"),
                "fetch run",
            )

            session = make_session(settings.worb_url, entity)

            # a. Upsert
            if not run["upserted"]:
                upsert_run_graphql(session, settings.worb_url, entity, project_name, wandb_run)
                self.db.mark_upserted(run_id)
                log.info("  upserted run %s", run["wandb_run_id"])

            self._wait_if_paused()
            if self._stopped.is_set():
                return

            # b. History (resumable)
            if not run["history_done"]:
                self._migrate_history(session, entity, project_name, wandb_run, run)
                self.db.mark_history_done(run_id)
                log.info("  history done for %s", run["wandb_run_id"])

            self._wait_if_paused()
            if self._stopped.is_set():
                return

            # c. Summary
            if not run["summary_done"]:
                summary = get_run_summary(wandb_run)
                if summary:
                    send_filestream(
                        session, settings.worb_url, entity, project_name, wandb_run.id,
                        files={
                            "wandb-summary.json": {
                                "offset": 0,
                                "content": [json.dumps(summary, default=str)],
                            }
                        },
                    )
                self.db.mark_summary_done(run_id)
                log.info("  summary done for %s", run["wandb_run_id"])

            # d. Logs
            if not run["logs_done"]:
                lines = fetch_console_logs(wandb_run)
                if lines:
                    for i in range(0, len(lines), FILESTREAM_BATCH_SIZE):
                        chunk = lines[i : i + FILESTREAM_BATCH_SIZE]
                        send_filestream(
                            session, settings.worb_url, entity, project_name, wandb_run.id,
                            files={"output.log": {"offset": i, "content": chunk}},
                        )
                self.db.mark_logs_done(run_id)
                log.info("  logs done for %s", run["wandb_run_id"])

            # e. Events
            if not run["events_done"]:
                event_lines = fetch_system_events(wandb_run)
                if event_lines:
                    for i in range(0, len(event_lines), FILESTREAM_BATCH_SIZE):
                        chunk = event_lines[i : i + FILESTREAM_BATCH_SIZE]
                        send_filestream(
                            session, settings.worb_url, entity, project_name, wandb_run.id,
                            files={"wandb-events.jsonl": {"offset": i, "content": chunk}},
                        )
                self.db.mark_events_done(run_id)
                log.info("  events done for %s", run["wandb_run_id"])

            # f. Complete signal
            if not run["completed"]:
                if wandb_run.state == "finished":
                    send_filestream(
                        session, settings.worb_url, entity, project_name, wandb_run.id,
                        files={}, complete=True,
                    )
                self.db.mark_completed(run_id)

            # g. Mark done
            self.db.update_run_status(run_id, "done")
            self.db.check_project_done(run["project_id"])
            log.info("  run %s done", run["wandb_run_id"])

        except Exception as e:
            log.error("Error migrating run %s: %s", run["wandb_run_id"], e)
            log.error(traceback.format_exc())
            self.db.record_run_error(run_id, str(e))

    def _migrate_history(self, session, entity, project_name, wandb_run, run: dict):
        """Migrate history with resume support."""
        merged = fetch_scan_history(wandb_run)
        if not merged:
            return

        already_sent = run["history_lines_sent"]
        if already_sent > 0:
            log.info("  resuming history from line %d/%d", already_sent, len(merged))

        offset = already_sent
        for i in range(already_sent, len(merged), FILESTREAM_BATCH_SIZE):
            self._wait_if_paused()
            if self._stopped.is_set():
                return

            # WAL backpressure between batches
            self._wal_backpressure()

            batch = [json.dumps(row, default=str) for row in merged[i : i + FILESTREAM_BATCH_SIZE]]
            send_filestream(
                session, settings.worb_url, entity, project_name, wandb_run.id,
                files={
                    "wandb-history.jsonl": {
                        "offset": offset,
                        "content": batch,
                    }
                },
            )
            offset += len(batch)
            self.db.update_history_lines_sent(run["id"], offset)


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

state_db: StateDB
worker: Worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    global state_db, worker
    state_db = StateDB(settings.state_db)
    worker = Worker(state_db)
    worker.start()
    log.info("Migration server started (worb_url=%s, state_db=%s)", settings.worb_url, settings.state_db)
    yield
    worker.stop()
    state_db.close()
    log.info("Migration server stopped")


app = FastAPI(title="worb migration server", lifespan=lifespan)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.post("/api/migrate")
def schedule_migration(req: MigrateRequest):
    source_id = state_db.upsert_source(settings.wandb_url, req.entity)

    if req.project:
        # Single project
        state_db.add_project(source_id, req.project)
        return {"source_id": source_id, "projects_scheduled": 1}
    else:
        # Discover all projects
        api = make_wandb_api(
            wandb_url=settings.wandb_url if settings.wandb_url != "https://api.wandb.ai" else None,
            timeout=settings.wandb_timeout,
        )
        try:
            projects = [p.name for p in api.projects(entity=req.entity)]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to list projects: {e}")

        for name in projects:
            state_db.add_project(source_id, name)

        return {"source_id": source_id, "projects_scheduled": len(projects)}


@app.get("/api/status")
def get_status():
    wal = get_wal_stats(settings.worb_url)
    return {
        "sources": state_db.get_status(),
        "wal_lag_bytes": wal["lag_bytes"] if wal else None,
        "worker_state": worker.state,
    }


@app.get("/api/projects/{project_id}/runs")
def get_project_runs(project_id: int):
    return state_db.get_project_runs(project_id)


@app.post("/api/reset")
def reset_runs(req: ResetRequest):
    if req.run_id:
        run = state_db.find_run(req.entity, req.project, req.run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        state_db.reset_run(run["id"])
        return {"reset": "run", "wandb_run_id": req.run_id}
    else:
        proj = state_db.find_project(req.entity, req.project)
        if not proj:
            raise HTTPException(status_code=404, detail="Project not found")
        if req.force:
            state_db.reset_all_runs(proj["id"])
        else:
            state_db.reset_failed_runs(proj["id"])
        if proj["status"] in ("error", "done"):
            state_db.reset_project(proj["id"])
        return {"reset": "project", "project": req.project, "force": req.force}


@app.post("/api/worker/pause")
def pause_worker():
    worker.pause()
    return {"worker_state": worker.state}


@app.post("/api/worker/resume")
def resume_worker():
    worker.resume()
    return {"worker_state": worker.state}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "migration_server:app",
        host=settings.host,
        port=settings.port,
        log_level="info",
    )
