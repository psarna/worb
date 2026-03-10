"""Migration state database for tracking progress across restarts."""

import sqlite3
from pathlib import Path


class StateDB:
    def __init__(self, path: str):
        p = Path(path).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(p), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=wal")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def _migrate(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS migration_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wandb_url TEXT NOT NULL,
                entity TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(wandb_url, entity)
            );

            CREATE TABLE IF NOT EXISTS migration_projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER NOT NULL REFERENCES migration_sources(id),
                project_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                total_runs INTEGER,
                error_message TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(source_id, project_name)
            );

            CREATE TABLE IF NOT EXISTS migration_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL REFERENCES migration_projects(id),
                wandb_run_id TEXT NOT NULL,
                wandb_run_name TEXT,
                wandb_run_state TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                upserted INTEGER NOT NULL DEFAULT 0,
                history_lines_sent INTEGER NOT NULL DEFAULT 0,
                history_done INTEGER NOT NULL DEFAULT 0,
                summary_done INTEGER NOT NULL DEFAULT 0,
                events_done INTEGER NOT NULL DEFAULT 0,
                logs_done INTEGER NOT NULL DEFAULT 0,
                completed INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                error_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                UNIQUE(project_id, wandb_run_id)
            );
        """)
        self.conn.commit()

    def close(self):
        self.conn.close()

    # --- sources ---

    def upsert_source(self, wandb_url: str, entity: str) -> int:
        self.conn.execute(
            "INSERT OR IGNORE INTO migration_sources (wandb_url, entity) VALUES (?, ?)",
            (wandb_url, entity),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT id FROM migration_sources WHERE wandb_url = ? AND entity = ?",
            (wandb_url, entity),
        ).fetchone()
        return row["id"]

    # --- projects ---

    def add_project(self, source_id: int, project_name: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO migration_projects (source_id, project_name) VALUES (?, ?)",
            (source_id, project_name),
        )
        self.conn.commit()

    def get_next_project(self, status: str = "pending") -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM migration_projects WHERE status = ? ORDER BY id LIMIT 1",
            (status,),
        ).fetchone()
        return dict(row) if row else None

    def update_project_status(
        self,
        project_id: int,
        status: str,
        total_runs: int | None = None,
        error: str | None = None,
    ):
        self.conn.execute(
            """UPDATE migration_projects
               SET status = ?, total_runs = COALESCE(?, total_runs),
                   error_message = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (status, total_runs, error, project_id),
        )
        self.conn.commit()

    def reset_project(self, project_id: int):
        self.conn.execute(
            """UPDATE migration_projects
               SET status = 'pending', error_message = NULL, updated_at = datetime('now')
               WHERE id = ?""",
            (project_id,),
        )
        self.conn.commit()

    # --- runs ---

    def upsert_migration_run(
        self,
        project_id: int,
        wandb_run_id: str,
        name: str | None = None,
        state: str | None = None,
    ):
        self.conn.execute(
            """INSERT INTO migration_runs (project_id, wandb_run_id, wandb_run_name, wandb_run_state)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(project_id, wandb_run_id) DO UPDATE SET
                   wandb_run_name = COALESCE(excluded.wandb_run_name, wandb_run_name),
                   wandb_run_state = COALESCE(excluded.wandb_run_state, wandb_run_state),
                   updated_at = datetime('now')""",
            (project_id, wandb_run_id, name, state),
        )
        self.conn.commit()

    def get_next_run(self, max_errors: int = 3) -> dict | None:
        # Prefer in_progress (resume), then pending
        for status in ("in_progress", "pending"):
            row = self.conn.execute(
                """SELECT r.*, p.project_name, p.source_id,
                          s.wandb_url, s.entity
                   FROM migration_runs r
                   JOIN migration_projects p ON r.project_id = p.id
                   JOIN migration_sources s ON p.source_id = s.id
                   WHERE r.status = ? AND r.error_count < ? AND p.status = 'ready'
                   ORDER BY r.id LIMIT 1""",
                (status, max_errors),
            ).fetchone()
            if row:
                return dict(row)
        return None

    def update_run_status(self, run_id: int, status: str):
        self.conn.execute(
            "UPDATE migration_runs SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, run_id),
        )
        self.conn.commit()

    def mark_upserted(self, run_id: int):
        self.conn.execute(
            "UPDATE migration_runs SET upserted = 1, updated_at = datetime('now') WHERE id = ?",
            (run_id,),
        )
        self.conn.commit()

    def update_history_lines_sent(self, run_id: int, count: int):
        self.conn.execute(
            "UPDATE migration_runs SET history_lines_sent = ?, updated_at = datetime('now') WHERE id = ?",
            (count, run_id),
        )
        self.conn.commit()

    def mark_history_done(self, run_id: int):
        self.conn.execute(
            "UPDATE migration_runs SET history_done = 1, updated_at = datetime('now') WHERE id = ?",
            (run_id,),
        )
        self.conn.commit()

    def mark_summary_done(self, run_id: int):
        self.conn.execute(
            "UPDATE migration_runs SET summary_done = 1, updated_at = datetime('now') WHERE id = ?",
            (run_id,),
        )
        self.conn.commit()

    def mark_events_done(self, run_id: int):
        self.conn.execute(
            "UPDATE migration_runs SET events_done = 1, updated_at = datetime('now') WHERE id = ?",
            (run_id,),
        )
        self.conn.commit()

    def mark_logs_done(self, run_id: int):
        self.conn.execute(
            "UPDATE migration_runs SET logs_done = 1, updated_at = datetime('now') WHERE id = ?",
            (run_id,),
        )
        self.conn.commit()

    def mark_completed(self, run_id: int):
        self.conn.execute(
            "UPDATE migration_runs SET completed = 1, updated_at = datetime('now') WHERE id = ?",
            (run_id,),
        )
        self.conn.commit()

    def record_run_error(self, run_id: int, message: str):
        self.conn.execute(
            """UPDATE migration_runs
               SET status = 'error', error_message = ?,
                   error_count = error_count + 1, updated_at = datetime('now')
               WHERE id = ?""",
            (message, run_id),
        )
        self.conn.commit()

    def reset_run(self, run_id: int):
        self.conn.execute(
            """UPDATE migration_runs
               SET status = 'pending', error_message = NULL, error_count = 0,
                   upserted = 0, history_lines_sent = 0, history_done = 0,
                   summary_done = 0, events_done = 0, logs_done = 0, completed = 0,
                   updated_at = datetime('now')
               WHERE id = ?""",
            (run_id,),
        )
        self.conn.commit()

    def check_project_done(self, project_id: int) -> bool:
        row = self.conn.execute(
            """SELECT COUNT(*) as remaining FROM migration_runs
               WHERE project_id = ? AND status != 'done'""",
            (project_id,),
        ).fetchone()
        if row["remaining"] == 0:
            self.update_project_status(project_id, "done")
            return True
        return False

    # --- status / queries ---

    def get_status(self) -> list[dict]:
        sources = []
        for src in self.conn.execute("SELECT * FROM migration_sources").fetchall():
            src_dict = {"wandb_url": src["wandb_url"], "entity": src["entity"], "projects": []}
            for proj in self.conn.execute(
                "SELECT * FROM migration_projects WHERE source_id = ? ORDER BY id",
                (src["id"],),
            ).fetchall():
                counts = self.conn.execute(
                    """SELECT
                        SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done,
                        SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                        SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error
                       FROM migration_runs WHERE project_id = ?""",
                    (proj["id"],),
                ).fetchone()
                src_dict["projects"].append({
                    "id": proj["id"],
                    "name": proj["project_name"],
                    "status": proj["status"],
                    "total_runs": proj["total_runs"],
                    "runs_done": counts["done"],
                    "runs_in_progress": counts["in_progress"],
                    "runs_pending": counts["pending"],
                    "runs_error": counts["error"],
                })
            sources.append(src_dict)
        return sources

    def get_project_runs(self, project_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM migration_runs WHERE project_id = ? ORDER BY id",
            (project_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def find_project(
        self, entity: str, project_name: str
    ) -> dict | None:
        row = self.conn.execute(
            """SELECT p.* FROM migration_projects p
               JOIN migration_sources s ON p.source_id = s.id
               WHERE s.entity = ? AND p.project_name = ?""",
            (entity, project_name),
        ).fetchone()
        return dict(row) if row else None

    def find_run(
        self, entity: str, project_name: str, wandb_run_id: str
    ) -> dict | None:
        row = self.conn.execute(
            """SELECT r.* FROM migration_runs r
               JOIN migration_projects p ON r.project_id = p.id
               JOIN migration_sources s ON p.source_id = s.id
               WHERE s.entity = ? AND p.project_name = ? AND r.wandb_run_id = ?""",
            (entity, project_name, wandb_run_id),
        ).fetchone()
        return dict(row) if row else None

    def reset_all_runs(self, project_id: int):
        """Reset all runs (including done) back to pending with cleared progress."""
        self.conn.execute(
            """UPDATE migration_runs
               SET status = 'pending', error_message = NULL, error_count = 0,
                   upserted = 0, history_lines_sent = 0, history_done = 0,
                   summary_done = 0, events_done = 0, logs_done = 0, completed = 0,
                   updated_at = datetime('now')
               WHERE project_id = ?""",
            (project_id,),
        )
        self.conn.commit()

    def reset_failed_runs(self, project_id: int):
        self.conn.execute(
            """UPDATE migration_runs
               SET status = 'pending', error_message = NULL, error_count = 0,
                   updated_at = datetime('now')
               WHERE project_id = ? AND status = 'error'""",
            (project_id,),
        )
        self.conn.commit()
