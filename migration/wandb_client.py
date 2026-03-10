"""Wandb data fetching functions for migration."""

import json
import sys
import time

import wandb


SCAN_HISTORY_PAGE_SIZE = 10_000  # must be large enough to avoid wandb downsampling within a page
MAX_RETRIES = 5
RETRY_BASE_DELAY = 5


def retry(fn, description="operation", retries=MAX_RETRIES):
    """Retry a function with exponential backoff."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            if attempt == retries - 1:
                raise
            delay = RETRY_BASE_DELAY * (2 ** attempt)
            print(f"        {description} failed (attempt {attempt + 1}/{retries}): {e}", file=sys.stderr)
            print(f"        retrying in {delay}s...", file=sys.stderr)
            time.sleep(delay)


def make_wandb_api(wandb_url: str | None = None, timeout: int = 300) -> wandb.Api:
    """Create a wandb API client."""
    api_kwargs = {"timeout": timeout}
    if wandb_url:
        api_kwargs["overrides"] = {"base_url": wandb_url}
    return wandb.Api(**api_kwargs)


def merge_history_rows(scanner) -> list[dict]:
    """Merge scan_history rows by _step.

    wandb's scan_history may return multiple rows per step (from multiple
    wandb.log() calls). Merge them into one dict per step and rename
    _step -> step so the worb parser picks it up as x_step.
    """
    by_step = {}
    order = []
    for row in scanner:
        step = row.get("_step", 0)
        if step not in by_step:
            by_step[step] = {}
            order.append(step)
        by_step[step].update(row)

    merged = []
    for step in order:
        row = by_step[step]
        if "_step" in row:
            row["step"] = row.pop("_step")
        for k in ("_runtime", "_timestamp"):
            row.pop(k, None)
        merged.append(row)
    return merged


def get_run_summary(run) -> dict:
    """Safely extract summary dict from a run."""
    try:
        summary = dict(run.summary._json_dict)
    except Exception:
        try:
            summary = dict(run.summary)
        except Exception:
            return {}
    return {k: v for k, v in summary.items() if not k.startswith("_")}


def fetch_scan_history(run):
    """Fetch and merge history rows from a wandb run."""
    scanner = retry(
        lambda: run.scan_history(page_size=SCAN_HISTORY_PAGE_SIZE),
        "scan_history init",
    )
    return merge_history_rows(scanner)


def fetch_system_events(run) -> list[str]:
    """Fetch system events from a wandb run. Returns JSON lines."""
    events = retry(
        lambda: run.history(stream="events", samples=10000, pandas=False),
        "fetch system events",
        retries=3,
    )
    if not events:
        return []
    return [json.dumps(row, default=str) for row in events]


def fetch_run_files(run) -> list:
    """Fetch the list of files for a wandb run.

    Returns wandb File objects (each has .name and .download()).
    Excludes internal files that are handled separately (history, events, logs, summary).
    """
    EXCLUDED = {
        "wandb-history.jsonl",
        "wandb-events.jsonl",
        "wandb-summary.json",
        "output.log",
        "config.yaml",
        "requirements.txt",
        "wandb-metadata.json",
    }
    EXCLUDED_PREFIXES = ("artifact/", "code/")
    try:
        all_files = retry(lambda: list(run.files()), "list files", retries=3)
    except Exception:
        return []
    return [
        f for f in all_files
        if f.name not in EXCLUDED and not f.name.startswith(EXCLUDED_PREFIXES)
    ]


def download_file(wandb_file, dest_dir: str = "/tmp/worb-migration") -> str | None:
    """Download a wandb file to a local path. Returns the local path or None."""
    import os
    os.makedirs(dest_dir, exist_ok=True)
    try:
        downloaded = retry(
            lambda: wandb_file.download(replace=True, root=dest_dir),
            f"download {wandb_file.name}",
            retries=3,
        )
        return downloaded.name
    except Exception:
        return None


def fetch_console_logs(run) -> list[str]:
    """Fetch console logs from a wandb run. Returns lines."""
    try:
        log_file = run.file("output.log").download(replace=True, root="/tmp/worb-migration")
    except Exception:
        return []

    try:
        with open(log_file.name, "r") as f:
            return f.read().splitlines()
    except Exception:
        return []
