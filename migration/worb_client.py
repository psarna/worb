"""Worb communication functions for migration."""

import json
import sys
import time

import requests


FILESTREAM_BATCH_SIZE = 200
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


def make_api_key(entity: str) -> str:
    """Generate a worb-compatible API key embedding the entity name."""
    return "dev-" + "lo" * 20 + "_" + entity


def make_session(worb_url: str, entity: str) -> requests.Session:
    """Create an HTTP session for talking to worb."""
    session = requests.Session()
    api_key = make_api_key(entity)
    session.headers["Authorization"] = f"Bearer {api_key}"
    session.headers["Content-Type"] = "application/json"
    return session


def upsert_run_graphql(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run,
) -> dict:
    """Create or update a run in worb via GraphQL."""
    from wandb_client import get_run_summary

    summary = get_run_summary(run)

    variables = {
        "entityName": entity,
        "project": project,
        "name": run.id,
        "displayName": run.name or run.id,
        "config": json.dumps(run.config or {}, default=str),
        "summary": json.dumps(summary, default=str),
        "state": "finished" if run.state == "finished" else run.state,
        "tags": run.tags or [],
        "notes": run.notes or "",
        "groupName": run.group or "",
        "jobType": run.job_type or "",
        "sweepName": run.sweep_name if hasattr(run, "sweep_name") and run.sweep_name else "",
        "commit": run.commit or "",
    }

    try:
        meta = run.metadata
        if meta:
            variables["host"] = meta.get("host", "")
            variables["program"] = meta.get("program", "")
    except Exception:
        pass

    query = """
    mutation UpsertBucket($input: UpsertBucketInput!) {
        upsertBucket(input: $input) {
            bucket { id name }
            inserted
        }
    }
    """

    def do():
        resp = session.post(
            f"{worb_url}/graphql",
            json={"query": query, "variables": {"input": variables}},
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]["upsertBucket"]

    return retry(do, "upsert run")


def send_filestream(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run_name: str,
    files: dict,
    complete: bool = False,
) -> None:
    """Send data to worb via the filestream endpoint."""
    url = f"{worb_url}/files/{entity}/{project}/{run_name}/file_stream"
    payload = {"files": files, "complete": complete}

    def do():
        resp = session.post(url, json=payload)
        resp.raise_for_status()

    retry(do, "filestream")


def get_wal_stats(worb_url: str) -> dict | None:
    """Fetch WAL stats from worb. Returns None on error."""
    try:
        resp = requests.get(f"{worb_url}/api/admin/wal-stats", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None
