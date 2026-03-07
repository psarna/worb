"""
Migrate data from an existing Weights & Biases instance to a worb server.

Usage:
    # Migrate all projects for an entity
    uv run migration.py --entity my-team --worb-url http://localhost:8080

    # Migrate a single project
    uv run migration.py --entity my-team --project my-project --worb-url http://localhost:8080

    # Migrate from a self-hosted wandb instance
    uv run migration.py --entity my-team --wandb-url https://wandb.example.com --worb-url http://localhost:8080

    # Dry run (list what would be migrated)
    uv run migration.py --entity my-team --dry-run
"""

import argparse
import json
import sys
import time

import requests
import wandb


FILESTREAM_BATCH_SIZE = 200  # history lines per filestream POST


def make_api_key(entity: str) -> str:
    """Generate a worb-compatible API key embedding the entity name."""
    return "dev-" + "lo" * 20 + "_" + entity


def upsert_run_graphql(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run: wandb.apis.public.Run,
) -> dict:
    """Create or update a run in worb via GraphQL."""
    config = run.config
    summary = {}
    try:
        summary = dict(run.summary._json_dict)
    except Exception:
        try:
            summary = dict(run.summary)
        except Exception:
            pass

    # Strip internal keys from summary
    summary = {k: v for k, v in summary.items() if not k.startswith("_")}

    tags_json = json.dumps(run.tags or [])
    config_json = json.dumps(config or {})
    summary_json = json.dumps(summary, default=str)

    variables = {
        "entityName": entity,
        "project": project,
        "name": run.id,
        "displayName": run.name or run.id,
        "config": config_json,
        "summary": summary_json,
        "state": "finished" if run.state == "finished" else run.state,
        "tags": run.tags or [],
        "notes": run.notes or "",
        "groupName": run.group or "",
        "jobType": run.job_type or "",
        "sweepName": run.sweep_name if hasattr(run, "sweep_name") and run.sweep_name else "",
        "commit": run.commit or "",
    }

    # Try to get host/program from metadata
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

    resp = session.post(
        f"{worb_url}/graphql",
        json={"query": query, "variables": {"input": variables}},
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL errors: {data['errors']}")
    return data["data"]["upsertBucket"]


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
    resp = session.post(url, json=payload)
    resp.raise_for_status()


def migrate_history(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run: wandb.apis.public.Run,
) -> int:
    """Migrate run history (metrics) via filestream. Returns number of steps migrated."""
    total = 0
    batch = []
    offset = 0

    for row in run.scan_history(page_size=1000):
        line = json.dumps(row, default=str)
        batch.append(line)
        total += 1

        if len(batch) >= FILESTREAM_BATCH_SIZE:
            send_filestream(
                session, worb_url, entity, project, run.id,
                files={
                    "wandb-history.jsonl": {
                        "offset": offset,
                        "content": batch,
                    }
                },
            )
            offset += len(batch)
            batch = []

    if batch:
        send_filestream(
            session, worb_url, entity, project, run.id,
            files={
                "wandb-history.jsonl": {
                    "offset": offset,
                    "content": batch,
                }
            },
        )

    return total


def migrate_summary(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run: wandb.apis.public.Run,
) -> None:
    """Send final summary via filestream."""
    try:
        summary = dict(run.summary._json_dict)
    except Exception:
        try:
            summary = dict(run.summary)
        except Exception:
            return

    summary = {k: v for k, v in summary.items() if not k.startswith("_")}
    if not summary:
        return

    send_filestream(
        session, worb_url, entity, project, run.id,
        files={
            "wandb-summary.json": {
                "offset": 0,
                "content": [json.dumps(summary, default=str)],
            }
        },
    )


def migrate_console_logs(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run: wandb.apis.public.Run,
) -> int:
    """Migrate console logs via filestream. Returns number of lines."""
    try:
        log_file = run.file("output.log").download(replace=True, root="/tmp/worb-migration")
    except Exception:
        return 0

    try:
        with open(log_file.name, "r") as f:
            lines = f.read().splitlines()
    except Exception:
        return 0

    if not lines:
        return 0

    for i in range(0, len(lines), FILESTREAM_BATCH_SIZE):
        chunk = lines[i : i + FILESTREAM_BATCH_SIZE]
        send_filestream(
            session, worb_url, entity, project, run.id,
            files={
                "output.log": {
                    "offset": i,
                    "content": chunk,
                }
            },
        )

    return len(lines)


def migrate_system_events(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run: wandb.apis.public.Run,
) -> int:
    """Migrate system metrics via filestream. Returns number of events."""
    try:
        events_df = run.history(stream="events", pandas=False)
    except Exception:
        return 0

    if not events_df:
        return 0

    lines = [json.dumps(row, default=str) for row in events_df]
    if not lines:
        return 0

    for i in range(0, len(lines), FILESTREAM_BATCH_SIZE):
        chunk = lines[i : i + FILESTREAM_BATCH_SIZE]
        send_filestream(
            session, worb_url, entity, project, run.id,
            files={
                "wandb-events.jsonl": {
                    "offset": i,
                    "content": chunk,
                }
            },
        )

    return len(lines)


def finish_run(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run_name: str,
) -> None:
    """Mark a run as complete via filestream."""
    send_filestream(
        session, worb_url, entity, project, run_name,
        files={},
        complete=True,
    )


def migrate_run(
    session: requests.Session,
    worb_url: str,
    entity: str,
    project: str,
    run: wandb.apis.public.Run,
) -> None:
    """Migrate a single run with all its data."""
    display = run.name or run.id
    print(f"    Run: {display} ({run.id}) state={run.state}")

    # 1. Create run with metadata
    result = upsert_run_graphql(session, worb_url, entity, project, run)
    was_new = result.get("inserted", False)
    print(f"      {'created' if was_new else 'updated'} run")

    # 2. Migrate history
    t0 = time.monotonic()
    steps = migrate_history(session, worb_url, entity, project, run)
    dt = time.monotonic() - t0
    if steps:
        print(f"      history: {steps} steps ({dt:.1f}s)")

    # 3. Migrate summary
    migrate_summary(session, worb_url, entity, project, run)

    # 4. Migrate console logs
    log_lines = migrate_console_logs(session, worb_url, entity, project, run)
    if log_lines:
        print(f"      logs: {log_lines} lines")

    # 5. Migrate system events
    events = migrate_system_events(session, worb_url, entity, project, run)
    if events:
        print(f"      events: {events}")

    # 6. Mark complete if the original run was finished
    if run.state == "finished":
        finish_run(session, worb_url, entity, project, run.id)

    print(f"      done")


def migrate_project(
    api: wandb.Api,
    session: requests.Session,
    worb_url: str,
    entity: str,
    project_name: str,
    dry_run: bool = False,
) -> None:
    """Migrate all runs in a project."""
    print(f"  Project: {entity}/{project_name}")

    runs = api.runs(f"{entity}/{project_name}", order="+created_at")
    run_list = list(runs)
    print(f"    {len(run_list)} runs")

    if dry_run:
        for run in run_list:
            display = run.name or run.id
            print(f"      {display} ({run.id}) state={run.state}")
        return

    for run in run_list:
        try:
            migrate_run(session, worb_url, entity, project_name, run)
        except Exception as e:
            print(f"      ERROR migrating run {run.id}: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Migrate data from Weights & Biases to worb",
    )
    parser.add_argument(
        "--entity",
        required=True,
        help="W&B entity (user or team) to migrate from",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="Migrate only this project (default: all projects)",
    )
    parser.add_argument(
        "--wandb-url",
        default=None,
        help="Source W&B instance URL (default: https://api.wandb.ai)",
    )
    parser.add_argument(
        "--worb-url",
        default="http://localhost:8080",
        help="Target worb server URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List projects and runs without migrating",
    )
    args = parser.parse_args()

    # Initialize wandb API (reads WANDB_API_KEY from env)
    api_kwargs = {}
    if args.wandb_url:
        api_kwargs["overrides"] = {"base_url": args.wandb_url}
    api = wandb.Api(**api_kwargs)

    # Set up HTTP session for worb
    session = requests.Session()
    api_key = make_api_key(args.entity)
    session.headers["Authorization"] = f"Bearer {api_key}"
    session.headers["Content-Type"] = "application/json"

    print(f"Migrating from W&B to {args.worb_url}")
    print(f"Entity: {args.entity}")

    if args.project:
        projects = [args.project]
    else:
        try:
            projects = [p.name for p in api.projects(entity=args.entity)]
        except Exception as e:
            print(f"Error listing projects: {e}", file=sys.stderr)
            sys.exit(1)

    print(f"Projects to migrate: {len(projects)}")
    print()

    for project_name in projects:
        try:
            migrate_project(api, session, args.worb_url, args.entity, project_name, args.dry_run)
        except Exception as e:
            print(f"  ERROR migrating project {project_name}: {e}", file=sys.stderr)
        print()

    print("Migration complete.")


if __name__ == "__main__":
    main()
