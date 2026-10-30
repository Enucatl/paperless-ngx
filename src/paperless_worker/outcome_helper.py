"""Apply durable parent outcomes in a disposable, fully initialized Django process."""

from __future__ import annotations

import base64
import fcntl
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from paperless_worker.outcomes import MAX_CALLBACK_BYTES
from paperless_worker.outcomes import MAX_RECORD_BYTES
from paperless_worker.outcomes import validate_outcome

logger = logging.getLogger("paperless.worker.outcomes")


def apply_outcome(
    task_id: str,
    status: str,
    result_data: dict,
    completed_at: datetime,
    *,
    task_type: str | None = None,
    require_existing: bool = False,
) -> None:
    """Update an unfinished task once, preserving a newer attempt or final state."""
    from django.db import close_old_connections
    from django.db.models import Q

    from documents.models import PaperlessTask

    if status not in (PaperlessTask.Status.FAILURE, PaperlessTask.Status.REVOKED):
        raise ValueError("Unsupported task outcome")
    close_old_connections()
    tasks = PaperlessTask.objects.filter(task_id=task_id)
    if task_type is not None:
        tasks = tasks.filter(task_type=task_type)
    task = tasks.values("status", "date_started", "date_created").first()
    if task is None:
        if require_existing:
            raise LookupError(f"Task history is not available for {task_id}")
        return
    if task["status"] in PaperlessTask.COMPLETE_STATUSES:
        return
    started = task["date_started"]
    if started is not None and started > completed_at:
        return
    update = {"status": status, "result_data": result_data, "date_done": completed_at}
    if status == PaperlessTask.Status.FAILURE and started:
        update["duration_seconds"] = (completed_at - started).total_seconds()
        if task["date_created"]:
            update["wait_time_seconds"] = (
                started - task["date_created"]
            ).total_seconds()
    tasks.filter(
        status__in=(PaperlessTask.Status.PENDING, PaperlessTask.Status.STARTED),
    ).filter(Q(date_started__isnull=True) | Q(date_started__lte=completed_at)).update(
        **update,
    )


def drain(directory: Path) -> bool:
    """Apply one bounded batch, retaining every unacknowledged record on failure."""
    from documents.signals.handlers import TRACKED_TASKS

    failed = False
    pending = []
    for path in directory.glob("*.json"):
        try:
            pending.append((path.stat(follow_symlinks=False).st_mtime_ns, path))
        except FileNotFoundError:
            continue
    rotation_time = max((mtime for mtime, _ in pending), default=0)
    for _, path in sorted(pending)[:100]:
        try:
            # Rotate before delivery so even a killed helper cannot starve later records.
            rotation_time += 1
            os.utime(path, ns=(rotation_time, rotation_time), follow_symlinks=False)
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as source:
                payload = source.read(MAX_CALLBACK_BYTES + 1)
            if len(payload) > MAX_CALLBACK_BYTES:
                raise ValueError("Worker outcome record is too large")
            record = json.loads(payload)
            if isinstance(record, dict) and record.get("kind") == "callback":
                apply_callback(record)
            else:
                if len(payload) > MAX_RECORD_BYTES:
                    raise ValueError("Worker outcome record is too large")
                event = validate_outcome(record, TRACKED_TASKS)
                apply_outcome(
                    event["task_id"],
                    event["status"],
                    event["result_data"],
                    datetime.fromisoformat(event["completed_at"]),
                    task_type=TRACKED_TASKS[event["task_name"]],
                    require_existing=True,
                )
            path.unlink()
        except Exception:
            if not path.exists() and not path.is_symlink():
                continue
            failed = True
            logger.exception("Worker outcome %s remains pending", path.name)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return not failed


def apply_callback(record: dict) -> None:
    """Authenticate a deferred native callback after Django is initialized."""
    from paperless.celery import app
    from paperless.signed_pickle import signed_pickle_loads

    if set(record) != {"kind", "payload"} or not isinstance(record["payload"], str):
        raise ValueError("Invalid inline worker callback record")
    payload = base64.b64decode(record["payload"], validate=True)
    callback = signed_pickle_loads(payload)
    if not isinstance(callback, tuple) or len(callback) != 3:
        raise ValueError("Invalid inline worker callback arguments")
    name, args, kwargs = callback
    if (
        not isinstance(name, str)
        or not isinstance(args, (tuple, list))
        or not isinstance(kwargs, dict)
        or not all(isinstance(key, str) for key in kwargs)
    ):
        raise ValueError("Invalid inline worker callback arguments")
    app.loader.import_default_modules()
    app.finalize()
    app.tasks[name](*args, **kwargs)


def main() -> None:
    """Initialize Django only here and serialize helper batches across workers."""
    directory = Path(sys.argv[1])
    with (directory / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "paperless.settings")
        import django

        django.setup()
        if not drain(directory):
            raise SystemExit(1)


if __name__ == "__main__":
    main()
