"""Persist parent-side task outcomes without importing Django."""

from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
import traceback as traceback_module
import uuid
from contextvars import ContextVar
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger("paperless.worker.outcomes")
request_context: ContextVar[object | None] = ContextVar(
    "paperless_worker_request",
    default=None,
)
MAX_RECORD_BYTES = 65536
MAX_CALLBACK_BYTES = 2 * 1024 * 1024
HELPER_TIMEOUT = 30


def failure_data(
    exception: BaseException | None,
    traceback: str | TracebackType | None = None,
) -> dict[str, str]:
    """Preserve the normal task failure payload using only primitive strings."""
    result = {
        "error_type": getattr(exception, "remote_type", type(exception).__name__)
        if exception
        else "Unknown",
        "error_message": str(exception) if exception else "Unknown error",
    }
    if traceback:
        result["traceback"] = (
            traceback
            if isinstance(traceback, str)
            else "".join(traceback_module.format_tb(traceback))
        )[:5000]
    return result


def revoked_data(*, expired: bool = False, terminated: bool = False) -> dict[str, str]:
    """Preserve the normal user-visible cancellation reason."""
    reason = "Task revoked before completion."
    if expired:
        reason = "Task expired before execution."
    elif terminated:
        reason = "Task terminated during execution."
    return {"reason": reason + " Check worker logs for details."}


def validate_outcome(event: object, tracked_tasks: dict[str, str]) -> dict:
    """Reject malformed local records before they reach Django or the database."""
    fields = {"task_id", "task_name", "status", "completed_at", "result_data"}
    if not isinstance(event, dict) or set(event) != fields:
        raise ValueError("Invalid worker outcome fields")
    if not isinstance(event["task_id"], str) or not 1 <= len(event["task_id"]) <= 72:
        raise ValueError("Invalid worker outcome task ID")
    if (
        not isinstance(event["task_name"], str)
        or event["task_name"] not in tracked_tasks
    ):
        raise ValueError("Untracked worker outcome task")
    if event["status"] not in ("failure", "revoked"):
        raise ValueError("Invalid worker outcome status")
    if not isinstance(event["completed_at"], str) or len(event["completed_at"]) > 40:
        raise ValueError("Invalid worker outcome timestamp")
    completed_at = datetime.fromisoformat(event["completed_at"])
    if completed_at.tzinfo is None:
        raise ValueError("Worker outcome timestamp must include timezone")
    result = event["result_data"]
    required = (
        {"error_type", "error_message"} if event["status"] == "failure" else {"reason"}
    )
    allowed = required | ({"traceback"} if event["status"] == "failure" else set())
    if not isinstance(result, dict) or not required <= set(result) <= allowed:
        raise ValueError("Invalid worker outcome result fields")
    if not all(
        isinstance(value, str) and len(value) <= 16000 for value in result.values()
    ):
        raise ValueError("Invalid worker outcome result value")
    return event


class OutcomeWriter:
    """Durably enqueue outcomes and supervise one disposable Django helper."""

    def __init__(
        self,
        data_dir: str | Path,
        tracked_tasks: dict[str, str],
        secret_key: bytes | str = b"",
    ) -> None:
        """Create a private spool within the existing persistent data directory."""
        self.directory = Path(data_dir) / "worker-outcomes"
        self.directory.mkdir(mode=0o700, exist_ok=True)
        if self.directory.is_symlink():
            raise ValueError("Worker outcome spool must not be a symlink")
        self.directory.chmod(0o700)
        self.tracked_tasks = dict(tracked_tasks)
        self.pid = os.getpid()
        self.helper: subprocess.Popen | None = None
        self.started = 0.0
        self.secret_key = (
            secret_key.encode() if isinstance(secret_key, str) else secret_key
        )
        self.failures = 0
        self.lock = threading.Lock()

    def _write(self, event: dict) -> Path:
        """Fsync before exposing a complete JSON record or attempting delivery."""
        callback = event.get("kind") == "callback"
        if not callback:
            validate_outcome(event, self.tracked_tasks)
        payload = json.dumps(event).encode()
        if len(payload) > (MAX_CALLBACK_BYTES if callback else MAX_RECORD_BYTES):
            raise ValueError("Worker outcome record is too large")
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.directory,
                prefix=".pending-",
                delete=False,
            ) as output:
                temporary = Path(output.name)
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            destination = self.directory / f"{time.time_ns()}-{uuid.uuid4().hex}.json"
            temporary.replace(destination)
            descriptor = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except Exception:
            logger.exception("Cannot durably record worker outcome")
            raise
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)
        self.tick()
        return destination

    def run_inline_task(self, task_name: str, args: tuple, kwargs: dict) -> None:
        """Run an authenticated native inline callback in a short-lived helper."""
        from paperless_worker.serialization import dumps

        if not self.secret_key:
            raise ValueError("Inline worker callbacks require the signing key")
        payload = dumps((task_name, args, kwargs), self.secret_key)
        failures = self.failures
        path = self._write(
            {"kind": "callback", "payload": base64.b64encode(payload).decode("ascii")},
        )
        deadline = time.monotonic() + HELPER_TIMEOUT
        while path.exists():
            self.tick()
            if not path.exists():
                return
            if self.failures != failures:
                raise RuntimeError(
                    "Inline worker callback failed; its durable record will be retried",
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "Inline worker callback timed out; its durable record will be retried",
                )
            time.sleep(0.1)

    def record_failed(
        self,
        task_id: str,
        task_name: str,
        error_type: str,
        message: str,
        traceback: str | None = None,
        *,
        completed_at: datetime | None = None,
    ) -> None:
        """Queue an allowlisted task failure, bounding externally supplied text."""
        if task_name not in self.tracked_tasks:
            return
        result = {"error_type": error_type[:256], "error_message": message[:5000]}
        if traceback:
            result["traceback"] = traceback[:5000]
        self._write(
            {
                "task_id": task_id,
                "task_name": task_name,
                "status": "failure",
                "completed_at": (completed_at or datetime.now(UTC)).isoformat(),
                "result_data": result,
            },
        )

    def record_revoked(
        self,
        task_id: str,
        task_name: str,
        *,
        expired: bool = False,
        terminated: bool = False,
        completed_at: datetime | None = None,
    ) -> None:
        """Queue an allowlisted cancellation or expiration with its event time."""
        if task_name not in self.tracked_tasks:
            return
        self._write(
            {
                "task_id": task_id,
                "task_name": task_name,
                "status": "revoked",
                "completed_at": (completed_at or datetime.now(UTC)).isoformat(),
                "result_data": revoked_data(expired=expired, terminated=terminated),
            },
        )

    def tick(self) -> None:
        """Reap or bound an existing helper, then replay pending records."""
        if os.getpid() != self.pid:
            return
        with self.lock:
            self._tick()

    def _tick(self) -> None:
        """Serialize helper launch and reaping across timer and signal callbacks."""
        if self.helper is not None:
            result = self.helper.poll()
            if result is None:
                if time.monotonic() - self.started >= HELPER_TIMEOUT:
                    logger.error(
                        "Worker outcome helper timed out; pending records will be replayed",
                    )
                    self.helper.kill()
                return
            if result:
                self.failures += 1
                logger.error(
                    "Worker outcome helper exited with status %s; pending records will be replayed",
                    result,
                )
            self.helper = None
        if not any(self.directory.glob("*.json")):
            return
        try:
            self.helper = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "paperless_worker.outcome_helper",
                    str(self.directory),
                ],
                stdin=subprocess.DEVNULL,
            )
            self.started = time.monotonic()
        except OSError:
            self.failures += 1
            logger.exception(
                "Cannot start worker outcome helper; pending records will be replayed",
            )

    @staticmethod
    def _occurred(request: object | None) -> datetime:
        """Reuse the first callback time when Celery repeats one request outcome."""
        if request is None:
            return datetime.now(UTC)
        occurred = getattr(request, "_paperless_outcome_at", None)
        if occurred is None:
            occurred = datetime.now(UTC)
            request._paperless_outcome_at = occurred
        return occurred

    def on_failure(
        self,
        sender=None,
        task_id=None,
        exception=None,
        traceback=None,
        **kwargs,
    ) -> None:
        """Record only parent-side failures; children retain normal ORM signals."""
        if os.getpid() != self.pid or task_id is None or sender is None:
            return
        data = failure_data(exception, traceback)
        request = request_context.get()
        if request is not None and request.id != task_id:
            request = None
        self.record_failed(
            task_id,
            sender.name,
            data["error_type"],
            data["error_message"],
            data.get("traceback"),
            completed_at=self._occurred(request),
        )

    def on_revoked(
        self,
        sender=None,
        request=None,
        *,
        expired=False,
        terminated=False,
        **kwargs,
    ) -> None:
        """Record cancellation without importing the task implementation."""
        if os.getpid() != self.pid or request is None or sender is None:
            return
        self.record_revoked(
            request.id,
            sender.name,
            expired=expired,
            terminated=terminated,
            completed_at=self._occurred(request),
        )

    def connect_signals(self) -> None:
        """Attach lean-parent callbacks while keeping inherited child ones inert."""
        from celery.signals import task_failure
        from celery.signals import task_revoked

        task_failure.connect(self.on_failure, weak=False)
        task_revoked.connect(self.on_revoked, weak=False)

    def close(self) -> None:
        """Stop a helper on shutdown, retaining its unacknowledged durable files."""
        if os.getpid() != self.pid:
            return
        with self.lock:
            if self.helper is None:
                return
            if self.helper.poll() is None:
                self.helper.kill()
            self.helper.wait(timeout=5)
            self.helper = None
