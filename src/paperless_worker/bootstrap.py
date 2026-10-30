"""Run a prefork queue parent with Django confined to disposable helpers/children."""

import functools
import inspect
import json
import logging
import logging.config
import socket
import subprocess
import sys
import time
from typing import Any

from celery import Celery
from celery import Task
from celery import bootsteps
from celery.worker import state
from celery.worker.consumer.consumer import Consumer
from celery.worker.request import Request

from paperless_worker.loader import ChildDjangoLoader
from paperless_worker.metadata import MAX_METADATA_BYTES
from paperless_worker.outcomes import OutcomeWriter
from paperless_worker.outcomes import request_context
from paperless_worker.serialization import LegacyModelPayloadError
from paperless_worker.serialization import register

logger = logging.getLogger(__name__)


def read_metadata() -> dict[str, Any]:
    """Receive bounded configuration over a private pipe, never stdout or disk."""
    parent, child = socket.socketpair()
    with parent, child:
        process = subprocess.Popen(
            [sys.executable, "-m", "paperless_worker.metadata", str(child.fileno())],
            pass_fds=(child.fileno(),),
            stdout=subprocess.DEVNULL,
        )
        child.close()
        deadline = time.monotonic() + 30
        payload = bytearray()
        try:
            while True:
                parent.settimeout(max(0.001, deadline - time.monotonic()))
                block = parent.recv(min(65536, MAX_METADATA_BYTES + 1 - len(payload)))
                if not block:
                    break
                payload.extend(block)
                if len(payload) > MAX_METADATA_BYTES:
                    raise ValueError("Worker metadata exceeds its one-MiB limit.")
            if process.wait(timeout=max(0.001, deadline - time.monotonic())) != 0:
                raise RuntimeError(
                    "Worker metadata helper failed; see its error above.",
                )
        except BaseException:
            process.kill()
            process.wait()
            raise
    metadata = json.loads(payload)
    if not isinstance(metadata, dict) or not metadata.get("tasks"):
        raise ValueError("Worker metadata helper returned no task registrations.")
    return metadata


def task_header(description: dict[str, Any]) -> Any:
    """Recreate signature checks and Celery's inline-errback arity decision."""
    signature = inspect.Signature(
        [
            inspect.Parameter(
                item["name"],
                item["kind"],
                default=None if item["default"] else inspect.Parameter.empty,
            )
            for item in description["parameters"]
        ],
    )

    def check(*args: Any, **kwargs: Any) -> None:
        """Validate the original signature without importing its implementation."""
        signature.bind(*args, **kwargs)

    check.__signature__ = signature
    return functools.partial(check) if description["partial"] else check


class LeanRequest(Request):
    """Expose the current failure attempt without changing Celery's decisions."""

    def on_failure(
        self,
        exc_info: Any,
        send_failed_event: bool = True,  # noqa: FBT001, FBT002
        return_ok: bool = False,  # noqa: FBT001, FBT002
    ) -> Any:
        """Keep failure outcome signals associated with their native request."""
        token = request_context.set(self)
        try:
            return super().on_failure(exc_info, send_failed_event, return_ok)
        finally:
            request_context.reset(token)

    def on_timeout(self, soft: bool, timeout: float) -> Any:  # noqa: FBT001
        """Record hard timeouts, which Celery does not emit as failure signals."""
        if not soft and not state.should_terminate:
            from billiard.exceptions import TimeLimitExceeded

            token = request_context.set(self)
            try:
                self.app._paperless_outcomes.on_failure(
                    sender=self.task,
                    task_id=self.id,
                    exception=TimeLimitExceeded(timeout),
                )
            finally:
                request_context.reset(token)
        return super().on_timeout(soft, timeout)


class MetadataTask(Task):
    """Hold parent options; inline callbacks execute in a short-lived helper."""

    abstract = True
    Request = LeanRequest

    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Preserve Celery's inline callback path without running Django here."""
        return self.app._paperless_outcomes.run_inline_task(self.name, args, kwargs)


class LeanConsumer(Consumer):
    """Make legacy model messages visibly fail instead of leaving pending rows."""

    def on_decode_error(self, message: Any, exc: Exception) -> None:
        """Record authenticated legacy-message failures durably before acking."""
        reason = exc.args[0] if exc.args else exc
        if not isinstance(reason, LegacyModelPayloadError):
            return super().on_decode_error(message, exc)
        task_id = message.headers.get("id")
        task_name = message.headers.get("task")
        if not isinstance(task_id, str) or task_name not in self.app.tasks:
            return super().on_decode_error(message, exc)
        try:
            self.app._paperless_outcomes.record_failed(
                task_id,
                task_name,
                type(reason).__name__,
                str(reason),
            )
            self.app.backend.mark_as_failure(task_id, reason)
        except Exception:
            message.reject(requeue=True)
            raise
        logger.error("Rejected legacy model payload for task %s: %s", task_id, reason)
        message.ack()


class OutcomeStep(bootsteps.StartStopStep):
    """Replay pending parent outcomes while the worker is idle or recovering."""

    requires = {"celery.worker.components:Timer"}

    def start(self, worker: Any) -> None:
        """Start a bounded helper pump on the existing worker timer."""
        self.reference = worker.timer.call_repeatedly(
            5,
            worker.app._paperless_outcomes.tick,
            priority=10,
        )
        worker.app._paperless_outcomes.tick()

    def stop(self, worker: Any) -> None:
        """Leave unfinished durable outcomes available for the next worker."""
        if getattr(self, "reference", None) is not None:
            self.reference.cancel()
        worker.app._paperless_outcomes.close()

    terminate = stop


def create_app(metadata: dict[str, Any]) -> Celery:
    """Build the queue parent without importing application settings or models."""
    logging.config.dictConfig(metadata["logging"])
    key = metadata["secret_key"].encode()
    register(key)
    app = Celery("paperless", loader=ChildDjangoLoader, fixups=[])
    app.conf.update(metadata["configuration"])
    app.conf.worker_consumer = "paperless_worker.bootstrap:LeanConsumer"
    app._paperless_metadata = metadata
    app._paperless_outcomes = OutcomeWriter(
        metadata["data_dir"],
        metadata["tracked_tasks"],
        secret_key=key,
    )
    for name, description in metadata["tasks"].items():
        options = dict(description["options"])
        options.update(name=name, Request=LeanRequest)
        if description["header"] is not None:
            options["__header__"] = staticmethod(task_header(description["header"]))
        task = type("RegisteredMetadataTask", (MetadataTask,), options)()
        app.register_task(task)
    app.steps["worker"].add(OutcomeStep)
    app._paperless_outcomes.connect_signals()
    return app


def main() -> None:
    """Start the opt-in worker using the normal resolved concurrency limit."""
    app = create_app(read_metadata())
    try:
        app.worker_main(
            [
                "worker",
                "--autoscale",
                f"{app.conf.worker_concurrency},0",
                "--loglevel",
                "INFO",
                "--without-mingle",
                "--without-gossip",
            ],
        )
    finally:
        app._paperless_outcomes.close()
