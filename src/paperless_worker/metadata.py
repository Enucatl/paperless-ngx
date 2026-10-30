"""Read authoritative worker configuration in a short-lived Django process."""

import functools
import inspect
import json
import socket
import sys
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any

MAX_METADATA_BYTES = 1024 * 1024


def _plain(value: Any) -> bool:
    """Identify values that JSON can carry without importing application classes."""
    if value is None or type(value) in (str, int, float, bool):
        return True
    if isinstance(value, (list, tuple)):
        return all(_plain(item) for item in value)
    return isinstance(value, Mapping) and all(
        isinstance(key, str) and _plain(item) for key, item in value.items()
    )


def describe_task(task: Any, app: Any) -> dict[str, Any]:
    """Preserve parent options and reject task subclasses with custom behavior."""
    from celery.app.task import Task
    from celery.contrib.django.task import DjangoTask

    if type(task).__bases__ != (app.Task,) or app.Task.__bases__ != (DjangoTask,):
        raise ValueError(f"Lean worker does not support custom task base: {task.name}")
    for attribute in ("Strategy", "Request"):
        if getattr(task, attribute) != getattr(Task, attribute):
            raise ValueError(
                f"Lean worker does not support custom task {attribute}: {task.name}",
            )
    for method in (
        "__call__",
        "start_strategy",
        "on_failure",
        "on_retry",
        "on_success",
        "after_return",
        "before_start",
    ):
        if getattr(type(task), method) is not getattr(DjangoTask, method):
            raise ValueError(
                f"Lean worker does not support task hook {method}: {task.name}",
            )
    if task._backend is not None:
        raise ValueError(
            f"Lean worker does not support a task-specific backend: {task.name}",
        )

    child_only = {
        "app",
        "backend",
        "request",
        "request_stack",
        "autoretry_for",
        "dont_autoretry_for",
        "throws",
    }
    options = {}
    for attribute in sorted(set(dir(Task)) | set(vars(type(task)))):
        if attribute.startswith("_") or attribute in child_only:
            continue
        value = getattr(task, attribute)
        if callable(value):
            continue
        if not _plain(value):
            raise ValueError(
                f"Lean worker cannot represent task option {attribute}: {task.name}",
            )
        options[attribute] = value
    header = getattr(task, "__header__", None)
    signature = None
    if header is not None:
        signature = {
            "partial": isinstance(header, functools.partial),
            "parameters": [
                {
                    "name": parameter.name,
                    "kind": int(parameter.kind),
                    "default": parameter.default is not inspect.Parameter.empty,
                }
                for parameter in inspect.signature(header).parameters.values()
            ],
        }
    return {"options": options, "header": signature, "module": type(task).__module__}


def logging_configuration(value: Any) -> Any:
    """Keep rotating file logging while rejecting imports of application handlers."""
    if (
        isinstance(value, str)
        and value.startswith("ext://")
        and value
        not in (
            "ext://sys.stdout",
            "ext://sys.stderr",
        )
    ):
        raise ValueError(
            "Lean worker does not support this external logging reference.",
        )
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if key in ("class", "()"):
                if item == "paperless.logging.ConsumeTaskFormatter":
                    item = "paperless_worker.logging.ConsumeTaskFormatter"
                if not isinstance(item, str) or not item.startswith(
                    (
                        "logging.",
                        "concurrent_log_handler.",
                        "paperless_worker.logging.",
                    ),
                ):
                    raise ValueError("Lean worker does not support this logging class.")
            result[key] = logging_configuration(item)
        return result
    if isinstance(value, (list, tuple)):
        return [logging_configuration(item) for item in value]
    if not _plain(value):
        raise ValueError("Lean worker cannot represent this logging configuration.")
    return value


def build_metadata() -> dict[str, Any]:
    """Resolve normal settings and registration without persisting credentials."""
    from django.conf import settings

    from paperless.celery import app

    app.loader.init_worker()
    app.finalize()
    if app.conf.worker_pool != "prefork":
        raise ValueError("Lean workers require the prefork pool.")
    if app.conf.worker_concurrency < 1:
        raise ValueError("PAPERLESS_TASK_WORKERS must be greater than zero.")
    configured = {
        setting.removeprefix("CELERY_").lower()
        for setting in dir(settings)
        if setting.startswith("CELERY_")
    }
    configured.update(
        key.removeprefix("CELERY_").lower()
        for key in app.conf.changes
        if key != "deprecated_settings"
    )
    configuration = {}
    for key in sorted(configured):
        # Discovery belongs in the child; this service never runs embedded beat.
        if key.startswith("beat_") or key in ("imports", "include"):
            continue
        value = app.conf.get(key)
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if not _plain(value):
            raise ValueError(f"Lean worker cannot represent configuration {key}.")
        configuration[key] = value

    from documents.signals.handlers import TRACKED_TASKS

    return {
        "configuration": configuration,
        "logging": logging_configuration(settings.LOGGING),
        "tasks": {name: describe_task(task, app) for name, task in app.tasks.items()},
        "secret_key": settings.SECRET_KEY,
        "data_dir": str(settings.DATA_DIR),
        "tracked_tasks": dict(TRACKED_TASKS),
    }


def main() -> None:
    """Send metadata only to the inherited private descriptor supplied by parent."""
    descriptor = int(sys.argv[1])
    payload = json.dumps(build_metadata(), allow_nan=False).encode()
    if len(payload) > MAX_METADATA_BYTES:
        raise ValueError("Worker metadata exceeds its one-MiB limit.")
    with socket.socket(fileno=descriptor) as connection:
        connection.sendall(payload)


if __name__ == "__main__":
    main()
