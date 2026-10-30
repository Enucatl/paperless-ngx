"""Share consumption log formatting without loading application settings."""

import logging
from contextvars import ContextVar

consume_task_id: ContextVar[str] = ContextVar("consume_task_id", default="")


class ConsumeTaskFormatter(logging.Formatter):
    """Add the active consumption task ID to the standard verbose log format."""

    def __init__(self) -> None:
        super().__init__(
            fmt="[{asctime}] [{levelname}] [{name}] {task_prefix}{message}",
            style="{",
            validate=False,
        )

    def format(self, record: logging.LogRecord) -> str:
        """Populate the task prefix before applying the shared format."""
        task_id = consume_task_id.get()
        record.task_prefix = f"[{task_id}] " if task_id else ""
        return super().format(record)
