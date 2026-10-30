"""Preserve application imports of the shared lightweight logging formatter."""

from paperless_worker.logging import ConsumeTaskFormatter
from paperless_worker.logging import consume_task_id

__all__ = ["ConsumeTaskFormatter", "consume_task_id"]
