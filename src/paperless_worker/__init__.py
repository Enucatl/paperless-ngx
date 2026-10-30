"""Keep the queue parent independent of Django application initialization."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celery import Celery

# Set only after fork, before importing the normal application entrypoint.
child_app: "Celery | None" = None
