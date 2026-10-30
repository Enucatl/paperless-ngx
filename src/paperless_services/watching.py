"""Filesystem-only filtering and stability tracking for consumption watchers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING

from watchfiles import Change

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

logger = logging.getLogger("paperless.management.consumer")


@dataclass
class TrackedFile:
    """Represents a file being tracked for stability."""

    path: Path
    last_event_time: float
    last_mtime: float | None = None
    last_size: int | None = None

    def update_stats(self) -> bool:
        """
        Update file stats. Returns True if file exists and stats were updated.
        """
        try:
            stat = self.path.stat()
            self.last_mtime = stat.st_mtime
            self.last_size = stat.st_size
            return True
        except OSError:
            return False

    def is_unchanged(self) -> bool:
        """
        Check if file stats match the previously recorded values.
        Returns False if file doesn't exist or stats changed.
        """
        try:
            stat = self.path.stat()
            return stat.st_mtime == self.last_mtime and stat.st_size == self.last_size
        except OSError:
            return False


class FileStabilityTracker:
    """
    Tracks file events and determines when files are stable for consumption.

    A file is considered stable when:
    1. No new events have been received for it within the stability delay
    2. Its size and modification time haven't changed
    3. It still exists as a regular file

    This handles various edge cases:
    - Network copies that write in chunks
    - Scanners that open/close files multiple times
    - Temporary files that get renamed
    - Files that are deleted before becoming stable
    """

    def __init__(self, stability_delay: float = 1.0) -> None:
        """
        Initialize the tracker.

        Args:
            stability_delay: Time in seconds a file must remain unchanged
                           before being considered stable.
        """
        self.stability_delay = stability_delay
        self._tracked: dict[Path, TrackedFile] = {}

    def track(self, path: Path, change: Change) -> None:
        """
        Register a file event.

        Args:
            path: The file path that changed.
            change: The type of change (added, modified, deleted).
        """
        path = path.resolve()

        match change:
            case Change.deleted:
                self._tracked.pop(path, None)
                logger.debug(f"Stopped tracking deleted file: {path}")
            case Change.added | Change.modified:
                current_time = monotonic()
                if path in self._tracked:
                    tracked = self._tracked[path]
                    tracked.last_event_time = current_time
                    tracked.update_stats()
                    logger.debug(f"Updated tracking for: {path}")
                else:
                    tracked = TrackedFile(path=path, last_event_time=current_time)
                    if tracked.update_stats():
                        self._tracked[path] = tracked
                        logger.debug(f"Started tracking: {path}")
                    else:
                        logger.debug(f"Could not stat file, not tracking: {path}")

    def get_stable_files(self) -> Iterator[Path]:
        """
        Yield files that have been stable for the configured delay.

        Files are removed from tracking once yielded or determined to be invalid.
        """
        current_time = monotonic()
        to_remove: list[Path] = []
        to_yield: list[Path] = []

        for path, tracked in self._tracked.items():
            time_since_event = current_time - tracked.last_event_time

            if time_since_event < self.stability_delay:
                continue

            # File has waited long enough, verify it's unchanged
            if not tracked.is_unchanged():
                # Stats changed or file gone - update and wait again
                if tracked.update_stats():
                    tracked.last_event_time = current_time
                    logger.debug(f"File changed during stability check: {path}")
                else:
                    # File no longer exists, remove from tracking
                    to_remove.append(path)
                    logger.debug(f"File disappeared during stability check: {path}")
                continue

            # Stable, but empty: some scanners create a zero byte placeholder
            # and only write the page some time later. Consuming it now can
            # only fail  so drop it and let the writer's next event
            # (or the periodic rescan) bring it back once it has content
            if not tracked.last_size:
                to_remove.append(path)
                logger.debug("Ignoring stable but empty file: %s", path)
                continue

            # File is stable, we can return it
            to_yield.append(path)
            logger.info(f"File is stable: {path}")

        # Remove files that are no longer valid
        for path in to_remove:
            self._tracked.pop(path, None)

        # Remove and yield stable files
        for path in to_yield:
            self._tracked.pop(path, None)
            yield path

    def is_tracking(self, path: Path) -> bool:
        """Check whether a path is currently being tracked for stability."""
        return path.resolve() in self._tracked

    def has_pending_files(self) -> bool:
        """Check if there are files waiting for stability check."""
        return len(self._tracked) > 0

    @property
    def pending_count(self) -> int:
        """Number of files being tracked."""
        return len(self._tracked)
