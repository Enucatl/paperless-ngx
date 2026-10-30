"""
Document consumer management command.

Watches a consumption directory for new documents and queues them for processing.
Uses watchfiles for efficient file system monitoring with support for both
native OS notifications and polling fallback.
"""

from __future__ import annotations

import logging
from pathlib import Path
from threading import Event
from time import monotonic
from typing import Final

from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from watchfiles import Change
from watchfiles import watch

from documents.consumption import _consume_file
from documents.parsers import get_supported_file_extensions
from paperless_services.filtering import FileFilter
from paperless_services.watching import FileStabilityTracker
from paperless_services.watching import (
    TrackedFile,  # noqa: F401 - retain the public import path
)

logger = logging.getLogger("paperless.management.consumer")


class ConsumerFilter(FileFilter):
    """Filter consumption paths using the registered parsers' extensions."""

    def __init__(
        self,
        *,
        supported_extensions: frozenset[str] | None = None,
        ignore_patterns: list[str] | None = None,
        ignore_dirs: list[str] | None = None,
    ) -> None:
        """Load parser extensions when the caller has not supplied them."""
        if supported_extensions is None:
            supported_extensions = frozenset(get_supported_file_extensions())
        super().__init__(
            supported_extensions=supported_extensions,
            ignore_patterns=ignore_patterns,
            ignore_dirs=ignore_dirs,
        )


class Command(BaseCommand):
    """
    Watch a consumption directory and queue new documents for processing.

    Uses watchfiles for efficient file system monitoring. Supports both
    native OS notifications (inotify on Linux, FSEvents on macOS) and
    polling for network filesystems.
    """

    help = "Watch the consumption directory for new documents"

    # For testing - allows tests to stop the consumer
    stop_flag: Event = Event()

    # Testing timeout in seconds
    testing_timeout_s: Final[float] = 0.5

    # How often to perform a full-glob rescan of the consume directory as a
    # safety net. Each watchfiles watcher is torn down and recreated on every
    # batch to reconfigure its timeout, and a fresh watcher silently adopts the
    # current directory contents as its baseline. A file that appears between
    # one batch and the next watcher's baseline is therefore never reported and
    # would sit in the consume directory forever. This periodic rescan re-injects
    # such files into the stability tracker (see GH issue #13011). Not currently
    # user-configurable; instances may override for testing.
    rescan_interval_s: float = 300.0

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "directory",
            default=None,
            nargs="?",
            help="The consumption directory (defaults to CONSUMPTION_DIR setting)",
        )
        parser.add_argument(
            "--oneshot",
            action="store_true",
            help="Process existing files and exit without watching",
        )
        parser.add_argument(
            "--testing",
            action="store_true",
            help="Enable testing mode with shorter timeouts",
            default=False,
        )

    def handle(self, *args, **options) -> None:
        # Resolve consumption directory
        directory = options.get("directory")
        if not directory:
            directory = getattr(settings, "CONSUMPTION_DIR", None)
        if not directory:
            raise CommandError("CONSUMPTION_DIR is not configured")

        directory = Path(directory).resolve()

        if not directory.exists():
            raise CommandError(f"Consumption directory does not exist: {directory}")

        if not directory.is_dir():
            raise CommandError(f"Consumption path is not a directory: {directory}")

        # Ensure scratch directory exists
        settings.SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

        # Get settings
        recursive: bool = settings.CONSUMER_RECURSIVE
        subdirs_as_tags: bool = settings.CONSUMER_SUBDIRS_AS_TAGS
        polling_interval: float = settings.CONSUMER_POLLING_INTERVAL
        stability_delay: float = settings.CONSUMER_STABILITY_DELAY
        ignore_patterns: list[str] = settings.CONSUMER_IGNORE_PATTERNS
        ignore_dirs: list[str] = settings.CONSUMER_IGNORE_DIRS
        is_testing: bool = options.get("testing", False)
        is_oneshot: bool = options.get("oneshot", False)

        # Create filter
        consumer_filter = ConsumerFilter(
            ignore_patterns=ignore_patterns,
            ignore_dirs=ignore_dirs,
        )

        # Process existing files
        queued = self._process_existing_files(
            directory=directory,
            recursive=recursive,
            subdirs_as_tags=subdirs_as_tags,
            consumer_filter=consumer_filter,
        )

        if is_oneshot:
            logger.info("Oneshot mode: processed existing files, exiting")
            return

        # Start watching
        self._watch_directory(
            directory=directory,
            recursive=recursive,
            subdirs_as_tags=subdirs_as_tags,
            consumer_filter=consumer_filter,
            polling_interval=polling_interval,
            stability_delay=stability_delay,
            is_testing=is_testing,
            queued=queued,
        )

        logger.debug("Consumer exiting")

    def _process_existing_files(
        self,
        *,
        directory: Path,
        recursive: bool,
        subdirs_as_tags: bool,
        consumer_filter: ConsumerFilter,
    ) -> set[Path]:
        """
        Process any existing files in the consumption directory.

        Returns the set of resolved paths that were queued, so the watch loop
        can seed its in-flight set and avoid re-queuing them on the first
        rescan before the consume tasks have removed them from disk.
        """
        logger.info(f"Processing existing files in {directory}")

        glob_pattern = "**/*" if recursive else "*"
        queued: set[Path] = set()

        for filepath in directory.glob(glob_pattern):
            # Use filter to check if file should be processed
            if not filepath.is_file():
                continue

            if not consumer_filter(Change.added, str(filepath)):
                continue

            if _consume_file(
                filepath=filepath,
                consumption_dir=directory,
                subdirs_as_tags=subdirs_as_tags,
            ):
                queued.add(filepath.resolve())

        return queued

    def _rescan_existing_files(
        self,
        *,
        directory: Path,
        recursive: bool,
        consumer_filter: ConsumerFilter,
        tracker: FileStabilityTracker,
        queued: set[Path],
    ) -> None:
        """
        Re-inject on-disk files the watcher never reported into the tracker.

        Acts as a safety net for files stranded by the watcher-recreation gap
        (see ``rescan_interval_s``). Files already being tracked or already
        queued and awaiting consumption are skipped, so a file is never queued
        twice. Queued paths that have since left the directory are pruned so a
        later file reusing the same name is not skipped forever.
        """
        # Prune in-flight paths that have left the directory
        for path in list(queued):
            if not path.exists():
                queued.discard(path)

        glob_pattern = "**/*" if recursive else "*"

        for filepath in directory.glob(glob_pattern):
            if not filepath.is_file():
                continue

            if not consumer_filter(Change.added, str(filepath)):
                continue

            resolved = filepath.resolve()
            if tracker.is_tracking(resolved) or resolved in queued:
                continue

            logger.debug(f"Rescan found untracked file: {resolved}")
            tracker.track(resolved, Change.added)

    def _watch_directory(
        self,
        *,
        directory: Path,
        recursive: bool,
        subdirs_as_tags: bool,
        consumer_filter: ConsumerFilter,
        polling_interval: float,
        stability_delay: float,
        is_testing: bool,
        queued: set[Path] | None = None,
    ) -> None:
        """Watch directory for changes and process stable files."""
        use_polling = polling_interval > 0
        poll_delay_ms = int(polling_interval * 1000) if use_polling else 0

        # Resolved paths that have been queued and are awaiting consumption.
        # Seeded from the startup scan so the first rescan does not re-queue
        # files whose consume tasks have not yet removed them from disk.
        queued = set() if queued is None else queued

        # Full-glob safety net cadence (0 disables)
        rescan_interval_s = self.rescan_interval_s
        rescan_timeout_ms = (
            int(rescan_interval_s * 1000) if rescan_interval_s > 0 else 0
        )
        last_rescan = monotonic()

        if use_polling:
            logger.info(
                f"Watching {directory} using polling (interval: {polling_interval}s)",
            )
        else:
            logger.info(f"Watching {directory} using native file system events")

        # Create stability tracker
        tracker = FileStabilityTracker(stability_delay=stability_delay)

        # Calculate timeouts
        stability_timeout_ms = int(stability_delay * 1000)
        testing_timeout_ms = int(self.testing_timeout_s * 1000)

        def cap_for_rescan(ms: int) -> int:
            """
            Ensure the watch loop wakes often enough to run the rescan.

            ``watch()`` blocks for up to ``rust_timeout``, so the rescan can
            only run that often. A timeout of 0 means "wait indefinitely",
            which would never wake to rescan; cap it at the rescan interval.
            """
            if rescan_timeout_ms <= 0:
                return ms
            if ms <= 0:
                return rescan_timeout_ms
            return min(ms, rescan_timeout_ms)

        # Calculate appropriate timeout for watch loop
        # In polling mode, rust_timeout must be significantly longer than poll_delay_ms
        # to ensure poll cycles can complete before timing out
        if is_testing:
            if use_polling:
                # For polling: timeout must be at least 3x the poll interval to allow
                # multiple poll cycles. This prevents timeouts from interfering with
                # the polling mechanism.
                min_polling_timeout_ms = poll_delay_ms * 3
                timeout_ms = max(min_polling_timeout_ms, testing_timeout_ms)
            else:
                # For native watching, use short timeout to check stop flag
                timeout_ms = testing_timeout_ms
        else:
            # Not testing, wait indefinitely for first event
            timeout_ms = 0

        timeout_ms = cap_for_rescan(timeout_ms)

        self.stop_flag.clear()

        while not self.stop_flag.is_set():
            try:
                for changes in watch(
                    directory,
                    watch_filter=consumer_filter,
                    rust_timeout=timeout_ms,
                    yield_on_timeout=True,
                    force_polling=use_polling,
                    poll_delay_ms=poll_delay_ms,
                    recursive=recursive,
                    stop_event=self.stop_flag,
                ):
                    # Process each change
                    for change_type, path in changes:
                        path = Path(path).resolve()
                        if change_type == Change.deleted:
                            # Consumed (or otherwise removed); a later file
                            # reusing this name must not be skipped as
                            # already-queued.
                            queued.discard(path)
                        if not path.is_file():
                            continue
                        if path in queued:
                            # Already queued and awaiting consumption; a stray
                            # event (NAS metadata touch, AV scan, etc.) while
                            # the file sits on disk mid-consumption must not
                            # cause it to be queued a second time (GH #13511).
                            logger.debug(f"Ignoring event for queued file: {path}")
                            continue
                        logger.debug(f"Event: {change_type.name} for {path}")
                        tracker.track(path, change_type)

                    # Check for stable files
                    for stable_path in tracker.get_stable_files():
                        # Only remember files that were actually queued, so the
                        # rescan does not re-queue them while the consume task
                        # has yet to remove them from disk, but does retry a
                        # failed publish instead of stranding it
                        if _consume_file(
                            filepath=stable_path,
                            consumption_dir=directory,
                            subdirs_as_tags=subdirs_as_tags,
                        ):
                            queued.add(stable_path)

                    # Exit watch loop to reconfigure timeout
                    break

                # Periodic full-glob safety net for files the watcher missed
                if rescan_timeout_ms > 0 and (
                    monotonic() - last_rescan >= rescan_interval_s
                ):
                    self._rescan_existing_files(
                        directory=directory,
                        recursive=recursive,
                        consumer_filter=consumer_filter,
                        tracker=tracker,
                        queued=queued,
                    )
                    last_rescan = monotonic()

                # Determine next timeout
                if tracker.has_pending_files():
                    # Check pending files at stability interval
                    timeout_ms = stability_timeout_ms
                elif is_testing:
                    # In testing, use appropriate timeout based on watch mode
                    if use_polling:
                        # For polling: ensure timeout allows polls to complete
                        min_polling_timeout_ms = poll_delay_ms * 3
                        timeout_ms = max(min_polling_timeout_ms, testing_timeout_ms)
                    else:
                        # For native watching, use short timeout to check stop flag
                        timeout_ms = testing_timeout_ms
                else:  # pragma: nocover
                    # No pending files, wait indefinitely
                    timeout_ms = 0

                timeout_ms = cap_for_rescan(timeout_ms)

            except KeyboardInterrupt:  # pragma: nocover
                logger.info("Received interrupt, stopping consumer")
                self.stop_flag.set()
