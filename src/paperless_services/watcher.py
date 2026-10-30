"""Watch a shared consumption directory and submit its paths over the API."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import time
import uuid
from pathlib import Path
from threading import Event
from time import monotonic
from typing import TYPE_CHECKING

from watchfiles import Change
from watchfiles import watch

from paperless_services.filtering import FileFilter
from paperless_services.watching import FileStabilityTracker

if TYPE_CHECKING:
    from paperless_services.http import APIClient

logger = logging.getLogger("paperless.api-consumer")
ENDPOINT = "/api/services/consumer/"
RETRY_SECONDS = 5.0
RESCAN_SECONDS = 300.0


class Watcher:
    """Keep pending request identities and admitted paths across restarts."""

    def __init__(self, directory: Path, state_file: Path, client: APIClient) -> None:
        """Read the journal without discarding corrupt or unreadable state."""
        self.directory = directory.resolve()
        self.state_file = state_file
        self.client = client
        self.records = json.loads(state_file.read_text()) if state_file.exists() else {}
        self.stop_event = Event()

    def save(self) -> None:
        """Atomically persist request identity before making an HTTP request."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_file.with_name(self.state_file.name + ".tmp")
        with temporary.open("w") as output:
            os.fchmod(output.fileno(), 0o600)
            json.dump(self.records, output)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(self.state_file)

    def configure(self) -> None:
        """Use the server's parser extensions and existing watcher settings."""
        if not self.directory.is_dir():
            raise ValueError(f"Consumption directory does not exist: {self.directory}")
        self.policy = self.client.request("GET", ENDPOINT)
        self.file_filter = FileFilter(
            supported_extensions=frozenset(self.policy["supported_extensions"]),
            ignore_patterns=self.policy["ignore_patterns"],
            ignore_dirs=self.policy["ignore_dirs"],
        )
        self.tracker = FileStabilityTracker(self.policy["stability_delay"])

    def relative_path(self, path: Path) -> str | None:
        """Keep local symlinks from submitting paths outside the shared root."""
        try:
            return path.resolve().relative_to(self.directory).as_posix()
        except ValueError:
            logger.warning("Ignoring path outside consumption directory: %s", path)
            return None

    def forget(self, relative: str) -> None:
        """Allow a disappeared or rejected path to be considered again."""
        if self.records.pop(relative, None) is not None:
            self.save()

    def submit(self, path: Path) -> None:
        """Retry uncertain HTTP responses with the same persisted request ID."""
        relative = self.relative_path(path)
        if relative is None:
            return
        if not path.is_file():
            self.forget(relative)
            return
        record = self.records.get(relative)
        if record and record["status"] != "pending":
            return
        if record is None:
            stats = path.stat()
            record = {
                "request_id": str(uuid.uuid4()),
                "size": stats.st_size,
                "mtime_ns": stats.st_mtime_ns,
                "status": "pending",
            }
            self.records[relative] = record
        self.save()
        try:
            response = self.client.request(
                "POST",
                ENDPOINT,
                {
                    "path": relative,
                    "request_id": record["request_id"],
                    "size": record["size"],
                    "mtime_ns": record["mtime_ns"],
                },
            )
        except (OSError, ValueError) as error:
            logger.warning("Will retry consumption request for %s: %s", relative, error)
            return
        status = response["status"]
        if status in {"queued", "publication_uncertain"}:
            record.update(status=status, task_id=response["task_id"])
            self.save()
            if status == "publication_uncertain":
                logger.warning(
                    "Publication uncertain for %s (task %s); inspect task status before retrying",
                    relative,
                    response["task_id"],
                )
        elif status in {"ignored", "missing", "changed"}:
            self.forget(relative)
            if status == "changed":
                self.tracker.track(path, Change.modified)
        else:
            raise ValueError(f"Unexpected consumption response: {status}")

    def scan(self, *, startup: bool = False) -> None:
        """Preserve immediate startup scanning and the periodic recovery scan."""
        vanished = [
            relative
            for relative in self.records
            if not (self.directory / relative).exists()
        ]
        for relative in vanished:
            del self.records[relative]
        if vanished:
            self.save()
        pattern = "**/*" if self.policy["recursive"] else "*"
        for path in self.directory.glob(pattern):
            if not path.is_file() or not self.file_filter(Change.added, str(path)):
                continue
            relative = self.relative_path(path)
            if relative is None or relative in self.records:
                continue
            if startup:
                self.submit(path)
            elif not self.tracker.is_tracking(path):
                self.tracker.track(path, Change.added)

    def retry_pending(self) -> None:
        """Retry publication requests, never completed or failed processing tasks."""
        for relative, record in list(self.records.items()):
            if record["status"] == "pending":
                self.submit(self.directory / relative)

    def run(self) -> None:
        """Watch native events or polling batches, retaining missed-file recovery."""
        self.configure()
        self.retry_pending()
        self.scan(startup=True)
        polling = self.policy["polling_interval"]
        timeout_ms = max(
            100,
            int(min(self.policy["stability_delay"], RETRY_SECONDS) * 1000),
        )
        if polling > 0:
            timeout_ms = max(timeout_ms, int(polling * 3000))
        timeout_ms = min(timeout_ms, int(RESCAN_SECONDS * 1000))
        last_retry = last_rescan = monotonic()
        logger.info("Watching %s through the consumption API", self.directory)
        for changes in watch(
            self.directory,
            watch_filter=self.file_filter,
            recursive=self.policy["recursive"],
            force_polling=polling > 0,
            poll_delay_ms=int(polling * 1000),
            rust_timeout=timeout_ms,
            yield_on_timeout=True,
            stop_event=self.stop_event,
        ):
            for change, raw_path in changes:
                path = Path(raw_path).resolve()
                relative = self.relative_path(path)
                if relative is None:
                    continue
                if change == Change.deleted:
                    self.forget(relative)
                    self.tracker.track(path, change)
                elif path.is_file() and relative not in self.records:
                    self.tracker.track(path, change)
            for path in self.tracker.get_stable_files():
                self.submit(path)
            now = monotonic()
            if now - last_retry >= RETRY_SECONDS:
                self.retry_pending()
                last_retry = now
            if now - last_rescan >= RESCAN_SECONDS:
                self.scan()
                last_rescan = now


def main() -> None:
    """Run one watcher with a persistent journal and exclusive local lock."""
    from paperless_services.http import APIClient

    logging.basicConfig(level=logging.INFO)
    directory = Path(
        os.getenv("PAPERLESS_CONSUMPTION_DIR", "/usr/src/paperless/consume"),
    )
    state_file = Path(
        os.getenv(
            "PAPERLESS_WATCHER_STATE",
            str(
                Path(os.getenv("PAPERLESS_DATA_DIR", "/usr/src/paperless/data"))
                / "api-watcher.json",
            ),
        ),
    )
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.with_name(state_file.name + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        watcher = Watcher(directory, state_file, APIClient())
        while not watcher.stop_event.is_set():
            try:
                watcher.run()
            except (OSError, ValueError) as error:
                logger.warning("Watcher will retry in %ss: %s", RETRY_SECONDS, error)
                time.sleep(RETRY_SECONDS)


if __name__ == "__main__":
    main()
