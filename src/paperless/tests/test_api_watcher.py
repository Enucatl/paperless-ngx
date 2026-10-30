"""Keep API watcher retries and filesystem behavior safe without Django imports."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING
from urllib.error import URLError

import pytest
from watchfiles import Change

from paperless_services import watcher as watcher_module
from paperless_services import watching
from paperless_services.watcher import ENDPOINT
from paperless_services.watcher import Watcher

if TYPE_CHECKING:
    from collections.abc import Iterator
    from unittest.mock import MagicMock

    from pytest_mock import MockerFixture


@pytest.fixture
def api(mocker: MockerFixture) -> MagicMock:
    """Provide the server's policy and successful publication responses."""
    client = mocker.Mock()
    policy = {
        "recursive": True,
        "subdirs_as_tags": True,
        "polling_interval": 0,
        "stability_delay": 1,
        "ignore_patterns": [r"^ignored"],
        "ignore_dirs": ["excluded"],
        "supported_extensions": [".pdf", ".txt"],
    }
    client.request.side_effect = lambda method, path, data=None: (
        policy
        if method == "GET"
        else {"status": "queued", "task_id": data["request_id"]}
    )
    return client


@pytest.fixture
def watcher(tmp_path: Path, api: MagicMock) -> Watcher:
    """Create a watcher with separate consume and durable state directories."""
    directory = tmp_path / "consume"
    directory.mkdir()
    instance = Watcher(directory, tmp_path / "state" / "watcher.json", api)
    instance.configure()
    return instance


def test_journal_precedes_post_and_survives_lost_response(
    watcher: Watcher,
    api: MagicMock,
) -> None:
    """A restarted watcher reuses the ID already accepted by the server."""
    source = watcher.directory / "invoice.pdf"
    source.write_bytes(b"document")
    accepted = []

    def lose_response(method: str, path: str, data: dict) -> None:
        """Model the server accepting publication before the HTTP response is lost."""
        assert method == "POST" and path == ENDPOINT
        journal = json.loads(watcher.state_file.read_text())["invoice.pdf"]
        assert journal["status"] == "pending"
        assert journal["request_id"] == data["request_id"]
        assert watcher.state_file.stat().st_mode & 0o777 == 0o600
        accepted.append(data)
        raise URLError("response lost")

    api.request.side_effect = lose_response
    watcher.submit(source)
    restarted = Watcher(watcher.directory, watcher.state_file, api)
    api.request.side_effect = lambda method, path, data: {
        "status": "queued",
        "task_id": data["request_id"],
    }
    restarted.retry_pending()
    assert api.request.call_args.args[2] == accepted[0]
    assert restarted.records["invoice.pdf"]["status"] == "queued"
    assert source.read_bytes() == b"document"


def test_unwritten_journal_prevents_post(
    watcher: Watcher,
    api: MagicMock,
    mocker: MockerFixture,
) -> None:
    """A disk failure cannot cause publication without its retry identity."""
    source = watcher.directory / "invoice.pdf"
    source.write_bytes(b"document")
    api.request.reset_mock()
    mocker.patch.object(watcher, "save", side_effect=OSError("disk full"))
    with pytest.raises(OSError, match="disk full"):
        watcher.submit(source)
    with pytest.raises(OSError, match="disk full"):
        watcher.retry_pending()
    api.request.assert_not_called()


@pytest.mark.parametrize("status", ["queued", "publication_uncertain"])
def test_admitted_paths_are_not_resubmitted(
    watcher: Watcher,
    api: MagicMock,
    status: str,
) -> None:
    """Later changes or processing failures do not start another task."""
    source = watcher.directory / "invoice.pdf"
    source.write_bytes(b"document")
    api.request.side_effect = lambda method, path, data: {
        "status": status,
        "task_id": data["request_id"],
    }
    watcher.submit(source)
    original_id = watcher.records["invoice.pdf"]["request_id"]
    api.request.reset_mock()
    source.write_bytes(b"modified while queued")
    watcher.submit(source)
    watcher.retry_pending()
    watcher.scan()
    api.request.assert_not_called()
    assert watcher.records["invoice.pdf"]["request_id"] == original_id
    assert source.exists()

    source.unlink()
    watcher.scan()
    assert not watcher.records
    source.write_bytes(b"new document with reused name")
    watcher.submit(source)
    assert watcher.records["invoice.pdf"]["request_id"] != original_id


def test_changed_file_waits_for_stability(
    watcher: Watcher,
    api: MagicMock,
    mocker: MockerFixture,
) -> None:
    """A changed-file response drops the old request and observes a new interval."""
    source = watcher.directory / "invoice.pdf"
    source.write_bytes(b"document")
    mocker.patch.object(watching, "monotonic", return_value=0)
    api.request.return_value = {"status": "changed", "task_id": None}
    api.request.side_effect = None
    watcher.submit(source)
    assert not watcher.records
    assert watcher.tracker.is_tracking(source)
    assert list(watcher.tracker.get_stable_files()) == []
    mocker.patch.object(watching, "monotonic", return_value=2)
    assert list(watcher.tracker.get_stable_files()) == [source]


@pytest.mark.parametrize("status", ["ignored", "missing"])
def test_rejected_request_drops_pending_state(
    watcher: Watcher,
    api: MagicMock,
    status: str,
) -> None:
    """A non-admission response leaves file ownership with the server/writer."""
    source = watcher.directory / "invoice.pdf"
    source.write_bytes(b"document")
    api.request.side_effect = None
    api.request.return_value = {"status": status, "task_id": None}
    watcher.submit(source)
    assert not watcher.records
    assert source.exists()


def test_scan_uses_existing_filter_and_recursive_policy(
    watcher: Watcher,
    api: MagicMock,
) -> None:
    """Keep filename regex, directory exclusions and hidden-file semantics."""
    for relative in (
        "root.pdf",
        "nested/invoice.PDF",
        ".hidden.pdf",
        "ignored.pdf",
        "excluded/skip.pdf",
        "._resource.pdf",
        "unsupported.xyz",
    ):
        path = watcher.directory / relative
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"document")
    watcher.policy["recursive"] = False
    watcher.scan(startup=True)
    assert set(watcher.records) == {"root.pdf", ".hidden.pdf"}
    watcher.policy["recursive"] = True
    watcher.scan(startup=True)
    assert set(watcher.records) == {"root.pdf", ".hidden.pdf", "nested/invoice.PDF"}


def test_rescan_tracks_unreported_files_without_requeue(
    watcher: Watcher,
    api: MagicMock,
) -> None:
    """A missed event goes through stability, while admitted paths stay excluded."""
    admitted = watcher.directory / "admitted.pdf"
    admitted.write_bytes(b"already admitted")
    watcher.submit(admitted)
    missed = watcher.directory / "missed.pdf"
    missed.write_bytes(b"missed event")
    api.request.reset_mock()
    watcher.scan()
    api.request.assert_not_called()
    assert watcher.tracker.is_tracking(missed)
    assert not watcher.tracker.is_tracking(admitted)


def test_outside_symlink_is_never_submitted(
    watcher: Watcher,
    api: MagicMock,
    tmp_path: Path,
) -> None:
    """The shared-root client never sends an outside target as a relative path."""
    outside = tmp_path / "outside.pdf"
    outside.write_bytes(b"outside")
    (watcher.directory / "link.pdf").symlink_to(outside)
    api.request.reset_mock()
    watcher.scan(startup=True)
    api.request.assert_not_called()
    assert outside.read_bytes() == b"outside"


@pytest.mark.parametrize("polling", [0, 0.25])
def test_watch_tracks_runtime_changes_and_polls(
    watcher: Watcher,
    api: MagicMock,
    mocker: MockerFixture,
    polling: float,
) -> None:
    """Runtime empty placeholders wait for content in both native/poll modes."""
    watcher.policy["polling_interval"] = polling
    now = [0.0]
    mocker.patch.object(watching, "monotonic", side_effect=lambda: now[0])
    mocker.patch.object(watcher_module, "monotonic", side_effect=lambda: now[0])
    source = watcher.directory / "scan.pdf"

    def events(*args: object, **kwargs: object) -> Iterator[set[tuple[Change, str]]]:
        """Deliver a scanner placeholder, its completed write, then quiet time."""
        assert kwargs["force_polling"] is (polling > 0)
        assert kwargs["poll_delay_ms"] == int(polling * 1000)
        assert kwargs["recursive"] is True
        source.touch()
        yield {(Change.added, str(source))}
        now[0] = 2
        yield set()
        assert not watcher.records
        source.write_bytes(b"complete scan")
        yield {(Change.modified, str(source))}
        now[0] = 4
        yield set()
        assert watcher.records["scan.pdf"]["status"] == "queued"

    mocker.patch.object(watcher_module, "watch", side_effect=events)
    watcher.run()
    assert source.read_bytes() == b"complete scan"


def test_corrupt_journal_is_not_reset(watcher: Watcher, api: MagicMock) -> None:
    """Invalid persisted state fails visibly instead of duplicating prior work."""
    watcher.state_file.parent.mkdir(parents=True)
    watcher.state_file.write_text("{")
    with pytest.raises(json.JSONDecodeError):
        Watcher(watcher.directory, watcher.state_file, api)


@pytest.mark.parametrize("polling", [0, 0.1])
def test_real_filesystem_watch(
    watcher: Watcher,
    api: MagicMock,
    mocker: MockerFixture,
    polling: float,
) -> None:
    """Use real native/poll watchers while a scanner fills an empty placeholder."""
    watcher.policy.update(stability_delay=0.1, polling_interval=polling)
    ready = Event()
    original_watch = watcher_module.watch

    def observed_watch(*args: object, **kwargs: object) -> Iterator[set]:
        """Signal readiness only after the native/poll watcher has yielded."""
        for changes in original_watch(*args, **kwargs):
            ready.set()
            yield changes

    mocker.patch.object(watcher_module, "watch", side_effect=observed_watch)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(watcher.run)
        try:
            assert ready.wait(timeout=5)
            source = watcher.directory / "scanner.pdf"
            source.touch()
            time.sleep(0.5)
            assert "scanner.pdf" not in watcher.records
            source.write_bytes(b"completed scan")
            # Make the mtime transition explicit for filesystem polling.
            modified = source.stat().st_mtime + 2
            os.utime(source, (modified, modified))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if watcher.records.get("scanner.pdf", {}).get("status") == "queued":
                    break
                time.sleep(0.02)
            assert watcher.records["scanner.pdf"]["status"] == "queued"
            assert source.read_bytes() == b"completed scan"
            source.write_bytes(b"modified after enqueue")
            time.sleep(0.5)
            posts = [
                call for call in api.request.call_args_list if call.args[0] == "POST"
            ]
            assert len(posts) == 1
        finally:
            watcher.stop_event.set()
            future.result(timeout=5)


def test_watcher_import_has_no_django() -> None:
    """The replacement process needs only the filesystem watcher and stdlib."""
    source = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import paperless_services.watcher; assert 'django' not in sys.modules; assert 'celery' not in sys.modules",
        ],
        env={**os.environ, "PYTHONPATH": str(source)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
