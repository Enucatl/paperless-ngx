"""Check durable parent delivery without loading Django into the parent."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from paperless_worker import outcomes

TRACKED = {"paperless_mail.tasks.process_mail_accounts": "mail_fetch"}
TASK = next(iter(TRACKED))


def test_import_does_not_initialize_django() -> None:
    """The controller-side transport has no Django import side effects."""
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import paperless_worker.outcomes; assert 'django' not in sys.modules",
        ],
        check=True,
        cwd=Path(__file__).resolve().parents[2],
    )


def test_durable_record_survives_failed_start_and_controller_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed subprocess launch does not acknowledge or discard history."""
    monkeypatch.setattr(
        outcomes.subprocess,
        "Popen",
        Mock(side_effect=OSError("unavailable")),
    )
    writer = outcomes.OutcomeWriter(tmp_path, TRACKED)
    writer.record_failed("task-1", TASK, "WorkerLostError", "child exited")
    paths = list(writer.directory.glob("*.json"))
    assert len(paths) == 1
    assert paths[0].stat().st_mode & 0o777 == 0o600
    assert writer.directory.stat().st_mode & 0o777 == 0o700
    event = json.loads(paths[0].read_text())
    assert event["result_data"] == {
        "error_type": "WorkerLostError",
        "error_message": "child exited",
    }
    helper = Mock()
    helper.poll.return_value = None
    launch = Mock(return_value=helper)
    monkeypatch.setattr(outcomes.subprocess, "Popen", launch)
    restarted = outcomes.OutcomeWriter(tmp_path, TRACKED)
    restarted.tick()
    assert launch.call_count == 1
    assert paths[0].exists()
    restarted.tick()
    assert launch.call_count == 1


def test_overdue_helper_is_killed_and_pending_records_replayed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stuck database/helper cannot become a permanent resident process."""
    clock = [100.0]
    monkeypatch.setattr(outcomes.time, "monotonic", lambda: clock[0])
    first, second = Mock(), Mock()
    first.poll.return_value = None
    second.poll.return_value = None
    launch = Mock(side_effect=[first, second])
    monkeypatch.setattr(outcomes.subprocess, "Popen", launch)
    writer = outcomes.OutcomeWriter(tmp_path, TRACKED)
    writer.record_revoked("task-1", TASK, expired=True)
    clock[0] += outcomes.HELPER_TIMEOUT + 1
    writer.tick()
    first.kill.assert_called_once()
    first.poll.return_value = -9
    writer.tick()
    assert writer.helper is second
    assert launch.call_count == 2
    assert len(list(writer.directory.glob("*.json"))) == 1
    writer.close()
    second.kill.assert_called_once()
    second.wait.assert_called_once_with(timeout=5)


def test_child_process_does_not_run_inherited_parent_callbacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forked children keep their normal Django task-state handlers."""
    writer = outcomes.OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(writer, "record_failed", Mock())
    monkeypatch.setattr(outcomes.os, "getpid", lambda: writer.pid + 1)
    writer.on_failure(
        sender=Mock(name=TASK),
        task_id="task-1",
        exception=ValueError("bad"),
    )
    writer.record_failed.assert_not_called()


def test_spool_failure_is_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not claim durability when the filesystem cannot persist a record."""
    writer = outcomes.OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(outcomes.os, "fsync", Mock(side_effect=OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        writer.record_revoked("task-1", TASK)
    assert not list(writer.directory.iterdir())


def test_duplicate_request_retains_original_timestamp(tmp_path: Path) -> None:
    """A repeated callback cannot make an old outcome look like a newer attempt."""
    writer = outcomes.OutcomeWriter(tmp_path, TRACKED)
    request = type("Request", (), {})()
    assert writer._occurred(request) == writer._occurred(request)


def test_inline_callback_start_failure_retains_authenticated_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A synchronous caller sees failure while restart replay retains the call."""
    monkeypatch.setattr(
        outcomes.subprocess,
        "Popen",
        Mock(side_effect=OSError("unavailable")),
    )
    writer = outcomes.OutcomeWriter(tmp_path, TRACKED, secret_key=b"test-key")
    with pytest.raises(RuntimeError, match="durable record"):
        writer.run_inline_task("paperless_mail.mail.error_callback", ("original",), {})
    record = json.loads(next(writer.directory.glob("*.json")).read_text())
    assert record["kind"] == "callback"
    assert "test-key" not in json.dumps(record)


@pytest.mark.parametrize(
    "change",
    [
        {"status": "success"},
        {"task_name": "unknown"},
        {"task_id": "x" * 73},
        {"completed_at": "2026-10-30T01:00:00"},
        {"result_data": {"reason": "x", "command": "unsafe"}},
    ],
)
def test_rejects_non_allowlisted_records(change: dict) -> None:
    """Spool input is data, never arbitrary task dispatch or ORM fields."""
    event = {
        "task_id": "task-1",
        "task_name": TASK,
        "status": "revoked",
        "completed_at": "2026-10-30T01:00:00+00:00",
        "result_data": {"reason": "expired"},
        **change,
    }
    with pytest.raises(ValueError):
        outcomes.validate_outcome(event, TRACKED)
