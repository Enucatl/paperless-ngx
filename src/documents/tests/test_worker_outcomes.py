"""Check outcome replay against real task-history rows."""

import base64
from datetime import timedelta
from pathlib import Path

import pytest
from django.db import OperationalError
from django.utils import timezone

from documents.models import PaperlessTask
from paperless_testing.factories import PaperlessTaskFactory
from paperless_worker import outcome_helper
from paperless_worker.outcomes import OutcomeWriter
from paperless_worker.serialization import dumps

TASK = "paperless_mail.tasks.process_mail_accounts"
TRACKED = {TASK: "mail_fetch"}


@pytest.mark.django_db
@pytest.mark.parametrize("status", PaperlessTask.COMPLETE_STATUSES)
def test_final_history_is_not_replaced_by_replay(status: str) -> None:
    """Duplicate or delayed parent callbacks cannot overwrite a final result."""
    done = timezone.now()
    task = PaperlessTaskFactory(
        status=status,
        result_data={"original": True},
        date_done=done,
    )
    outcome_helper.apply_outcome(
        task.task_id,
        "failure",
        {"error_type": "WorkerLostError"},
        done + timedelta(seconds=10),
    )
    task.refresh_from_db()
    assert task.status == status
    assert task.result_data == {"original": True}
    assert task.date_done == done


@pytest.mark.django_db
def test_old_event_does_not_fail_newer_attempt() -> None:
    """Spool replay uses occurrence time rather than helper execution time."""
    now = timezone.now()
    task = PaperlessTaskFactory(status="started", date_started=now)
    outcome_helper.apply_outcome(
        task.task_id,
        "failure",
        {"error_type": "WorkerLostError"},
        now - timedelta(seconds=1),
    )
    task.refresh_from_db()
    assert task.status == "started"
    assert task.date_done is None


@pytest.mark.django_db
def test_database_failure_retains_record_for_later_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed helper retries the exact original event after recovery."""
    now = timezone.now()
    task = PaperlessTaskFactory(
        task_type="mail_fetch",
        status="started",
        date_started=now - timedelta(seconds=5),
    )
    writer = OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(writer, "tick", lambda: None)
    writer.record_failed(
        task.task_id,
        TASK,
        "WorkerLostError",
        "child died",
        completed_at=now,
    )
    original = outcome_helper.apply_outcome

    def unavailable(*args, **kwargs):
        raise OperationalError("database unavailable")

    monkeypatch.setattr(outcome_helper, "apply_outcome", unavailable)
    assert outcome_helper.drain(writer.directory) is False
    assert len(list(writer.directory.glob("*.json"))) == 1
    monkeypatch.setattr(outcome_helper, "apply_outcome", original)
    assert outcome_helper.drain(writer.directory) is True
    assert not list(writer.directory.glob("*.json"))
    task.refresh_from_db()
    assert task.status == "failure"
    assert task.date_done == now
    assert task.duration_seconds == pytest.approx(5)
    assert task.result_data["error_type"] == "WorkerLostError"


@pytest.mark.django_db
def test_missing_publication_row_is_retained_until_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transaction-delayed history row must not silently lose cancellation."""
    writer = OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(writer, "tick", lambda: None)
    writer.record_revoked("not-yet-committed", TASK, expired=True)
    assert outcome_helper.drain(writer.directory) is False
    assert len(list(writer.directory.glob("*.json"))) == 1
    task = PaperlessTaskFactory(task_id="not-yet-committed", task_type="mail_fetch")
    assert outcome_helper.drain(writer.directory) is True
    task.refresh_from_db()
    assert task.status == "revoked"
    assert "expired" in task.result_data["reason"]


@pytest.mark.django_db
def test_malformed_record_does_not_block_valid_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid records remain visible while independent valid records drain."""
    task = PaperlessTaskFactory(task_type="mail_fetch")
    writer = OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(writer, "tick", lambda: None)
    (writer.directory / "invalid.json").write_text('{"status": "success"}')
    writer.record_revoked(task.task_id, TASK)
    assert outcome_helper.drain(writer.directory) is False
    pending = list(writer.directory.glob("*.json"))
    assert len(pending) == 1
    assert pending[0].read_text() == '{"status": "success"}'
    task.refresh_from_db()
    assert task.status == "revoked"


@pytest.mark.django_db
def test_failed_batch_does_not_starve_later_valid_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every retained error moves behind events not yet attempted by a helper."""
    task = PaperlessTaskFactory(task_type="mail_fetch")
    writer = OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(writer, "tick", lambda: None)
    for number in range(100):
        (writer.directory / f"0-{number:03}.json").write_text("{}")
    writer.record_revoked(task.task_id, TASK)
    assert outcome_helper.drain(writer.directory) is False
    task.refresh_from_db()
    assert task.status == "pending"
    assert outcome_helper.drain(writer.directory) is False
    task.refresh_from_db()
    assert task.status == "revoked"
    assert len(list(writer.directory.glob("*.json"))) == 100


@pytest.mark.django_db
def test_signed_inline_callback_preserves_complete_arguments(settings) -> None:
    """The helper calls the registered task with authenticated Context and dates."""
    from celery.app.task import Context

    from paperless.celery import app

    received = []

    @app.task(name="paperless.tests.inline_callback")
    def callback(request, exception, traceback, *, when):
        received.append((request, exception, traceback, when))

    now = timezone.now()
    args = (
        Context(id="failed-task", retries=2),
        ValueError("original error"),
        "original traceback",
    )
    payload = dumps((callback.name, args, {"when": now}), settings.SECRET_KEY.encode())
    record = {"kind": "callback", "payload": base64.b64encode(payload).decode()}
    outcome_helper.apply_callback(record)
    assert received[0][0].id == "failed-task"
    assert received[0][0].retries == 2
    assert isinstance(received[0][1], ValueError)
    assert str(received[0][1]) == "original error"
    assert received[0][2:] == ("original traceback", now)
    app.tasks.pop(callback.name)


@pytest.mark.django_db
def test_callback_rejects_bad_signature_without_execution(settings) -> None:
    """Private spool files still require the existing task signing boundary."""
    from paperless_worker.serialization import SignedPickleError

    payload = dumps(("paperless_mail.mail.error_callback", (), {}), b"wrong-key")
    with pytest.raises(SignedPickleError):
        outcome_helper.apply_callback(
            {"kind": "callback", "payload": base64.b64encode(payload).decode()},
        )


@pytest.mark.django_db
def test_failed_callback_keeps_its_acknowledgement_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry scheduling must never look like successful callback acknowledgement."""
    writer = OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(writer, "tick", lambda: None)
    payload = dumps(("unknown.callback", (), {}), b"wrong-key")
    path = writer._write(
        {"kind": "callback", "payload": base64.b64encode(payload).decode()},
    )
    original = path.read_bytes()
    assert outcome_helper.drain(writer.directory) is False
    assert path.read_bytes() == original
    assert list(writer.directory.glob("*.json")) == [path]


@pytest.mark.django_db
def test_interrupted_delivery_does_not_starve_later_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A helper killed during its oldest record still advances replay ordering."""
    stuck = PaperlessTaskFactory(task_type="mail_fetch")
    later = PaperlessTaskFactory(task_type="mail_fetch")
    writer = OutcomeWriter(tmp_path, TRACKED)
    monkeypatch.setattr(writer, "tick", lambda: None)
    writer.record_revoked(stuck.task_id, TASK)
    writer.record_revoked(later.task_id, TASK)
    original_mtime = max(
        path.stat().st_mtime_ns for path in writer.directory.glob("*.json")
    )
    original = outcome_helper.apply_outcome

    def interrupted(task_id, *args, **kwargs):
        if task_id == stuck.task_id:
            raise SystemExit("helper interrupted during delivery")
        return original(task_id, *args, **kwargs)

    monkeypatch.setattr(outcome_helper, "apply_outcome", interrupted)
    with pytest.raises(SystemExit):
        outcome_helper.drain(writer.directory)
    assert (
        max(path.stat().st_mtime_ns for path in writer.directory.glob("*.json"))
        > original_mtime
    )
    with pytest.raises(SystemExit):
        outcome_helper.drain(writer.directory)
    later.refresh_from_db()
    assert later.status == "revoked"
    assert len(list(writer.directory.glob("*.json"))) == 1
