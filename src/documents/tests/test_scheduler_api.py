"""Verify configured schedule publication through the superuser API."""

from datetime import datetime
from unittest.mock import Mock

import pytest
from celery import Celery
from celery.schedules import crontab
from django.contrib.auth.models import User
from pytest_django.fixtures import Settings
from pytest_mock import MockerFixture
from rest_framework.authtoken.models import Token
from rest_framework.test import APIClient

from documents import scheduler_api
from documents.models import PaperlessTask
from paperless.settings.custom import parse_beat_schedule
from paperless_testing.factories import UserFactory

pytestmark = [pytest.mark.api, pytest.mark.django_db]
ENDPOINT = "/api/services/scheduler/"


@pytest.fixture
def job(settings: Settings) -> dict:
    """Provide a configured minute-by-minute job with its publication options."""
    entry = {
        "task": "documents.tasks.train_classifier",
        "schedule": crontab(),
        "options": {"expires": 3540.0, "headers": {"trigger_source": "scheduled"}},
    }
    settings.CELERY_BEAT_SCHEDULE = {"Train the classifier": entry}
    settings.CELERY_TIMEZONE = "UTC"
    return entry


@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize("kind", ["anonymous", "regular", "staff", "inactive"])
def test_requires_active_superuser(
    method: str,
    kind: str,
    job: dict,
    mocker: MockerFixture,
) -> None:
    """An ordinary token, staff token or inactive account cannot schedule jobs."""
    client = APIClient()
    if kind != "anonymous":
        user = UserFactory(
            is_staff=kind == "staff",
            is_superuser=kind == "inactive",
            is_active=kind != "inactive",
        )
        token = Token.objects.create(user=user)
        client.credentials(HTTP_AUTHORIZATION=f"Token {token.key}")
    publish = mocker.patch.object(scheduler_api.app, "send_task")
    response = getattr(client, method)(
        ENDPOINT,
        {"name": "Train the classifier"},
        format="json",
    )
    assert response.status_code in (401, 403)
    publish.assert_not_called()


def test_superuser_token_reads_resolved_schedule(
    admin_user: User,
    settings: Settings,
    mocker: MockerFixture,
) -> None:
    """The response preserves resolved mail offsets and excludes disabled jobs."""
    mocker.patch.dict(
        "os.environ",
        {
            "PAPERLESS_SECRET_KEY": "scheduler-test-secret",
            "PAPERLESS_INDEX_TASK_CRON": "disable",
        },
    )
    settings.CELERY_BEAT_SCHEDULE = parse_beat_schedule()
    settings.CELERY_TIMEZONE = "Europe/Rome"
    token = Token.objects.create(user=admin_user)
    client = APIClient()
    client.credentials(HTTP_AUTHORIZATION=f"Token {token.key}")
    response = client.get(ENDPOINT)
    assert response.status_code == 200
    assert response.data["timezone"] == "Europe/Rome"
    jobs = {entry["name"]: entry["cron"] for entry in response.data["jobs"]}
    assert "Optimize the index" not in jobs
    assert jobs["Perform sanity check"] == "30 0 * * 0"
    minutes = settings.CELERY_BEAT_SCHEDULE["Check all e-mail accounts"][
        "schedule"
    ].minute
    assert (
        jobs["Check all e-mail accounts"]
        == f"{','.join(map(str, sorted(minutes)))} * * * *"
    )
    assert "scheduler-test-secret" not in response.content.decode()


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "disabled"},
        {"task": "documents.tasks.empty_trash"},
        {"name": "Train the classifier", "options": {"expires": 0}},
        {"name": "Train the classifier", "args": [1]},
    ],
)
def test_rejects_unknown_jobs_and_client_overrides(
    admin_client: APIClient,
    job: dict,
    payload: dict,
    mocker: MockerFixture,
) -> None:
    """Only an enabled schedule name may control publication."""
    publish = mocker.patch.object(scheduler_api.app, "send_task")
    response = admin_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 400
    publish.assert_not_called()


def test_empty_schedule(admin_client: APIClient, settings: Settings) -> None:
    """Disabling all jobs produces an empty configuration for Supercronic."""
    settings.CELERY_BEAT_SCHEDULE = {}
    response = admin_client.get(ENDPOINT)
    assert response.status_code == 200
    assert response.data["jobs"] == []
    assert (
        admin_client.post(
            ENDPOINT,
            {"name": "Train the classifier"},
            format="json",
        ).status_code
        == 400
    )


@pytest.mark.parametrize(
    ("timestamp", "dispatch"),
    [
        ("2026-11-02T23:15:00+00:00", True),
        ("2026-11-09T23:15:00+00:00", False),
        ("2026-11-03T23:15:00+00:00", False),
        ("2026-11-02T23:16:00+00:00", False),
        ("2026-11-02T22:15:00+00:00", False),
        ("2026-12-02T23:15:00+00:00", False),
    ],
)
def test_calendar_uses_timezone_and_both_day_constraints(
    admin_client: APIClient,
    settings: Settings,
    job: dict,
    mocker: MockerFixture,
    timestamp: str,
    *,
    dispatch: bool,
) -> None:
    """Only the configured minute matches; DOM and weekday retain AND semantics."""
    settings.CELERY_TIMEZONE = "Europe/Rome"
    job["schedule"] = crontab(
        minute="15",
        hour="0",
        day_of_month="3",
        month_of_year="11",
        day_of_week="tue",
    )
    mocker.patch.object(
        scheduler_api.timezone,
        "now",
        return_value=datetime.fromisoformat(timestamp),
    )
    publish = mocker.patch.object(
        scheduler_api.app,
        "send_task",
        return_value=Mock(id="queued-task-id"),
    )
    configuration = admin_client.get(ENDPOINT)
    assert configuration.data["jobs"] == [
        {"name": "Train the classifier", "cron": "15 0 * 11 2"},
    ]
    response = admin_client.post(
        ENDPOINT,
        {"name": "Train the classifier"},
        format="json",
    )
    assert response.status_code == 200
    if dispatch:
        assert response.data == {"task_id": "queued-task-id"}
        publish.assert_called_once_with(
            job["task"],
            args=(),
            kwargs={},
            **job["options"],
        )
    else:
        assert response.data == {"task_id": None, "skipped": True}
        publish.assert_not_called()


def test_publication_keeps_signed_serializer_and_task_history(
    admin_client: APIClient,
    job: dict,
    mocker: MockerFixture,
) -> None:
    """Real Celery publication signs its message and creates scheduled task history."""
    with Celery(
        "scheduler-api-test",
        broker="memory://",
        backend="cache+memory://",
        set_as_current=False,
    ) as app:
        app.conf.task_serializer = scheduler_api.app.conf.task_serializer
        assert app.conf.task_serializer == "signed-pickle"
        app.conf.accept_content = ["application/x-signed-pickle"]
        mocker.patch.object(scheduler_api, "app", app)
        response = admin_client.post(
            ENDPOINT,
            {"name": "Train the classifier"},
            format="json",
        )
        assert response.status_code == 200
        task = PaperlessTask.objects.get(task_id=response.data["task_id"])
        assert task.task_type == PaperlessTask.TaskType.TRAIN_CLASSIFIER
        assert task.trigger_source == PaperlessTask.TriggerSource.SCHEDULED
        with app.connection_for_read() as connection:
            queue = connection.SimpleQueue(
                "celery",
                accept=["application/x-signed-pickle"],
            )
            message = queue.get(block=False)
            try:
                assert message.content_type == "application/x-signed-pickle"
                assert message.headers["trigger_source"] == "scheduled"
                assert message.headers["task"] == job["task"]
                assert message.headers["expires"] is not None
                assert message.payload[0] == ()
                assert message.payload[1] == {}
            finally:
                message.ack()
                queue.close()
