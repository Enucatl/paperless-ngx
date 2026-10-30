"""Keep API folder admission authenticated, confined and idempotent."""

from pathlib import Path
from uuid import uuid4

import pytest
from django.contrib.auth.models import User
from django.db import DatabaseError
from pytest_django.fixtures import Settings
from pytest_mock import MockerFixture
from rest_framework.authtoken.models import Token
from rest_framework.test import APIClient

from documents.data_models import DocumentSource
from documents.models import PaperlessTask
from documents.models import Tag
from paperless_testing.dirs import PaperlessDirs
from paperless_testing.factories import UserFactory

pytestmark = [pytest.mark.api, pytest.mark.django_db]
ENDPOINT = "/api/services/consumer/"
PUBLISH = "documents.consumption.consume_file.apply_async"


@pytest.fixture
def consume_root(paperless_dirs: PaperlessDirs, settings: Settings) -> Path:
    """Use separate disposable directories with an explicit detection policy."""
    root = paperless_dirs.consumption_dir
    settings.CONSUMER_RECURSIVE = True
    settings.CONSUMER_SUBDIRS_AS_TAGS = True
    settings.CONSUMER_POLLING_INTERVAL = 0.25
    settings.CONSUMER_STABILITY_DELAY = 2.5
    settings.CONSUMER_IGNORE_PATTERNS = [r"^ignored"]
    settings.CONSUMER_IGNORE_DIRS = ["excluded"]
    return root


@pytest.fixture
def token_client(admin_user: User) -> APIClient:
    """Authenticate through the configured token backend, not force_authenticate."""
    client = APIClient()
    token = Token.objects.create(user=admin_user)
    client.credentials(HTTP_AUTHORIZATION=f"Token {token.key}")
    return client


def submission(root: Path, relative: str = "invoice.txt") -> tuple[Path, dict]:
    """Create a source file and capture the metadata observed by a watcher."""
    source = root / relative
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"Invoice test content")
    stat = source.stat()
    return source, {
        "path": relative,
        "request_id": str(uuid4()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize("kind", ["anonymous", "regular", "staff", "inactive"])
def test_only_active_superusers_can_admit_files(
    method: str,
    kind: str,
    consume_root: Path,
    mocker: MockerFixture,
) -> None:
    """Neither ordinary/staff tokens nor inactive superusers can use the service."""
    client = APIClient()
    if kind != "anonymous":
        user = UserFactory(
            is_staff=kind == "staff",
            is_superuser=kind == "inactive",
            is_active=kind != "inactive",
        )
        token = Token.objects.create(user=user)
        client.credentials(HTTP_AUTHORIZATION=f"Token {token.key}")
    source, payload = submission(consume_root)
    publish = mocker.patch(PUBLISH)
    response = getattr(client, method)(ENDPOINT, payload, format="json")
    assert response.status_code in (401, 403)
    publish.assert_not_called()
    assert source.exists()
    assert not PaperlessTask.objects.exists()


def test_superuser_reads_effective_policy(
    token_client: APIClient,
    consume_root: Path,
) -> None:
    """The service supplies the same recursive, filtering and stability policy."""
    response = token_client.get(ENDPOINT)
    assert response.status_code == 200
    extensions = response.data.pop("supported_extensions")
    assert extensions == sorted(extensions)
    assert {".pdf", ".txt", ".png"} <= set(extensions)
    assert response.data == {
        "recursive": True,
        "subdirs_as_tags": True,
        "polling_interval": 0.25,
        "stability_delay": 2.5,
        "ignore_patterns": [r"^ignored"],
        "ignore_dirs": ["excluded"],
    }


@pytest.mark.parametrize(
    "path",
    ["../outside.txt", "nested/../../outside.txt", "/tmp/outside.txt", "bad\x00.txt"],
)
def test_paths_cannot_escape_root(
    token_client: APIClient,
    consume_root: Path,
    path: str,
    mocker: MockerFixture,
) -> None:
    """Reject traversal, absolute names and invalid paths before publishing."""
    source, payload = submission(consume_root)
    payload["path"] = path
    publish = mocker.patch(PUBLISH)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 400
    publish.assert_not_called()
    assert source.exists()
    assert not PaperlessTask.objects.exists()


def test_symlink_outside_root_is_rejected(
    token_client: APIClient,
    consume_root: Path,
    tmp_path: Path,
    mocker: MockerFixture,
) -> None:
    """An apparently relative symlink must not submit an outside document."""
    source, payload = submission(tmp_path, "outside.txt")
    (consume_root / "link.txt").symlink_to(source)
    payload["path"] = "link.txt"
    publish = mocker.patch(PUBLISH)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 400
    publish.assert_not_called()
    assert source.read_bytes() == b"Invoice test content"


@pytest.mark.parametrize(
    "relative",
    ["ignored.txt", "excluded/invoice.txt", "._invoice.txt", "unsupported.xyz"],
)
def test_server_filters_ignore_files(
    token_client: APIClient,
    consume_root: Path,
    relative: str,
    mocker: MockerFixture,
) -> None:
    """The API applies configured and built-in folder filters itself."""
    source, payload = submission(consume_root, relative)
    publish = mocker.patch(PUBLISH)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 200
    assert response.data == {"status": "ignored", "task_id": None}
    publish.assert_not_called()
    assert source.exists()
    assert not PaperlessTask.objects.exists()


def test_nonrecursive_policy_ignores_subdirectories(
    token_client: APIClient,
    consume_root: Path,
    settings: Settings,
    mocker: MockerFixture,
) -> None:
    """A client cannot bypass the server's recursion setting."""
    source, payload = submission(consume_root, "nested/invoice.txt")
    settings.CONSUMER_RECURSIVE = False
    publish = mocker.patch(PUBLISH)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.data == {"status": "ignored", "task_id": None}
    publish.assert_not_called()
    assert source.exists()


@pytest.mark.parametrize("field", ["size", "mtime_ns"])
def test_changed_metadata_is_not_admitted(
    token_client: APIClient,
    consume_root: Path,
    field: str,
    mocker: MockerFixture,
) -> None:
    """A file changed since observation must complete a new stability interval."""
    source, payload = submission(consume_root)
    payload[field] += 1
    publish = mocker.patch(PUBLISH)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.data == {"status": "changed", "task_id": None}
    publish.assert_not_called()
    assert source.exists()
    assert not PaperlessTask.objects.exists()


def test_missing_file_is_not_admitted(
    token_client: APIClient,
    consume_root: Path,
    mocker: MockerFixture,
) -> None:
    """A vanished source does not acquire an admission receipt."""
    source, payload = submission(consume_root)
    source.unlink()
    publish = mocker.patch(PUBLISH)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.data == {"status": "missing", "task_id": None}
    publish.assert_not_called()
    assert not PaperlessTask.objects.exists()


@pytest.mark.parametrize(
    "extra",
    [
        {"owner_id": 1},
        {"task": "documents.tasks.empty_trash"},
        {"overrides": {"tag_ids": [1]}},
    ],
)
def test_client_cannot_supply_task_metadata(
    token_client: APIClient,
    consume_root: Path,
    extra: dict,
    mocker: MockerFixture,
) -> None:
    """Only the file observation and retry ID can be controlled by the client."""
    _, payload = submission(consume_root)
    publish = mocker.patch(PUBLISH)
    response = token_client.post(ENDPOINT, {**payload, **extra}, format="json")
    assert response.status_code == 400
    publish.assert_not_called()


def test_admission_uses_folder_context_and_precedes_publication(
    token_client: APIClient,
    consume_root: Path,
    mocker: MockerFixture,
) -> None:
    """Reserve first, preserve path/tag behavior and never assign the token owner."""
    source, payload = submission(consume_root, "Invoices/2026/invoice.TXT")

    def inspect_admission(**options: object) -> None:
        """Observe a durable receipt before the broker can receive the message."""
        task = PaperlessTask.objects.get(task_id=payload["request_id"])
        assert task.result_data == {"api_publication_uncertain": True}
        assert task.input_data["api_consumer_key"]
        assert source.exists()

    publish = mocker.patch(PUBLISH, side_effect=inspect_admission)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 202
    assert response.data == {"task_id": payload["request_id"], "status": "queued"}
    publish.assert_called_once()
    options = publish.call_args.kwargs
    assert options["task_id"] == payload["request_id"]
    assert options["headers"] == {
        "trigger_source": PaperlessTask.TriggerSource.FOLDER_CONSUME,
    }
    document = options["kwargs"]["input_doc"]
    assert document.source == DocumentSource.ConsumeFolder
    assert document.original_file == source
    overrides = options["kwargs"]["overrides"]
    assert set(
        Tag.objects.filter(pk__in=overrides.tag_ids).values_list("name", flat=True),
    ) == {"Invoices", "2026"}
    assert overrides.owner_id is None
    assert overrides.actor_id is None
    task = PaperlessTask.objects.get(task_id=payload["request_id"])
    assert task.task_type == PaperlessTask.TaskType.CONSUME_FILE
    assert task.trigger_source == PaperlessTask.TriggerSource.FOLDER_CONSUME
    assert task.owner_id is None
    assert task.result_data is None
    assert source.read_bytes() == b"Invoice test content"


@pytest.mark.parametrize(
    "status",
    [
        PaperlessTask.Status.PENDING,
        PaperlessTask.Status.SUCCESS,
        PaperlessTask.Status.FAILURE,
    ],
)
def test_retry_after_lost_response_and_deleted_source_never_republishes(
    token_client: APIClient,
    consume_root: Path,
    status: str,
    mocker: MockerFixture,
) -> None:
    """A receipt remains authoritative after the response or original file is gone."""
    source, payload = submission(consume_root)
    publish = mocker.patch(PUBLISH)
    token_client.post(ENDPOINT, payload, format="json")
    source.unlink()
    PaperlessTask.objects.filter(task_id=payload["request_id"]).update(status=status)
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 202
    assert response.data == {"task_id": payload["request_id"], "status": "queued"}
    publish.assert_called_once()
    assert PaperlessTask.objects.count() == 1


@pytest.mark.parametrize("field", ["path", "size", "mtime_ns"])
def test_request_id_cannot_be_reused_for_another_observation(
    token_client: APIClient,
    consume_root: Path,
    field: str,
    mocker: MockerFixture,
) -> None:
    """Conflicting request data cannot reuse or overwrite an existing receipt."""
    source, payload = submission(consume_root)
    publish = mocker.patch(PUBLISH)
    token_client.post(ENDPOINT, payload, format="json")
    payload[field] = "another.txt" if field == "path" else payload[field] + 1
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 409
    publish.assert_called_once()
    assert PaperlessTask.objects.count() == 1
    assert source.exists()


def test_publication_failure_stays_uncertain_without_automatic_retry(
    token_client: APIClient,
    consume_root: Path,
    mocker: MockerFixture,
) -> None:
    """A broker exception can mean acceptance, so retry only returns the receipt."""
    source, payload = submission(consume_root)
    publish = mocker.patch(
        PUBLISH,
        side_effect=ConnectionError("publication result unknown"),
    )
    first = token_client.post(ENDPOINT, payload, format="json")
    second = token_client.post(ENDPOINT, payload, format="json")
    assert first.status_code == second.status_code == 202
    assert (
        first.data
        == second.data
        == {"task_id": payload["request_id"], "status": "publication_uncertain"}
    )
    publish.assert_called_once()
    assert PaperlessTask.objects.get(task_id=payload["request_id"]).result_data == {
        "api_publication_uncertain": True,
    }
    assert source.read_bytes() == b"Invoice test content"


def test_receipt_creation_failure_prevents_publication(
    token_client: APIClient,
    consume_root: Path,
    mocker: MockerFixture,
) -> None:
    """Without a durable deduplication receipt, leave the source and return 503."""
    source, payload = submission(consume_root)
    publish = mocker.patch(PUBLISH)
    mocker.patch.object(
        PaperlessTask.objects,
        "create",
        side_effect=DatabaseError("receipt write failed"),
    )
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 503
    publish.assert_not_called()
    assert not PaperlessTask.objects.exists()
    assert source.read_bytes() == b"Invoice test content"


@pytest.mark.usefixtures("fake_progress_manager")
def test_worker_detects_api_mime_before_plugins_and_preserves_receipt(
    token_client: APIClient,
    consume_root: Path,
    mocker: MockerFixture,
) -> None:
    """Admission and serialization skip libmagic; worker execution resolves it."""
    import pickle

    import magic

    from documents.plugins.base import StopConsumeTaskError
    from documents.tasks import consume_file

    source, payload = submission(consume_root)
    detect = mocker.spy(magic, "from_file")
    detect_buffer = mocker.spy(magic, "from_buffer")
    publish = mocker.patch(PUBLISH)
    assert token_client.get(ENDPOINT).status_code == 200
    response = token_client.post(ENDPOINT, payload, format="json")
    assert response.status_code == 202
    kwargs = pickle.loads(pickle.dumps(publish.call_args.kwargs["kwargs"]))
    detect.assert_not_called()
    detect_buffer.assert_not_called()
    assert kwargs["input_doc"].mime_type is None
    receipt = PaperlessTask.objects.get(task_id=payload["request_id"])
    assert receipt.input_data["mime_type"] is None
    original_metadata = receipt.input_data.copy()

    def check_mime(plugin) -> None:
        """Every plugin starts with the worker's detected MIME and updated receipt."""
        assert plugin.input_doc.mime_type == "text/plain"
        receipt.refresh_from_db()
        assert receipt.status == PaperlessTask.Status.STARTED
        assert receipt.input_data == {**original_metadata, "mime_type": "text/plain"}
        raise StopConsumeTaskError("MIME boundary checked")

    mocker.patch(
        "documents.consumer.ConsumerPreflightPlugin.run",
        autospec=True,
        side_effect=check_mime,
    )
    result = consume_file.apply(
        kwargs=kwargs,
        task_id=payload["request_id"],
        throw=True,
    )
    assert result.result == {"reason": "MIME boundary checked"}
    detect.assert_called_once_with(source, mime=True)
    assert source.exists()


@pytest.mark.parametrize("missing", [False, True])
def test_worker_mime_failure_is_a_failed_task_without_republishing(
    token_client: APIClient,
    consume_root: Path,
    mocker: MockerFixture,
    *,
    missing: bool,
) -> None:
    """A vanished source or detector exception fails in the worker, not admission."""
    from documents.tasks import consume_file

    source, payload = submission(consume_root)
    publish = mocker.patch(PUBLISH)
    assert token_client.post(ENDPOINT, payload, format="json").status_code == 202
    if missing:
        source.unlink()
    else:
        mocker.patch("magic.from_file", side_effect=OSError("MIME detection failed"))
    result = consume_file.apply(
        kwargs=publish.call_args.kwargs["kwargs"],
        task_id=payload["request_id"],
        throw=False,
    )
    assert result.failed()
    receipt = PaperlessTask.objects.get(task_id=payload["request_id"])
    assert receipt.status == PaperlessTask.Status.FAILURE
    assert receipt.result_data["error_message"]
    assert receipt.input_data["mime_type"] is None
    assert receipt.input_data["api_consumer_key"]
    assert source.exists() is not missing
    assert token_client.post(ENDPOINT, payload, format="json").status_code == 202
    publish.assert_called_once()


def test_native_consumable_document_still_detects_mime_immediately(
    consume_root: Path,
    mocker: MockerFixture,
) -> None:
    """Existing command, upload, mail and bulk constructors retain eager detection."""
    import magic

    from documents.data_models import ConsumableDocument

    source, _ = submission(consume_root)
    detect = mocker.spy(magic, "from_file")
    document = ConsumableDocument(DocumentSource.ConsumeFolder, source)
    assert document.mime_type == "text/plain"
    detect.assert_called_once_with(source, mime=True)
