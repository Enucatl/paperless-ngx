"""Exercise real document workflows without loading disabled processing engines."""

import os
import shutil
import subprocess
import sys
from hashlib import sha256
from pathlib import Path

import pytest

_SRC_DIR = Path(__file__).resolve().parents[2]
_DISABLED_MODULES = (
    "paperless_ai.ai_classifier",
    "paperless_ai.base_model",
    "paperless_ai.chat",
    "paperless_ai.client",
    "paperless_ai.embedding",
    "paperless_ai.indexing",
    "paperless_ai.vector_store",
    "llama_index",
    "openai",
    "ollama",
    "transformers",
    "sentence_transformers",
    "torch",
    "sqlite_vec",
    "tika_client",
    "gotenberg_client",
    "ocrmypdf",
)


def _assert_disabled_modules_absent(operation: str) -> None:
    """Identify the first operation that loads a disabled implementation."""
    imported = sorted(
        name
        for name in sys.modules
        if any(
            name == prefix or name.startswith(prefix + ".")
            for prefix in _DISABLED_MODULES
        )
    )
    assert not imported, f"{operation} loaded disabled implementations: {imported}"


def _run_document_workflow(configuration: str) -> None:
    """Consume and update real documents in a fresh interpreter and database."""
    import django

    django.setup()

    from django.conf import settings
    from django.contrib.admin.sites import AdminSite
    from django.contrib.auth.models import User
    from django.core.management import call_command
    from rest_framework.test import APIClient

    from documents.admin import TagAdmin
    from documents.data_models import ConsumableDocument
    from documents.data_models import DocumentMetadataOverrides
    from documents.data_models import DocumentSource
    from documents.models import Document
    from documents.models import Tag
    from documents.tasks import consume_file
    from documents.tasks import update_document_content_maybe_archive_file
    from paperless.celery import app
    from paperless.config import AIConfig
    from paperless.config import OcrConfig
    from paperless.models import ApplicationConfiguration
    from paperless.parsers.registry import get_parser_registry

    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = True
    app.conf.broker_url = "memory://"
    app.conf.result_backend = "cache+memory://"
    call_command("migrate", verbosity=0, interactive=False, skip_checks=True)
    settings.INDEX_DIR.mkdir(parents=True)
    get_parser_registry()
    if configuration == "database":
        assert AIConfig().ai_enabled
        assert AIConfig().llm_index_enabled
        assert OcrConfig().mode == "auto"
        config = ApplicationConfiguration.objects.get()
        config.ai_enabled = False
        config.mode = "off"
        config.archive_file_generation = "never"
        config.save()
    assert not AIConfig().ai_enabled
    assert not AIConfig().llm_index_enabled
    assert OcrConfig().mode == "off"
    assert OcrConfig().archive_file_generation == "never"
    _assert_disabled_modules_absent("configuration and registration")

    user = User.objects.create(username="operator", is_superuser=True, is_staff=True)
    client = APIClient()
    client.force_authenticate(user=user)
    tag = Tag.objects.create(name="manual")
    parent = Tag.objects.create(name="parent")
    samples = _SRC_DIR / "documents" / "tests" / "samples"
    documents = []
    for filename, title in (
        ("simple.pdf", "sapphire"),
        ("simple.png", "emerald"),
        ("local.txt", "amethyst"),
    ):
        source = settings.CONSUMPTION_DIR / filename
        if filename == "local.txt":
            source.write_text("Local workflow content for search verification.")
        else:
            from paperless_testing.samples import SIMPLE_DIGITAL_PDF

            shutil.copy(
                SIMPLE_DIGITAL_PDF if filename == "simple.pdf" else samples / filename,
                source,
            )
        original_checksum = sha256(source.read_bytes()).hexdigest()
        result = consume_file(
            ConsumableDocument(DocumentSource.ConsumeFolder, source),
            DocumentMetadataOverrides(owner_id=user.pk),
        )
        assert result is not None and "document_id" in result, result
        document = Document.objects.get(pk=result["document_id"])
        documents.append(document)
        assert document.source_path.is_file()
        assert (
            sha256(document.source_path.read_bytes()).hexdigest() == original_checksum
        )
        assert document.checksum == original_checksum
        assert document.thumbnail_path.stat().st_size > 0
        assert not document.archive_filename
        if filename == "simple.png":
            assert not document.content.strip()
        else:
            assert document.content.strip()
        _assert_disabled_modules_absent(f"consume {filename}")

        original_content = document.content
        Document.objects.filter(pk=document.pk).update(content="stale content")
        update_document_content_maybe_archive_file(document.pk)
        document.refresh_from_db()
        assert document.content == original_content
        assert document.thumbnail_path.stat().st_size > 0
        assert not document.archive_filename
        assert (
            sha256(document.source_path.read_bytes()).hexdigest() == original_checksum
        )
        assert document.checksum == original_checksum
        _assert_disabled_modules_absent(f"reprocess {filename}")

        response = client.patch(
            f"/api/documents/{document.pk}/",
            {"title": title, "tags": [tag.pk]},
            format="json",
        )
        assert response.status_code == 200, response.content
        document.refresh_from_db()
        assert document.title == title
        assert list(document.tags.all()) == [tag]
        assert (
            sha256(document.source_path.read_bytes()).hexdigest() == original_checksum
        )
        assert document.checksum == original_checksum
        _assert_disabled_modules_absent(f"edit and tag {filename}")

    tag.tn_parent = parent
    TagAdmin(Tag, AdminSite()).save_model(None, tag, None, change=True)
    assert all(parent in document.tags.all() for document in documents)
    _assert_disabled_modules_absent("parent tag update and follow-up task")

    response = client.get("/api/documents/")
    assert response.status_code == 200, response.content
    assert response.data["count"] == len(documents)
    _assert_disabled_modules_absent("document list")
    for document in documents:
        document.refresh_from_db()
        response = client.get("/api/documents/", {"query": document.title})
        assert response.status_code == 200, response.content
        assert [item["id"] for item in response.data["results"]] == [document.pk], (
            response.data,
        )
        _assert_disabled_modules_absent("search")
    assert not list(settings.ARCHIVE_DIR.rglob("*.pdf"))


@pytest.mark.parametrize("configuration", ["environment", "database"])
def test_disabled_features_stay_unloaded_during_workflows(
    configuration: str,
    tmp_path: Path,
) -> None:
    """Keep disabled engines unloaded through real ingestion and ordinary use."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("PAPERLESS_")
    }
    config_file = tmp_path / "paperless.conf"
    config_file.touch()
    env.update(
        PYTHONPATH=str(_SRC_DIR),
        DJANGO_SETTINGS_MODULE="paperless.settings",
        PAPERLESS_CONFIGURATION_PATH=str(config_file),
        PAPERLESS_SECRET_KEY="disabled-feature-workflow-test-secret",
        PAPERLESS_DBENGINE="sqlite",
        PAPERLESS_ALLOWED_HOSTS="testserver",
        PAPERLESS_AI_ENABLED="true" if configuration == "database" else "false",
        PAPERLESS_AI_LLM_EMBEDDING_BACKEND="huggingface",
        PAPERLESS_OCR_MODE="auto" if configuration == "database" else "off",
        PAPERLESS_ARCHIVE_FILE_GENERATION="always"
        if configuration == "database"
        else "never",
        PAPERLESS_TIKA_ENABLED="false",
        PAPERLESS_CACHE_BACKEND="django.core.cache.backends.locmem.LocMemCache",
        PAPERLESS_CHANNELS_BACKEND="channels.layers.InMemoryChannelLayer",
        PAPERLESS_DISABLE_DBHANDLER="true",
    )
    for name in ("PAPERLESS_STATICDIR", "PAPERLESS_THUMBNAIL_FONT_NAME"):
        if name in os.environ:
            env[name] = os.environ[name]
    for name in ("DATA_DIR", "MEDIA_ROOT", "CONSUMPTION_DIR", "SCRATCH_DIR"):
        directory = tmp_path / name.lower()
        directory.mkdir()
        env[f"PAPERLESS_{name}"] = str(directory)
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), configuration],
        cwd=_SRC_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    _run_document_workflow(sys.argv[1])
