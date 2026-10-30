"""Keep startup registration independent of unused implementations."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

_SRC_DIR = Path(__file__).resolve().parents[2]
_PROCESSING_MODULES = (
    "documents.barcodes",
    "documents.bulk_download",
    "documents.classifier",
    "documents.consumer",
    "documents.double_sided",
    "documents.sanity_checker",
    "paperless_ai.indexing",
    "pikepdf",
    "tantivy",
    "ocrmypdf",
    "llama_index",
    "sklearn",
)
_UNUSED_IMPLEMENTATIONS = (
    "dateparser",
    "pikepdf",
    "tantivy",
    "ocrmypdf",
    "paperless_ai.ai_classifier",
    "paperless_ai.chat",
    "paperless_ai.indexing",
    "llama_index",
    "sklearn",
    "tika_client",
    "gotenberg_client",
    "turbohtml",
)
_REQUIRED_TASKS = (
    "required = {\n"
    "    'documents.tasks.consume_file',\n"
    "    'paperless_mail.tasks.process_mail_accounts',\n"
    "    'paperless_mail.mail.apply_mail_action',\n"
    "    'paperless_mail.mail.error_callback',\n"
    "    'documents.workflows.webhooks.send_webhook',\n"
    "    'documents.signals.handlers.process_cf_select_update',\n"
    "}\n"
    "assert not required.difference(app.tasks), required.difference(app.tasks)\n"
)
_CONSUMER_ERROR_PICKLE = """
import django
django.setup()
import pickle
import sys
from documents.consumer import ConsumerError
from documents.exceptions import ConsumerError as SharedError
assert ConsumerError is SharedError
payload = pickle.dumps(ConsumerError('missing file'), protocol=0)
legacy = payload.replace(b'documents.exceptions', b'documents.consumer')
assert type(pickle.loads(legacy)) is SharedError
sys.stdout.buffer.write(payload)
"""


@pytest.mark.parametrize(
    ("operation", "forbidden"),
    [
        pytest.param(
            "import paperless.settings\n",
            ("dateparser",),
            id="default-settings-without-dateparser",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "from django.contrib import admin\n"
            "from documents.models import Tag\n"
            "assert admin.site.is_registered(Tag)\n",
            _PROCESSING_MODULES,
            id="admin-and-task-registration-without-processing",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "from paperless.celery import app\n"
            "app.autodiscover_tasks(force=True)\n"
            "app.finalize()\n" + _REQUIRED_TASKS,
            (
                *_PROCESSING_MODULES,
                "documents.templating.filepath",
                "documents.templating.workflows",
                "babel",
            ),
            id="celery-discovers-tasks-and-callbacks",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "import pickle, subprocess\n"
            f"payload = subprocess.check_output([sys.executable, '-c', {_CONSUMER_ERROR_PICKLE!r}])\n"
            "from documents.exceptions import ConsumerError\n"
            "error = pickle.loads(payload)\n"
            "assert type(error) is ConsumerError\n"
            "assert error.args == ('missing file',)\n",
            _PROCESSING_MODULES,
            id="consumer-error-unpickling-without-processing",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "from django.core.management import call_command\n"
            "call_command('migrate', verbosity=0, interactive=False, skip_checks=True)\n"
            "from unittest.mock import patch\n"
            "from django.contrib.admin.sites import AdminSite\n"
            "from documents.admin import TagAdmin\n"
            "from documents.models import Document, PaperlessTask, Tag\n"
            "document = Document.objects.create(title='test')\n"
            "parent = Tag.objects.create(name='parent')\n"
            "child = Tag.objects.create(name='child')\n"
            "document.tags.add(child)\n"
            "child.tn_parent = parent\n"
            "with patch('documents.tasks.bulk_update_documents.apply_async') as publish:\n"
            "    TagAdmin(Tag, AdminSite()).save_model(None, child, None, change=True)\n"
            "    publish.assert_called_once_with(\n"
            "        kwargs={'document_ids': [document.pk]},\n"
            "        headers={'trigger_source': PaperlessTask.TriggerSource.SYSTEM})\n"
            "assert parent in document.tags.all()\n",
            _PROCESSING_MODULES,
            id="tag-parent-update-without-processing",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "from django.urls import reverse\n"
            "assert reverse('document-list') == '/api/documents/'\n",
            (
                *_UNUSED_IMPLEMENTATIONS,
                "watchfiles",
                "paperless_services.watching",
                "documents.management.commands.document_consumer",
            ),
            id="urls-without-processing",
        ),
        pytest.param(
            "from paperless.asgi import application\n"
            "assert {'http', 'websocket'} <= application.application_mapping.keys()\n",
            (
                *_UNUSED_IMPLEMENTATIONS,
                "watchfiles",
                "paperless_services.watching",
                "documents.management.commands.document_consumer",
            ),
            id="asgi-without-processing",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "from django.test import override_settings\n"
            "from paperless.parsers.registry import get_parser_registry\n"
            "registry = get_parser_registry()\n"
            "names = {parser.__name__ for parser in registry.all_parsers()}\n"
            "assert names == {'TextDocumentParser', 'RemoteDocumentParser',\n"
            "    'TikaDocumentParser', 'MailDocumentParser', 'RasterisedDocumentParser'}\n"
            "assert registry.get_parser_for_file('text/plain', 'test.txt').__name__ == 'TextDocumentParser'\n"
            "assert registry.get_parser_for_file('application/msword', 'test.doc') is None\n"
            "with override_settings(TIKA_ENABLED=True):\n"
            "    assert registry.get_parser_for_file('application/msword', 'test.doc').__name__ == 'TikaDocumentParser'\n",
            _UNUSED_IMPLEMENTATIONS,
            id="parser-registration-and-selection-without-processing",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "from django.core.checks import run_checks\n"
            "from django.core.checks.registry import registry\n"
            "registered = set(registry.get_checks())\n"
            "from documents.checks import parser_check, filename_format_check\n"
            "from documents import parser_check as legacy_parser_check\n"
            "assert legacy_parser_check is parser_check\n"
            "assert {parser_check, filename_format_check} <= registered\n"
            "errors = [message for message in run_checks() if message.is_serious()]\n"
            "assert not errors, errors\n",
            _UNUSED_IMPLEMENTATIONS,
            id="system-checks-without-processing",
        ),
        pytest.param(
            "from paperless.celery import app\n"
            "app.loader.init_worker()\n"
            "app.finalize()\n" + _REQUIRED_TASKS,
            _UNUSED_IMPLEMENTATIONS,
            id="full-celery-loader-without-processing",
        ),
        pytest.param(
            "import django\n"
            "django.setup()\n"
            "from django.core.management import call_command\n"
            "call_command('migrate', verbosity=0, interactive=False, skip_checks=True)\n"
            "from django.contrib.auth.models import User\n"
            "from documents.models import Document\n"
            "from rest_framework.test import APIClient\n"
            "user = User.objects.create(username='viewer', is_superuser=True)\n"
            "document = Document.objects.create(title='test', owner=user)\n"
            "client = APIClient()\n"
            "client.force_authenticate(user=user)\n"
            "response = client.get('/api/documents/')\n"
            "assert response.status_code == 200, response.content\n"
            "assert response.data['count'] == 1, response.data\n"
            "assert response.data['results'][0]['id'] == document.pk\n"
            "response = client.get(f'/api/documents/{document.pk}/ai_suggestions/')\n"
            "assert response.status_code == 400, response.content\n"
            "assert b'AI is required' in response.content\n"
            "response = client.post('/api/documents/chat/', {'q': 'test'}, format='json')\n"
            "assert response.status_code == 400, response.content\n"
            "assert b'AI is required' in response.content\n",
            _UNUSED_IMPLEMENTATIONS,
            id="document-list-and-disabled-ai-without-processing",
        ),
    ],
)
def test_import_boundary(
    operation: str,
    forbidden: tuple[str, ...],
    tmp_path: Path,
) -> None:
    """Keep startup and ordinary document operations free of unused engines."""
    configuration = tmp_path / "paperless.conf"
    configuration.touch()
    env = os.environ.copy()
    env.update(
        DJANGO_SETTINGS_MODULE="paperless.settings",
        PAPERLESS_CONFIGURATION_PATH=str(configuration),
        PAPERLESS_DATA_DIR=str(tmp_path),
        PAPERLESS_DBENGINE="sqlite",
        PAPERLESS_DBHOST="",
        PAPERLESS_SECRET_KEY="import-boundary-test-secret",
        PAPERLESS_DATE_PARSER_LANGUAGES="",
        PAPERLESS_AI_ENABLED="false",
        PAPERLESS_TIKA_ENABLED="false",
        PAPERLESS_CACHE_BACKEND="django.core.cache.backends.locmem.LocMemCache",
        PAPERLESS_CHANNELS_BACKEND="channels.layers.InMemoryChannelLayer",
        PAPERLESS_DISABLE_DBHANDLER="true",
    )
    for name in ("MEDIA_ROOT", "CONSUMPTION_DIR", "SCRATCH_DIR"):
        directory = tmp_path / name.lower()
        directory.mkdir()
        env[f"PAPERLESS_{name}"] = str(directory)
    env.pop("PAPERLESS_IGNORE_DATES", None)
    env.pop("PAPERLESS_SEARCH_LANGUAGE", None)
    code = (
        "import sys\n" + operation + f"forbidden = {forbidden!r}\n"
        "leaked = sorted(name for name in sys.modules if any(\n"
        "    name == prefix or name.startswith(prefix + '.')\n"
        "    for prefix in forbidden))\n"
        "assert not leaked, f'Unexpected implementation imports: {leaked}'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=_SRC_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
