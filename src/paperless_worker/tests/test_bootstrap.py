"""Exercise real metadata extraction, child initialization and parent contracts."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[2]


@pytest.fixture
def worker_env(tmp_path: Path) -> dict[str, str]:
    """Keep configuration and helper state in disposable test storage."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PAPERLESS_", "CELERY_", "DJANGO_"))
    }
    config = tmp_path / "paperless.conf"
    config.write_text("PAPERLESS_TASK_WORKERS=3\n")
    env.update(
        PAPERLESS_CONFIGURATION_PATH=str(config),
        PAPERLESS_SECRET_KEY="test-bootstrap-secret-never-printed",
        PAPERLESS_DBHOST="",
        PAPERLESS_DBENGINE="sqlite",
        PAPERLESS_TASK_WORKERS="2",
        CELERY_SKIP_CHECKS="1",
        PYTHONPATH=str(SOURCE),
        PYTHONDONTWRITEBYTECODE="1",
    )
    for setting in ("DATA_DIR", "MEDIA_ROOT", "SCRATCH_DIR", "CONSUMPTION_DIR"):
        path = tmp_path / setting.lower()
        path.mkdir()
        env[f"PAPERLESS_{setting}"] = str(path)
    return env


def run_python(code: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run parent assertions in a fresh process outside pytest's Django setup."""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=SOURCE,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert env["PAPERLESS_SECRET_KEY"] not in result.stdout + result.stderr
    return result


def test_parent_metadata_and_real_child_app(worker_env: dict[str, str]) -> None:
    """Keep the parent bare while the forked child restores the same real app."""
    run_python(
        """
import os, sys
from paperless_worker.bootstrap import create_app, read_metadata, MetadataTask
metadata = read_metadata()
assert metadata['configuration']['worker_concurrency'] == 2
assert metadata['secret_key'] == os.environ['PAPERLESS_SECRET_KEY']
app = create_app(metadata)
assert set(app.tasks) == set(metadata['tasks'])
assert all(isinstance(task, MetadataTask) for task in app.tasks.values())
assert 'django.apps' not in sys.modules
assert 'django.conf' not in sys.modules
assert not any(name.startswith(('documents', 'paperless.')) for name in sys.modules)
pid = os.fork()
if not pid:
    try:
        app.loader.on_worker_process_init()
        from paperless.celery import app as real_app
        from celery.contrib.django.task import DjangoTask
        from django.apps import apps
        assert app is real_app
        assert apps.ready
        assert set(app.tasks) == set(metadata['tasks'])
        assert all(isinstance(task, DjangoTask) for task in app.tasks.values())
        assert not any(isinstance(task, MetadataTask) for task in app.tasks.values())
        assert app.tasks['paperless_mail.tasks.process_mail_accounts'].app is app
        retry_task = app.tasks['documents.tasks.apply_ai_suggestions']
        assert retry_task.max_retries == metadata['tasks'][retry_task.name]['options']['max_retries']
        from paperless_ai.exceptions import LLMTimeoutError
        assert retry_task.autoretry_for == (LLMTimeoutError,)
        assert hasattr(retry_task, '_orig_run')
    except BaseException:
        import traceback
        traceback.print_exc()
        os._exit(1)
    os._exit(0)
assert os.waitpid(pid, 0)[1] == 0
assert 'django.apps' not in sys.modules
assert 'django.conf' not in sys.modules
assert not any(name.startswith(('documents', 'paperless.')) for name in sys.modules)
app._paperless_outcomes.close()
""",
        worker_env,
    )


def test_header_preserves_callback_decisions_and_argument_checks(
    worker_env: dict[str, str],
) -> None:
    """Match real header arity and bound markers without loading task code."""
    run_python(
        """
import functools, inspect
from celery.utils.functional import arity_greater
from paperless_worker.bootstrap import create_app, read_metadata
metadata = read_metadata()
app = create_app(metadata)
callback = app.tasks['paperless_mail.mail.error_callback']
assert not isinstance(callback.__header__, functools.partial)
assert arity_greater(callback.__header__, 1)
assert isinstance(app.tasks['documents.tasks.consume_file'].__header__, functools.partial)
callback.__header__(None, None, None, rule_id=1, message_uid='1', message_subject='x', message_date=None)
try:
    callback.__header__(None)
except TypeError:
    pass
else:
    raise AssertionError('Required callback arguments were not checked')
app._paperless_outcomes.close()
""",
        worker_env,
    )


def test_custom_task_hooks_fail_explicitly(worker_env: dict[str, str]) -> None:
    """Reject custom parent behavior instead of silently replacing it."""
    run_python(
        """
from celery import Celery
from paperless_worker.metadata import describe_task
app = Celery('test', task_cls='celery.contrib.django.task:DjangoTask', fixups=[])
@app.task(name='custom')
def task():
    pass
app.finalize()
assert describe_task(task._get_current_object(), app)['options']['name'] == 'custom'
task.Request = 'custom.Request'
try:
    describe_task(task._get_current_object(), app)
except ValueError as error:
    assert 'Request' in str(error)
else:
    raise AssertionError('Custom request was silently dropped')
""",
        worker_env,
    )


@pytest.mark.parametrize(
    "configuration",
    [
        {"formatters": {"custom": {"()": "documents.custom.Formatter"}}},
        {"handlers": {"custom": {"class": "documents.custom.Handler"}}},
        {"handlers": {"console": {"stream": "ext://paperless.settings.SECRET_KEY"}}},
    ],
)
def test_custom_logging_cannot_import_application(configuration: dict) -> None:
    """Reject factory and external references that could initialize Django."""
    from paperless_worker.metadata import logging_configuration

    with pytest.raises(ValueError, match="Lean worker does not support"):
        logging_configuration(configuration)
