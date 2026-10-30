"""Verify lean transport preserves Celery's child task lifecycle."""

import os
import pickle
import subprocess
import sys
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from billiard.exceptions import Terminated
from billiard.exceptions import TimeLimitExceeded
from billiard.exceptions import WorkerLostError
from celery import Celery
from celery import Task
from celery import states
from celery.app.trace import build_tracer
from celery.app.trace import fast_trace_task
from celery.exceptions import Ignore
from celery.exceptions import Reject
from celery.exceptions import Retry
from celery.signals import task_failure
from celery.utils.serialization import UnpickleableExceptionWrapper
from celery.worker.request import Request

from paperless_worker.trace import RemoteTaskError
from paperless_worker.trace import _install_tracers
from paperless_worker.trace import install
from paperless_worker.trace import transport_exception
from paperless_worker.trace import wrap_tracer


class ApplicationError(Exception):
    """Stand in for an exception defined in a Django-heavy application module."""


@pytest.fixture
def app():
    """Use real Celery tracing and a disposable in-process result backend."""
    application = Celery(
        "transport-test",
        broker="memory://",
        backend="cache+memory://",
        fixups=[],
        set_as_current=False,
    )
    application.conf.update(task_always_eager=False)
    yield application
    application.close()


def test_child_failure_callbacks_keep_original_exception(app) -> None:
    """Normalize after backend, task hooks, signals and inline errbacks finish."""
    seen = {}

    class FailingTask(Task):
        name = "transport.failure"

        def run(self):
            raise ApplicationError("original failure")

        def on_failure(self, exc, task_id, args, kwargs, einfo):
            seen["on_failure"] = exc
            seen["original_einfo"] = einfo

        def after_return(self, status, retval, task_id, args, kwargs, einfo):
            seen["after_return"] = retval

    task = app.register_task(FailingTask())
    task.backend.on_chord_part_return = Mock()

    @app.task(name="transport.errback")
    def errback(request, exc, traceback):
        seen["errback"] = exc
        seen["traceback"] = traceback

    def failure_signal(exception, **kwargs):
        seen["signal"] = exception

    task_failure.connect(failure_signal, sender=task, weak=False)
    task.__trace__ = wrap_tracer(build_tracer(task.name, task, app.loader, app=app))
    try:
        result = task.__trace__(
            "failure-id",
            (),
            {},
            {"id": "failure-id", "errbacks": [errback.s()], "chord": errback.s()},
        )
    finally:
        task_failure.disconnect(failure_signal, sender=task)
    assert result.info.state == states.FAILURE
    for hook in ("on_failure", "after_return", "errback", "signal"):
        assert type(seen[hook]) is ApplicationError
        assert str(seen[hook]) == "original failure"
    assert seen["traceback"] is not None
    assert type(seen["original_einfo"].exception) is ApplicationError
    assert type(task.backend.get_task_meta("failure-id")["result"]) is ApplicationError
    assert type(task.backend.on_chord_part_return.call_args.args[2]) is ApplicationError
    remote = result.retval.exception.exc
    assert isinstance(remote, RemoteTaskError)
    assert remote.remote_type == "ApplicationError"
    assert str(remote) == "original failure"
    assert result.retval.tb is None
    assert "ApplicationError: original failure" in result.retval.traceback
    request = Mock()
    request.task.acks_late = False
    with patch("celery.worker.request.task_ready"):
        Request.on_failure(
            request,
            pickle.loads(pickle.dumps(result.retval)),
            return_ok=True,
        )
    request.task.backend.mark_as_failure.assert_not_called()
    assert request.send_event.call_args.kwargs["exception"] == (
        "ApplicationError('original failure')"
    )


def test_success_result_and_callbacks_are_unchanged(app) -> None:
    """Only the display string travels to the parent on successful completion."""

    @app.task(name="transport.callback")
    def callback(value):
        return value

    @app.task(name="transport.success")
    def success():
        return {"value": [1, 2, 3]}

    callback.apply_async = Mock()
    success.__trace__ = wrap_tracer(
        build_tracer(success.name, success, app.loader, app=app),
    )
    failed, value, _runtime = fast_trace_task(
        success.name,
        "success-id",
        {"callbacks": [callback.s()]},
        ((), {}, {}),
        None,
        None,
        _loc=(app.tasks, set(), "test-host"),
    )
    assert failed == 0
    assert isinstance(value, str)
    assert app.backend.get_task_meta("success-id")["result"] == {"value": [1, 2, 3]}
    callback.apply_async.assert_called_once()
    assert callback.apply_async.call_args.args[0] == ({"value": [1, 2, 3]},)


def test_retry_keeps_original_child_callbacks_and_parent_predicate(app) -> None:
    """Only the parent's retry diagnostic replaces the nested application error."""
    seen = []
    reason = ApplicationError("retry cause")

    class RetryingTask(Task):
        name = "transport.retry"

        def run(self):
            raise Retry(exc=reason, when=12)

        def on_retry(self, exc, task_id, args, kwargs, einfo):
            seen.append(exc)

    task = app.register_task(RetryingTask())
    trace = wrap_tracer(build_tracer(task.name, task, app.loader, app=app))
    result = trace("retry-id", (), {}, {"id": "retry-id"})
    assert seen == [reason]
    assert task.backend.get_task_meta("retry-id")["status"] == states.RETRY
    retry = pickle.loads(pickle.dumps(result.retval)).exception
    assert type(retry) is Retry
    assert retry.when == 12
    assert isinstance(retry.exc, RemoteTaskError)
    assert retry.exc.remote_type == "ApplicationError"
    request = Mock()
    request.task.acks_late = True
    Request.on_retry(request, pickle.loads(pickle.dumps(result.retval)))
    request.acknowledge.assert_called_once()
    request.send_event.assert_called_once()
    assert "ApplicationError" in request.send_event.call_args.kwargs["exception"]


@pytest.mark.parametrize(
    "exception",
    [
        Reject("try again", requeue=True),
        Ignore("ignore me"),
        SystemExit(7),
        KeyboardInterrupt(),
        MemoryError("out of memory"),
        Terminated(15),
        WorkerLostError("lost"),
        TimeLimitExceeded(3.5),
    ],
)
def test_native_parent_predicates_roundtrip(exception) -> None:
    """Retain native predicate classes and their primitive arguments/codes."""
    recovered = pickle.loads(pickle.dumps(transport_exception(exception)))
    assert type(recovered) is type(exception)
    assert recovered.args == exception.args
    if isinstance(exception, Reject):
        assert recovered.requeue is True
    if isinstance(exception, SystemExit):
        assert recovered.code == 7


def test_retry_datetime_and_nested_error_roundtrip() -> None:
    """Keep retry scheduling metadata while removing application exception imports."""
    when = datetime(2026, 10, 30, tzinfo=UTC)
    retry = Retry(exc=ApplicationError("later"), when=when)
    recovered = pickle.loads(pickle.dumps(transport_exception(retry)))
    assert recovered.when == when
    assert str(recovered.exc) == "later"


@pytest.mark.parametrize("reject", [False, True])
def test_parent_handles_reject_and_ignore_without_backend_failure(
    app,
    *,
    reject: bool,
) -> None:
    """Transported predicates still select Celery's reject/acknowledge branches."""

    @app.task(name="transport.predicate")
    def predicate():
        if reject:
            raise Reject("retry delivery", requeue=True)
        raise Ignore("already handled")

    trace = wrap_tracer(build_tracer(predicate.name, predicate, app.loader, app=app))
    result = trace("predicate-id", (), {}, {"id": "predicate-id"})
    info = pickle.loads(pickle.dumps(result.retval))
    request = Mock()
    with patch("celery.worker.request.task_ready"):
        Request.on_failure(request, info)
    if reject:
        request.reject.assert_called_once_with(requeue=True)
    else:
        request.acknowledge.assert_called_once()
    request.task.backend.mark_as_failure.assert_not_called()


def test_child_signal_wraps_the_built_tracer(app, monkeypatch) -> None:
    """Install after Celery constructs tracers without wrapping task.run itself."""
    from celery.signals import worker_process_init

    @app.task(name="transport.installed")
    def task():
        raise ApplicationError("installed")

    original_run = task.run
    original_trace = build_tracer(task.name, task, app.loader, app=app)
    task.__trace__ = original_trace
    monkeypatch.setattr("celery.current_app", app)
    install()
    try:
        worker_process_init.send(sender=None)
    finally:
        worker_process_init.disconnect(_install_tracers)
    assert task.__trace__ is not original_trace
    assert task.run == original_run
    result = task.__trace__("installed-id", (), {}, {"id": "installed-id"})
    assert result.retval.exception.exc.remote_type == "ApplicationError"


def test_base_exceptions_escape_unchanged() -> None:
    """Do not turn process-exit controls raised by the tracer into task failures."""
    exception = SystemExit(9)

    def tracer(*args, **kwargs):
        raise exception

    with pytest.raises(SystemExit) as caught:
        wrap_tracer(tracer)()
    assert caught.value is exception


def test_failure_transport_unpickles_without_application_module(tmp_path) -> None:
    """A fresh parent can decode an error without even having its module available."""
    source = Path(__file__).resolve().parents[2]
    payload = tmp_path / "result.pickle"
    code = """
import pickle, sys, types
from billiard.einfo import ExceptionInfo
from celery.app.trace import trace_ok_t, TraceInfo
from celery import states
from paperless_worker.trace import wrap_tracer
module = types.ModuleType('application_heavy_error')
exec('class CannotMoveFilesException(Exception): pass', module.__dict__)
sys.modules[module.__name__] = module
try:
    raise module.CannotMoveFilesException('rename failed')
except Exception:
    info = ExceptionInfo()
original = trace_ok_t(info, TraceInfo(states.FAILURE), 0, None)
result = wrap_tracer(lambda: original)()
with open(sys.argv[1], 'wb') as output:
    pickle.dump((1, result.retval, result.runtime), output)
"""
    env = {**os.environ, "PYTHONPATH": str(source), "PYTHONDONTWRITEBYTECODE": "1"}
    subprocess.run([sys.executable, "-c", code, str(payload)], env=env, check=True)
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import pickle, sys
with open(sys.argv[1], 'rb') as source:
    failed, info, runtime = pickle.load(source)
assert failed == 1
assert info.exception.remote_type == 'CannotMoveFilesException'
assert info.exception.remote_module == 'application_heavy_error'
assert str(info.exception) == 'rename failed'
assert not any(name == 'django' or name.startswith(('django.', 'documents.', 'application_heavy_error')) for name in sys.modules)
""",
            str(payload),
        ],
        env=env,
        check=True,
    )


def test_unpickleable_exception_keeps_original_diagnostics() -> None:
    """Retain the identity Celery restores without importing the original module."""
    exception = UnpickleableExceptionWrapper(
        "documents.consumer",
        "UnpickleableConsumerError",
        ("original failure",),
        "UnpickleableConsumerError('original failure')",
    )
    recovered = pickle.loads(pickle.dumps(transport_exception(exception)))
    assert isinstance(recovered, RemoteTaskError)
    assert recovered.remote_module == "documents.consumer"
    assert recovered.remote_type == "UnpickleableConsumerError"
    assert str(recovered) == "original failure"
    assert repr(recovered) == "UnpickleableConsumerError('original failure')"
