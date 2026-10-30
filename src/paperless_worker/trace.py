"""Keep child failure transport independent of application exception imports."""

from __future__ import annotations

from copy import copy
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from celery.app.trace import trace_ok_t


class RemoteTaskError(Exception):
    """Carry the original exception identity and text without importing its class."""

    def __init__(self, remote_module: str, remote_type: str, message: str) -> None:
        self.remote_module = remote_module
        self.remote_type = remote_type
        self.message = message
        super().__init__(remote_module, remote_type, message)

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return f"{self.remote_type}({self.message!r})"


def _transport_value(value: object) -> object:
    """Retain primitive predicate arguments without transporting custom classes."""
    if type(value) in (type(None), bool, int, float, str, bytes):
        return value
    if type(value) in (tuple, list):
        return type(value)(_transport_value(item) for item in value)
    if type(value) is dict:
        return {
            _transport_value(key): _transport_value(item) for key, item in value.items()
        }
    if isinstance(value, datetime):
        return datetime.fromisoformat(value.isoformat())
    return str(value)


def transport_exception(exception: BaseException) -> BaseException:
    """Preserve Celery's parent control predicates, replacing application errors."""
    from billiard.einfo import ExceptionWithTraceback
    from billiard.exceptions import Terminated
    from billiard.exceptions import TimeLimitExceeded
    from billiard.exceptions import WorkerLostError
    from celery.exceptions import Ignore
    from celery.exceptions import Reject
    from celery.exceptions import Retry
    from celery.utils.serialization import UnpickleableExceptionWrapper

    if isinstance(exception, ExceptionWithTraceback):
        exception = exception.exc
    if isinstance(exception, RemoteTaskError):
        return exception
    if isinstance(exception, UnpickleableExceptionWrapper):
        return RemoteTaskError(
            exception.exc_module,
            exception.exc_cls_name,
            str(exception.restore()),
        )
    if isinstance(exception, Retry):
        return Retry(
            message=_transport_value(exception.message),
            exc=transport_exception(exception.exc)
            if exception.exc is not None
            else None,
            when=_transport_value(exception.when),
        )
    if isinstance(exception, Reject):
        return Reject(
            reason=_transport_value(exception.reason),
            requeue=bool(exception.requeue),
        )
    if isinstance(exception, Ignore):
        return Ignore(*(_transport_value(arg) for arg in exception.args))
    for predicate in (
        SystemExit,
        KeyboardInterrupt,
        MemoryError,
        Terminated,
        WorkerLostError,
        TimeLimitExceeded,
    ):
        if isinstance(exception, predicate):
            return predicate(*(_transport_value(arg) for arg in exception.args))
    return RemoteTaskError(
        type(exception).__module__,
        type(exception).__name__,
        str(exception),
    )


def wrap_tracer(tracer: Callable[..., trace_ok_t]) -> Callable[..., trace_ok_t]:
    """Normalize only pool-bound errors after original child handling finishes."""
    from billiard.einfo import ExceptionInfo
    from billiard.einfo import ExceptionWithTraceback

    def trace(*args, **kwargs) -> trace_ok_t:
        """Run original child handling before replacing the pool failure envelope."""
        result = tracer(*args, **kwargs)
        if result.info and isinstance(result.retval, ExceptionInfo):
            original = result.retval
            exception = transport_exception(original.exception)
            info = copy(original)
            info.type = type(exception)
            info.exception = ExceptionWithTraceback(exception, original.traceback)
            info.tb = None
            return result._replace(retval=info)
        return result

    return trace


def _install_tracers(**kwargs) -> None:
    """Wrap the tracers Celery just constructed in this prefork child."""
    from celery import current_app

    for task in current_app.tasks.values():
        if task.__trace__ is not None:
            task.__trace__ = wrap_tracer(task.__trace__)


def install() -> None:
    """Register after child Django initialization and before prefork's signal."""
    from celery.signals import worker_process_init

    worker_process_init.connect(_install_tracers, weak=False)
