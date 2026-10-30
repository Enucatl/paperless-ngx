import logging

from paperless.logging import ConsumeTaskFormatter
from paperless.logging import consume_task_id
from paperless_worker.logging import ConsumeTaskFormatter as ParentFormatter
from paperless_worker.logging import consume_task_id as parent_task_id


def test_parent_and_application_share_formatter_context() -> None:
    """Application and parent import paths keep one task correlation context."""
    assert ParentFormatter is ConsumeTaskFormatter
    assert parent_task_id is consume_task_id


def _make_record(msg: str = "Test message") -> logging.LogRecord:
    return logging.LogRecord(
        name="paperless.consumer",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=msg,
        args=(),
        exc_info=None,
    )


def test_formatter_includes_task_id_when_set():
    token = consume_task_id.set("a8098c1a")
    try:
        formatter = ConsumeTaskFormatter()
        output = formatter.format(_make_record())
        assert "[a8098c1a] Test message" in output
    finally:
        consume_task_id.reset(token)


def test_formatter_omits_prefix_when_no_task_id():
    # ContextVar default is "" — no task active
    formatter = ConsumeTaskFormatter()
    output = formatter.format(_make_record())
    assert "[] " not in output
    assert "Test message" in output
