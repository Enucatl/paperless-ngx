"""Preserve authenticated task bytes without Django setup in the parent."""

import hmac
import os
import pickle
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import pytest

from documents.data_models import ConsumableDocument
from documents.data_models import DocumentSource
from documents.models import Document
from paperless_worker.serialization import HMAC_SIZE
from paperless_worker.serialization import LegacyModelPayloadError
from paperless_worker.serialization import SignedPickleError
from paperless_worker.serialization import dumps
from paperless_worker.serialization import loads

KEY = b"disposable-worker-serialization-key"


def test_signed_wire_format_is_unchanged() -> None:
    """Existing signatures and complex primitive task data remain compatible."""
    value = {"path": Path("/tmp/document.pdf"), "ids": [1, 2], "flags": {"ocr"}}
    data = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    expected = hmac.new(KEY, data, sha256).digest() + data
    assert dumps(value, KEY) == expected
    assert loads(expected, KEY, reject_models=True) == value


@pytest.mark.parametrize("reject_models", [False, True])
def test_authentication_precedes_all_unpickling(*, reject_models: bool) -> None:
    """Neither decoder may inspect objects before verifying the signature."""
    payload = dumps({"id": 1}, KEY)
    with (
        patch("paperless_worker.serialization.pickle.loads") as unpickle,
        patch("paperless_worker.serialization._ParentUnpickler") as parent,
        pytest.raises(SignedPickleError, match="HMAC verification failed"),
    ):
        loads(payload, b"wrong-key", reject_models=reject_models)
    unpickle.assert_not_called()
    parent.assert_not_called()


@pytest.mark.parametrize("payload", [b"", bytes(HMAC_SIZE)])
def test_truncated_payload_is_rejected(payload: bytes) -> None:
    """Empty or signature-only messages never reach pickle decoding."""
    with pytest.raises(SignedPickleError, match="too short"):
        loads(payload, KEY, reject_models=True)


def test_normal_worker_keeps_legacy_model_compatibility() -> None:
    """The staged rollout can drain old model messages on a normal worker."""
    payload = dumps({"document": Document(id=42, title="queued snapshot")}, KEY)
    assert loads(payload, KEY)["document"].title == "queued snapshot"
    with pytest.raises(LegacyModelPayloadError, match="drain queued work"):
        loads(payload, KEY, reject_models=True)


@pytest.mark.parametrize("model", [False, True])
def test_parent_decoding_never_initializes_django(*, model: bool) -> None:
    """Accept DTOs and reject models before importing Django in a fresh process."""
    value = (
        Document(id=42, title="old queued model")
        if model
        else ConsumableDocument(
            DocumentSource.ConsumeFolder,
            Path("/tmp/disposable-document.txt"),
            defer_mime_type=True,
        )
    )
    code = """
import sys
from paperless_worker.serialization import loads, LegacyModelPayloadError
try:
    value = loads(sys.stdin.buffer.read(), b'disposable-worker-serialization-key', reject_models=True)
except LegacyModelPayloadError:
    assert sys.argv[1] == 'model'
else:
    assert sys.argv[1] == 'dto'
    assert value.original_file.name == 'disposable-document.txt'
assert not any(name == 'django' or name.startswith('django.') for name in sys.modules)
assert 'paperless.celery' not in sys.modules
assert 'documents.models' not in sys.modules
"""
    source = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-c", code, "model" if model else "dto"],
        input=dumps(value, KEY),
        env={**os.environ, "PYTHONPATH": str(source)},
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode()
