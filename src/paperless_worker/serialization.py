"""Share the signed task wire format without initializing Django."""

from __future__ import annotations

import hmac
import io
import pickle
from functools import partial
from hashlib import sha256
from typing import Any

HMAC_SIZE = sha256().digest_size


class SignedPickleError(ValueError):
    """Reject an unauthenticated signed-pickle payload."""


class LegacyModelPayloadError(ValueError):
    """Require legacy model-valued messages to drain on a normal worker."""


class _ParentUnpickler(pickle.Unpickler):
    """Reject Django model reconstruction before it imports application models."""

    def find_class(self, module: str, name: str) -> Any:
        if (module, name) == ("django.db.models.base", "model_unpickle"):
            raise LegacyModelPayloadError(
                "Legacy model-valued task: drain queued work with the normal worker "
                "before enabling PAPERLESS_USE_LEAN_WORKER.",
            )
        return super().find_class(module, name)


def dumps(obj: object, key: bytes) -> bytes:
    """Authenticate the existing pickle wire format with the supplied key."""
    data = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    return hmac.new(key, data, sha256).digest() + data


def loads(payload: bytes, key: bytes, *, reject_models: bool = False) -> Any:
    """Authenticate before decoding, optionally rejecting legacy model payloads."""
    if len(payload) <= HMAC_SIZE:
        raise SignedPickleError("Signed-pickle payload too short")
    signature, data = payload[:HMAC_SIZE], payload[HMAC_SIZE:]
    expected = hmac.new(key, data, sha256).digest()
    if not hmac.compare_digest(signature, expected):
        raise SignedPickleError(
            "Signed-pickle HMAC verification failed; payload may have been tampered with",
        )
    if reject_models:
        return _ParentUnpickler(io.BytesIO(data)).load()
    return pickle.loads(data)


def register(key: bytes, *, reject_models: bool = True) -> None:
    """Register the shared wire format using only the parent's supplied key."""
    from kombu.serialization import register as register_serializer

    register_serializer(
        "signed-pickle",
        partial(dumps, key=key),
        partial(loads, key=key, reject_models=reject_models),
        content_type="application/x-signed-pickle",
        content_encoding="binary",
    )
