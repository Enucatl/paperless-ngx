from __future__ import annotations

from typing import Any

from django.conf import settings

from paperless_worker.serialization import HMAC_SIZE
from paperless_worker.serialization import SignedPickleError
from paperless_worker.serialization import dumps
from paperless_worker.serialization import loads

__all__ = [
    "HMAC_SIZE",
    "SignedPickleError",
    "signed_pickle_dumps",
    "signed_pickle_loads",
]


def _get_signing_key() -> bytes:
    return settings.SECRET_KEY.encode()


def signed_pickle_dumps(obj: object) -> bytes:
    return dumps(obj, _get_signing_key())


def signed_pickle_loads(payload: bytes) -> Any:
    return loads(payload, _get_signing_key())
