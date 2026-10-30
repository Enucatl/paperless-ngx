"""Authenticated JSON requests for the local service replacements."""

import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler
from urllib.request import Request
from urllib.request import build_opener


class NoRedirects(HTTPRedirectHandler):
    """Keep the admin token on the configured API endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Reject redirects instead of forwarding service credentials."""
        raise HTTPError(
            req.full_url,
            code,
            "API redirects are not supported",
            headers,
            fp,
        )


class APIClient:
    """Read service credentials and communicate only through the web API."""

    def __init__(self) -> None:
        """Read a token from the environment or a mounted secret file."""
        self.url = os.environ.get("PAPERLESS_API_URL", "http://127.0.0.1:8000").rstrip(
            "/",
        )
        token = os.environ.get("PAPERLESS_API_TOKEN", "")
        token_file = os.environ.get("PAPERLESS_API_TOKEN_FILE")
        if token_file:
            token = Path(token_file).read_text().strip()
        if not token:
            raise ValueError("An admin API token is required")
        self._token = token
        self._opener = build_opener(NoRedirects())

    def request(self, method: str, path: str, data: dict | None = None):
        """Send one request, leaving operation-specific retry decisions to callers."""
        request = Request(
            self.url + path,
            data=json.dumps(data).encode() if data is not None else None,
            headers={
                "Authorization": f"Token {self._token}",
                "Accept": "application/json; version=10",
                "Content-Type": "application/json",
            },
            method=method,
        )
        with self._opener.open(request, timeout=30) as response:
            return json.load(response)
