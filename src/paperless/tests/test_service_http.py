"""Exercise credential loading and redirect refusal in the shared API client."""

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError

import pytest

from paperless_services.http import APIClient


@pytest.fixture
def credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep host service settings out of each credential-loading test."""
    for name in (
        "PAPERLESS_API_URL",
        "PAPERLESS_API_TOKEN",
        "PAPERLESS_API_TOKEN_FILE",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def endpoint(
    credentials: None,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[list[dict]]:
    """Serve local JSON and redirects, recording whether a redirect was followed."""
    requests = []

    class Handler(BaseHTTPRequestHandler):
        """Record test requests without logging their synthetic credentials."""

        def do_GET(self) -> None:
            """Return JSON or an explicit redirect to a different hostname."""
            size = int(self.headers.get("Content-Length", "0"))
            requests.append(
                {
                    "method": self.command,
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "accept": self.headers.get("Accept"),
                    "body": self.rfile.read(size),
                },
            )
            if self.path.startswith("/redirect/"):
                self.send_response(int(self.path.rsplit("/", 1)[1]))
                self.send_header(
                    "Location",
                    f"http://localhost:{self.server.server_port}/target",
                )
                self.end_headers()
            else:
                body = json.dumps({"ok": True}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        do_POST = do_GET

        def log_message(self, format: str, *args: object) -> None:
            """Suppress the local test server's request log."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("PAPERLESS_API_URL", f"http://127.0.0.1:{server.server_port}/")
    try:
        yield requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("source", ["environment", "file", "both"])
def test_requests_use_configured_token_and_json(
    endpoint: list[dict],
    source: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both credential sources authenticate real JSON requests identically."""
    if source in ("file", "both"):
        token_file = tmp_path / "token"
        token_file.write_text("  synthetic-token\n")
        monkeypatch.setenv("PAPERLESS_API_TOKEN_FILE", str(token_file))
        if source == "both":
            monkeypatch.setenv("PAPERLESS_API_TOKEN", "environment-token")
    else:
        monkeypatch.setenv("PAPERLESS_API_TOKEN", "synthetic-token")
    client = APIClient()
    assert client.request(
        "POST",
        "/api/services/consumer/",
        {"path": "invoice.txt"},
    ) == {"ok": True}
    assert endpoint == [
        {
            "method": "POST",
            "path": "/api/services/consumer/",
            "authorization": "Token synthetic-token",
            "accept": "application/json; version=10",
            "body": b'{"path": "invoice.txt"}',
        },
    ]


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_redirect_does_not_forward_credentials(
    endpoint: list[dict],
    status: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not send the superuser token to a redirect target, even locally."""
    monkeypatch.setenv("PAPERLESS_API_TOKEN", "synthetic-token")
    client = APIClient()
    with pytest.raises(HTTPError, match="redirects are not supported") as error:
        client.request("GET", f"/redirect/{status}")
    assert error.value.code == status
    assert len(endpoint) == 1
    assert endpoint[0]["path"] == f"/redirect/{status}"


def test_missing_token_fails_before_request(credentials: None) -> None:
    """An unauthenticated replacement service must fail visibly at startup."""
    with pytest.raises(ValueError, match="admin API token is required"):
        APIClient()


def test_unreadable_token_file_fails(
    credentials: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not fall back to the environment when the token file cannot be read."""
    monkeypatch.setenv("PAPERLESS_API_TOKEN", "synthetic-token")
    monkeypatch.setenv("PAPERLESS_API_TOKEN_FILE", "/nonexistent-token-file")
    with pytest.raises(FileNotFoundError):
        APIClient()


def test_empty_token_file_fails(
    credentials: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Whitespace-only secret files are not valid credentials."""
    token_file = tmp_path / "token"
    token_file.write_text(" \n")
    monkeypatch.setenv("PAPERLESS_API_TOKEN_FILE", str(token_file))
    with pytest.raises(ValueError, match="admin API token is required"):
        APIClient()
