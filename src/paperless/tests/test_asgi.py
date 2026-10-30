"""Exercise HTTP routing through the configured ASGI middleware stack."""

import runpy
from pathlib import Path

import pytest
from channels.testing import HttpCommunicator
from pytest_django.fixtures import Settings
from pytest_mock import MockerFixture


@pytest.mark.anyio
@pytest.mark.parametrize("anyio_backend", ["asyncio"])
async def test_http_and_static_routing(
    tmp_path: Path,
    settings: Settings,
    mocker: MockerFixture,
) -> None:
    """Serve ordinary HTTP and indexed static files through the shared handler."""
    static_file = tmp_path / "asgi-test.txt"
    static_file.write_bytes(b"ASGI static content")
    settings.STATIC_ROOT = tmp_path
    settings.DEBUG = False
    settings.WHITENOISE_AUTOREFRESH = False
    settings.WHITENOISE_USE_FINDERS = False
    from django.core import asgi

    get_application = mocker.spy(asgi, "get_asgi_application")
    namespace = runpy.run_path(str(Path(__file__).parents[1] / "asgi.py"))
    application = namespace["application"]
    get_application.assert_called_once_with()
    assert application.application_mapping["http"] is namespace["django_asgi_app"]

    communicator = HttpCommunicator(application, "GET", "/")
    response = await communicator.get_response()
    assert response["status"] == 302
    headers = {name.lower(): value for name, value in response["headers"]}
    assert headers[b"location"] == b"/accounts/login/?next=/"
    await communicator.wait()

    communicator = HttpCommunicator(application, "GET", "/static/asgi-test.txt")
    await communicator.send_input({"type": "http.request", "body": b""})
    response = await communicator.receive_output()
    assert response["type"] == "http.response.start"
    assert response["status"] == 200
    headers = {name.lower(): value for name, value in response["headers"]}
    assert headers[b"content-type"].startswith(b"text/plain")
    body = b""
    while True:
        chunk = await communicator.receive_output()
        assert chunk["type"] == "http.response.body"
        # Django may omit body on the final empty streaming message.
        body += chunk.get("body", b"")
        if not chunk.get("more_body", False):
            break
    assert body == static_file.read_bytes()
    await communicator.wait()
