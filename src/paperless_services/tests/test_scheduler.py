"""Verify the external scheduler's commands and one-shot publication behavior."""

import os
import shlex
from pathlib import Path
from unittest.mock import Mock

import pytest

from paperless_services import scheduler


@pytest.mark.parametrize(
    "jobs",
    [[], [{"name": "Bob's scheduled job", "cron": "15 0 * 11 2"}]],
)
def test_run_uses_protected_crontab_and_timezone(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    jobs: list[dict],
) -> None:
    """Empty schedules work and job names are quoted without exposing credentials."""
    client = Mock()
    client.request.return_value = {"timezone": "Europe/Rome", "jobs": jobs}
    monkeypatch.setenv("TZ", "UTC")
    monkeypatch.setenv("PAPERLESS_DATA_DIR", str(tmp_path))
    existing = tmp_path / "api-scheduler.crontab"
    existing.write_text("old contents")
    existing.chmod(0o644)
    paths = []

    def capture_exec(command: str, argv: list[str]) -> None:
        """Inspect the exact file passed to Supercronic before replacing the process."""
        assert command == "supercronic"
        assert argv[0] == "supercronic"
        path = Path(argv[1])
        assert path == existing
        paths.append(path)
        assert path.stat().st_mode & 0o777 == 0o600
        assert os.environ["TZ"] == "Europe/Rome"
        lines = path.read_text().splitlines()
        assert len(lines) == len(jobs)
        if jobs:
            assert lines[0].startswith("15 0 * 11 2 ")
            assert shlex.split(lines[0])[5:] == [
                scheduler.sys.executable,
                "-m",
                "paperless_services.scheduler",
                "dispatch",
                jobs[0]["name"],
            ]

    monkeypatch.setattr(scheduler.os, "execvp", capture_exec)
    try:
        scheduler.run(client)
    finally:
        for path in paths:
            path.unlink()
    client.request.assert_called_once_with("GET", scheduler.ENDPOINT)


def test_dispatch_does_not_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """An ambiguous failed POST must not publish the same scheduled job again."""
    client = Mock()
    client.request.side_effect = OSError("connection lost")
    monkeypatch.setattr(scheduler, "APIClient", lambda: client)
    monkeypatch.setattr(
        scheduler.sys,
        "argv",
        ["scheduler", "dispatch", "Train the classifier"],
    )
    with pytest.raises(OSError, match="connection lost"):
        scheduler.main()
    client.request.assert_called_once_with(
        "POST",
        scheduler.ENDPOINT,
        {"name": "Train the classifier"},
    )
