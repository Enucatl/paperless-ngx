"""Keep Docker's initial validation while avoiding repeated service checks."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
SERVICES = ROOT / "docker/rootfs/etc/s6-overlay/s6-rc.d"


@pytest.fixture
def service_env(tmp_path: Path) -> dict[str, str]:
    """Use isolated data and leave standalone check defaults unset."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PAPERLESS_", "CELERY_", "DJANGO_"))
    }
    configuration = tmp_path / "paperless.conf"
    configuration.touch()
    env.update(
        PAPERLESS_CONFIGURATION_PATH=str(configuration),
        PAPERLESS_SRC_DIR=str(ROOT / "src"),
        PAPERLESS_SECRET_KEY="service-check-test-not-production",
        PAPERLESS_DBENGINE="sqlite",
        PAPERLESS_DBHOST="",
        PAPERLESS_DISABLE_DBHANDLER="true",
        PYTHONPATH=str(ROOT / "src"),
        PYTHONDONTWRITEBYTECODE="1",
        TEST_PYTHON=sys.executable,
        USER_IS_NON_ROOT="1",
        C_FORCE_ROOT="1",
    )
    for name in ("DATA_DIR", "MEDIA_ROOT", "CONSUMPTION_DIR", "SCRATCH_DIR"):
        directory = tmp_path / name.lower()
        directory.mkdir()
        env[f"PAPERLESS_{name}"] = str(directory)
    # Exercise the privilege branch without changing this test process's identity.
    commands = tmp_path / "bin"
    commands.mkdir()
    switch_user = commands / "s6-setuidgid"
    switch_user.write_text(
        '#!/bin/sh\n[ "$1" = paperless ] || exit 91\nshift\nexec "$@"\n',
    )
    switch_user.chmod(0o755)
    env["PATH"] = f"{commands}:{Path(sys.executable).parent}:{env['PATH']}"
    return env


def launch_spy(service: str, env: dict[str, str]) -> dict:
    """Run the real shell branches and capture their final exec boundary."""
    shell = """
exec() {
    "$TEST_PYTHON" -c 'import json, os, sys; print(json.dumps({"argv": sys.argv[1:], "skip": os.environ.get("CELERY_SKIP_CHECKS"), "cwd": os.getcwd()}))' "$@"
    exit $?
}
s6-svc() { exec s6-svc "$@"; }
. "$1"
"""
    result = subprocess.run(
        ["bash", "-c", shell, "test-service", str(SERVICES / service / "run")],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return json.loads(result.stdout.splitlines()[-1])


@pytest.mark.parametrize("non_root", ["", "1"])
@pytest.mark.parametrize(
    ("service", "command"),
    [
        (
            "svc-worker",
            ["python3", "-m", "paperless.worker"],
        ),
        (
            "svc-scheduler",
            [
                "/usr/local/bin/celery",
                "--app",
                "paperless",
                "beat",
                "--loglevel",
                "INFO",
            ],
        ),
        (
            "svc-consumer",
            ["python3", "manage.py", "document_consumer", "--skip-checks"],
        ),
    ],
)
def test_service_exec(
    service_env: dict[str, str],
    non_root: str,
    service: str,
    command: list[str],
) -> None:
    """Both privilege branches retain their commands and skip only local checks."""
    service_env["USER_IS_NON_ROOT"] = non_root
    result = launch_spy(service, service_env)
    assert (
        result["argv"] == ([] if non_root else ["s6-setuidgid", "paperless"]) + command
    )
    assert result["cwd"] == str(ROOT / "src")
    assert result["skip"] == (None if service == "svc-consumer" else "1")
    assert "CELERY_SKIP_CHECKS" not in service_env


@pytest.mark.parametrize("non_root", ["", "1"])
def test_lean_worker_opt_in(service_env: dict[str, str], non_root: str) -> None:
    """Only the explicit flag changes the worker entrypoint in either branch."""
    service_env.update(USER_IS_NON_ROOT=non_root, PAPERLESS_USE_LEAN_WORKER="1")
    result = launch_spy("svc-worker", service_env)
    assert result["argv"] == (
        ([] if non_root else ["s6-setuidgid", "paperless"])
        + ["python3", "-m", "paperless_worker"]
    )
    assert result["skip"] == "1"


@pytest.mark.parametrize(
    ("configured", "environment", "expected"),
    [(None, None, 1), (3, None, 3), (3, 2, 2)],
)
def test_worker_autoscale_configuration(
    service_env: dict[str, str],
    configured: int | None,
    environment: int | None,
    expected: int,
) -> None:
    """Resolve worker limits through the existing configuration precedence."""
    if configured is not None:
        Path(service_env["PAPERLESS_CONFIGURATION_PATH"]).write_text(
            f"PAPERLESS_TASK_WORKERS={configured}\n",
        )
    if environment is not None:
        service_env["PAPERLESS_TASK_WORKERS"] = str(environment)
    code = """
import json
import runpy
from unittest.mock import patch
with patch('paperless.celery.app.worker_main') as worker:
    runpy.run_module('paperless.worker', run_name='__main__')
worker.assert_called_once()
print(json.dumps(worker.call_args.args[0]))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT / "src",
        env=service_env,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert json.loads(result.stdout.splitlines()[-1]) == [
        "worker",
        "--autoscale",
        f"{expected},0",
        "--loglevel",
        "INFO",
        "--without-mingle",
        "--without-gossip",
    ]


@pytest.mark.parametrize("maximum", [0, -1])
def test_worker_rejects_nonpositive_limit(
    service_env: dict[str, str],
    maximum: int,
) -> None:
    """Never start an autoscaling pool that cannot execute queued tasks."""
    code = (
        "from unittest.mock import patch\n"
        "from paperless.worker import main\n"
        "with patch('paperless.worker.app') as app:\n"
        f"    app.conf.worker_concurrency = {maximum}\n"
        "    main()\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT / "src",
        env=service_env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0
    assert "PAPERLESS_TASK_WORKERS must be greater than zero" in result.stderr


@pytest.mark.parametrize("non_root", ["", "1"])
@pytest.mark.parametrize(
    ("service", "disable"),
    [
        ("svc-consumer", "PAPERLESS_CONSUMER_DISABLE"),
        ("svc-scheduler", "PAPERLESS_SCHEDULER_DISABLE"),
    ],
)
def test_disabled_service(
    service_env: dict[str, str],
    non_root: str,
    service: str,
    disable: str,
) -> None:
    """Either resident service can be disabled without starting Django."""
    service_env.update(USER_IS_NON_ROOT=non_root)
    service_env[disable] = "1"
    result = launch_spy(service, service_env)
    assert result["argv"] == ["s6-svc", "-Od", "."]
    assert result["skip"] is None


@pytest.mark.parametrize("service", ["svc-worker", "svc-scheduler", "svc-consumer"])
def test_initial_checks_are_required(service: str) -> None:
    """The real S6 dependency closure gates each service on initial validation."""
    pending = [service]
    visited = set()
    while pending:
        current = pending.pop()
        if current not in visited:
            visited.add(current)
            pending.extend(
                path.name for path in (SERVICES / current / "dependencies.d").glob("*")
            )
    assert "init-system-checks" in visited
    assert (SERVICES / "init-system-checks" / "type").read_text().strip() == "oneshot"


@pytest.mark.parametrize("non_root", ["", "1"])
@pytest.mark.parametrize("invalid", [False, True])
def test_initial_check_script(
    service_env: dict[str, str],
    non_root: str,
    *,
    invalid: bool,
) -> None:
    """The actual initial-check script propagates Django validation failures."""
    service_env["USER_IS_NON_ROOT"] = non_root
    if invalid:
        Path(service_env["PAPERLESS_CONSUMPTION_DIR"]).rmdir()
    result = subprocess.run(
        ["bash", str(SERVICES / "init-system-checks" / "run")],
        env=service_env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if invalid:
        assert result.returncode != 0
        assert "PAPERLESS_CONSUMPTION_DIR is set but doesn't exist" in result.stderr
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert "System check identified no issues" in result.stdout


def test_standalone_consumer_still_validates(service_env: dict[str, str]) -> None:
    """The bare-metal consumer rejects bad settings before its watcher starts."""
    Path(service_env["PAPERLESS_CONSUMPTION_DIR"]).rmdir()
    assert "CELERY_SKIP_CHECKS" not in service_env
    result = subprocess.run(
        [sys.executable, "manage.py", "document_consumer", "--oneshot"],
        cwd=ROOT / "src",
        env=service_env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "SystemCheckError" in result.stderr
    assert "PAPERLESS_CONSUMPTION_DIR is set but doesn't exist" in result.stderr


@pytest.mark.parametrize("command", ["worker", "beat"])
def test_standalone_celery_still_runs_checks(
    service_env: dict[str, str],
    command: str,
) -> None:
    """Standalone Celery still invokes real Django checks with invalid settings."""
    Path(service_env["PAPERLESS_CONSUMPTION_DIR"]).rmdir()
    assert "CELERY_SKIP_CHECKS" not in service_env
    # Celery ignores returned check errors; stop after observing them, before startup.
    code = """
import runpy
import sys
from unittest.mock import patch
from django.core.checks import run_checks

def record_checks(*args, **kwargs):
    errors = run_checks(*args, **kwargs)
    for error in errors:
        print(error.msg)
    raise SystemExit(85)

sys.argv = ['celery', '--app', 'paperless', sys.argv[1], '--loglevel', 'INFO']
with patch('django.core.checks.run_checks', side_effect=record_checks):
    runpy.run_module('celery', run_name='__main__')
"""
    result = subprocess.run(
        [sys.executable, "-c", code, command],
        cwd=ROOT / "src",
        env=service_env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 85, result.stdout + result.stderr
    assert "PAPERLESS_CONSUMPTION_DIR is set but doesn't exist" in result.stdout


@pytest.mark.parametrize("non_root", ["", "1"])
@pytest.mark.parametrize(
    ("service", "enable", "disable", "command"),
    [
        (
            "svc-scheduler",
            "PAPERLESS_USE_API_SCHEDULER",
            "PAPERLESS_SCHEDULER_DISABLE",
            ["python3", "-m", "paperless_services.scheduler", "run"],
        ),
        (
            "svc-consumer",
            "PAPERLESS_USE_API_CONSUMER",
            "PAPERLESS_CONSUMER_DISABLE",
            ["python3", "-m", "paperless_services.watcher"],
        ),
    ],
)
def test_api_service_is_opt_in(
    service_env: dict[str, str],
    non_root: str,
    service: str,
    enable: str,
    disable: str,
    command: list[str],
) -> None:
    """Each optional API client replaces only its service and respects disablement."""
    service_env.update(USER_IS_NON_ROOT=non_root)
    service_env[enable] = "1"
    result = launch_spy(service, service_env)
    assert (
        result["argv"] == ([] if non_root else ["s6-setuidgid", "paperless"]) + command
    )
    assert result["skip"] is None
    service_env[disable] = "1"
    assert launch_spy(service, service_env)["argv"] == ["s6-svc", "-Od", "."]
