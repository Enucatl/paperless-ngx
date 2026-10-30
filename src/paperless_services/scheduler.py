"""Run Supercronic against Paperless's configured schedules through its API."""

import argparse
import os
import shlex
import sys
from pathlib import Path

from paperless_services.http import APIClient

ENDPOINT = "/api/services/scheduler/"


def run(client: APIClient) -> None:
    """Fetch schedules once and replace this process with Supercronic."""
    configuration = client.request("GET", ENDPOINT)
    os.environ["TZ"] = configuration["timezone"]
    path = (
        Path(os.environ.get("PAPERLESS_DATA_DIR", "/usr/src/paperless/data"))
        / "api-scheduler.crontab"
    )
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w") as crontab:
        for job in configuration["jobs"]:
            command = shlex.join(
                [
                    sys.executable,
                    "-m",
                    "paperless_services.scheduler",
                    "dispatch",
                    job["name"],
                ],
            )
            crontab.write(f"{job['cron']} {command}\n")
    os.execvp("supercronic", ["supercronic", str(path)])


def main() -> None:
    """Start scheduling, or make one dispatch request without automatic retries."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "dispatch"))
    parser.add_argument("name", nargs="?")
    args = parser.parse_args()
    if (args.command == "dispatch") != (args.name is not None):
        parser.error("Only dispatch requires a schedule name.")
    client = APIClient()
    if args.command == "dispatch":
        client.request("POST", ENDPOINT, {"name": args.name})
    else:
        run(client)


if __name__ == "__main__":
    main()
