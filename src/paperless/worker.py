"""Run Docker's prefork worker without keeping idle children resident."""

from paperless.celery import app


def main() -> None:
    """Use resolved Paperless concurrency as the autoscaling upper limit."""
    maximum = app.conf.worker_concurrency
    if maximum < 1:
        raise ValueError("PAPERLESS_TASK_WORKERS must be greater than zero.")
    app.worker_main(
        [
            "worker",
            "--autoscale",
            f"{maximum},0",
            "--loglevel",
            "INFO",
            "--without-mingle",
            "--without-gossip",
        ],
    )


if __name__ == "__main__":
    main()
