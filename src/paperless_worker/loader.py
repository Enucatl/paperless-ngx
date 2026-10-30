"""Initialize the real Django application only after the worker forks."""

import importlib

from celery.loaders.base import BaseLoader


class ChildDjangoLoader(BaseLoader):
    """Replace metadata tasks before Celery builds the child's execution tracers."""

    def on_worker_process_init(self) -> None:
        """Restore the normal app, registration, connections and task classes."""
        from celery.fixups.django import DjangoWorkerFixup

        import paperless_worker

        expected = self.app._paperless_metadata["tasks"]
        self.app._tasks.clear()
        self.app.finalized = False
        self.app.task_cls = "celery.contrib.django.task:DjangoTask"
        self.app.__dict__.pop("Task", None)
        paperless_worker.child_app = self.app
        self.app.set_current()
        self.app.set_default()

        from paperless.celery import app

        if app is not self.app:
            raise RuntimeError("The child did not retain the configured Celery app.")
        import django

        django.setup()
        self._django_fixup = DjangoWorkerFixup(app)
        self._django_fixup.install()
        app.loader.import_default_modules()
        app.autodiscover_tasks(force=True)
        for module in {task["module"] for task in expected.values()}:
            importlib.import_module(module)
        app.finalize()
        if set(app.tasks) != set(expected):
            raise RuntimeError(
                "Task registration changed after worker metadata was read.",
            )
        from paperless_worker.bootstrap import MetadataTask

        if any(isinstance(task, MetadataTask) for task in app.tasks.values()):
            raise RuntimeError("A metadata task survived child initialization.")
        from paperless_worker.trace import install

        install()
