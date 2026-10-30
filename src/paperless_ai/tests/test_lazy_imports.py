import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

_SRC_DIR = Path(__file__).parent.parent.parent


class TestLazyAiImports:
    def test_skipped_ai_workflows_do_not_load_implementations(
        self,
        tmp_path: Path,
    ) -> None:
        """Keep no-op workflows lightweight with live config."""
        configuration = tmp_path / "paperless.conf"
        configuration.touch()
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("PAPERLESS_")
        }
        env.update(
            DJANGO_SETTINGS_MODULE="paperless.settings",
            PAPERLESS_CONFIGURATION_PATH=str(configuration),
            PAPERLESS_DATA_DIR=str(tmp_path),
            PAPERLESS_DBENGINE="sqlite",
            PAPERLESS_SECRET_KEY="workflow-import-test-secret",
            PAPERLESS_AI_ENABLED="true",
            PAPERLESS_AI_LLM_EMBEDDING_BACKEND="huggingface",
            PAPERLESS_CACHE_BACKEND="django.core.cache.backends.locmem.LocMemCache",
        )
        code = dedent(
            """\
            import sys
            import django
            from django.core.management import call_command

            django.setup()
            call_command("migrate", verbosity=0, interactive=False, skip_checks=True)
            from documents.models import Document, WorkflowAction
            from documents.workflows.ai import apply_ai_suggestions_to_document
            from paperless.config import AIConfig
            from paperless.models import ApplicationConfiguration

            document = Document(content="text")
            action = WorkflowAction(ai_suggestion_fields=[])
            config = ApplicationConfiguration.objects.first()
            config.ai_enabled = False
            config.save()
            assert AIConfig().llm_embedding_backend == "huggingface"
            assert not AIConfig().llm_index_enabled
            assert apply_ai_suggestions_to_document(action, document) == []
            action.ai_suggestion_fields = [WorkflowAction.AISuggestionField.TITLE]
            assert apply_ai_suggestions_to_document(action, document) == []
            config.ai_enabled = True
            config.save()
            assert AIConfig().llm_index_enabled
            document.content = " "
            assert apply_ai_suggestions_to_document(action, document) == []
            config.ai_enabled = False
            config.save()

            forbidden = (
                "paperless_ai.ai_classifier", "paperless_ai.indexing",
                "paperless_ai.embedding", "paperless_ai.client", "paperless_ai.base_model",
                "llama_index", "torch", "sentence_transformers", "sqlite_vec",
            )
            leaked = sorted(name for name in sys.modules if any(
                name == prefix or name.startswith(prefix + ".") for prefix in forbidden))
            assert not leaked, f"Skipped AI operations loaded implementations: {leaked}"
            """,
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=_SRC_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def test_importing_tasks_does_not_load_ai_libraries(self) -> None:
        code = (
            "import os, django, sys\n"
            "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'paperless.settings')\n"
            "django.setup()\n"
            "import documents.tasks  # noqa: F401\n"
            "leaked = [m for m in ('lancedb', 'pyarrow', 'llama_index', 'sqlite_vec') "
            "if m in sys.modules]\n"
            "assert not leaked, f'AI libraries leaked into the light path: {leaked}'\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            cwd=_SRC_DIR,
        )
        assert result.returncode == 0, result.stdout + result.stderr
