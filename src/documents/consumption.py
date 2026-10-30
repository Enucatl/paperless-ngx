"""Queue consumption-folder files without importing a filesystem watcher."""

import logging
from pathlib import Path

from django import db

from documents.data_models import ConsumableDocument
from documents.data_models import DocumentMetadataOverrides
from documents.data_models import DocumentSource
from documents.models import PaperlessTask
from documents.models import Tag
from documents.tasks import consume_file

logger = logging.getLogger("paperless.management.consumer")


def _tags_from_path(filepath: Path, consumption_dir: Path) -> list[int]:
    """
    Walk up the directory tree from filepath to consumption_dir
    and get or create Tag IDs for every directory.

    Returns list of Tag primary keys.
    """
    db.close_old_connections()
    tag_ids: set[int] = set()
    path_parts = filepath.relative_to(consumption_dir).parent.parts

    for part in path_parts:
        tag, _ = Tag.objects.get_or_create(
            name__iexact=part,
            defaults={"name": part},
        )
        tag_ids.add(tag.pk)

    return list(tag_ids)


def _consume_file(
    filepath: Path,
    consumption_dir: Path,
    *,
    subdirs_as_tags: bool,
    task_id: str | None = None,
    submission_key: str | None = None,
) -> bool:
    """
    Queue a file for consumption.

    Args:
        filepath: Path to the file to consume.
        consumption_dir: Base consumption directory.
        subdirs_as_tags: Whether to create tags from subdirectory names.
        task_id: Stable task ID for an API submission.
        submission_key: Opaque file identity for deduplicating API retries.

    Returns:
        True if the file was successfully handed to Celery, False otherwise.
        The original watcher retries False results. API callers must check the
        reserved PaperlessTask and never republish an existing receipt, because
        publication may have succeeded before an error was reported.
    """
    # Verify file still exists and is accessible
    try:
        if not filepath.is_file():
            logger.debug(f"Not consuming {filepath}: not a file or doesn't exist")
            return False
    except OSError as e:
        logger.warning(f"Not consuming {filepath}: {e}")
        return False

    # Get tags from path if configured
    tag_ids: list[int] | None = None
    if subdirs_as_tags:
        try:
            tag_ids = _tags_from_path(filepath, consumption_dir)
        except Exception:
            logger.exception(f"Error creating tags from path for {filepath}")

    # Queue for consumption
    try:
        task_kwargs = {
            "input_doc": ConsumableDocument(
                source=DocumentSource.ConsumeFolder,
                original_file=filepath,
                defer_mime_type=task_id is not None,
            ),
            "overrides": DocumentMetadataOverrides(tag_ids=tag_ids),
        }
        options = {}
        if task_id is not None:
            from documents.signals.handlers import _extract_input_data

            input_data = _extract_input_data(
                PaperlessTask.TaskType.CONSUME_FILE,
                task_kwargs,
            )
            input_data["api_consumer_key"] = submission_key
            # Reserve before publishing: a retry must never publish the same ID twice.
            PaperlessTask.objects.create(
                task_id=task_id,
                task_type=PaperlessTask.TaskType.CONSUME_FILE,
                trigger_source=PaperlessTask.TriggerSource.FOLDER_CONSUME,
                input_data=input_data,
                result_data={"api_publication_uncertain": True},
            )
            options["task_id"] = task_id
        logger.info(f"Adding {filepath} to the task queue")
        consume_file.apply_async(
            kwargs=task_kwargs,
            headers={"trigger_source": PaperlessTask.TriggerSource.FOLDER_CONSUME},
            **options,
        )
        if task_id is not None:
            PaperlessTask.objects.filter(
                task_id=task_id,
                status=PaperlessTask.Status.PENDING,
                result_data__api_publication_uncertain=True,
            ).update(result_data=None)
    except Exception:
        logger.exception(f"Error while queuing document {filepath}")
        return False

    return True
