"""Keep deferred task messages independent of Django model deserialization."""

import json
from unittest.mock import patch

import pytest
from django.db import transaction
from django.test import override_settings

from documents import tasks
from documents.models import CustomField
from documents.models import CustomFieldInstance
from documents.models import Document
from documents.signals.handlers import add_or_update_document_in_llm_index
from documents.signals.handlers import process_cf_select_update
from paperless_testing.factories import DocumentFactory


@pytest.mark.django_db
@override_settings(AI_ENABLED=True, LLM_EMBEDDING_BACKEND="huggingface")
def test_ai_publishers_capture_ids_until_commit(
    django_capture_on_commit_callbacks,
) -> None:
    """Delay publication until commit and retain a deleted instance's former ID."""
    document = DocumentFactory()
    document_id = document.pk
    with (
        patch.object(tasks.update_document_in_llm_index, "apply_async") as update,
        patch.object(tasks.remove_document_from_llm_index, "apply_async") as remove,
        django_capture_on_commit_callbacks(execute=True),
    ):
        with transaction.atomic():
            add_or_update_document_in_llm_index(None, document)
            document.hard_delete()
            assert document.pk is None
            update.assert_not_called()
            remove.assert_not_called()
    update.assert_called_once_with(kwargs={"document_id": document_id})
    remove.assert_called_once_with(kwargs={"document_id": document_id})
    assert json.loads(json.dumps(remove.call_args.kwargs["kwargs"])) == {
        "document_id": document_id,
    }


@pytest.mark.django_db
@override_settings(AI_ENABLED=True, LLM_EMBEDDING_BACKEND="huggingface")
def test_rolled_back_ai_changes_publish_nothing(
    django_capture_on_commit_callbacks,
) -> None:
    """A rolled-back delete/update must not change the asynchronous index."""
    document = DocumentFactory()
    with (
        patch.object(tasks.update_document_in_llm_index, "apply_async") as update,
        patch.object(tasks.remove_document_from_llm_index, "apply_async") as remove,
        django_capture_on_commit_callbacks(execute=True),
    ):
        with pytest.raises(RuntimeError, match="rollback"), transaction.atomic():
            add_or_update_document_in_llm_index(None, document)
            document.hard_delete()
            raise RuntimeError("rollback")
    update.assert_not_called()
    remove.assert_not_called()


@pytest.mark.django_db
def test_index_update_loads_document_in_child_and_skips_deleted_rows() -> None:
    """New messages fetch committed data; disappearance before execution is safe."""
    document = DocumentFactory(content="old content")
    Document.objects.filter(pk=document.pk).update(content="committed content")
    with (
        patch("documents.tasks.AIConfig") as config,
        patch("paperless_ai.indexing.llm_index_add_or_update_document") as update,
    ):
        config.return_value.llm_index_enabled = True
        tasks.update_document_in_llm_index(document_id=document.pk)
        assert update.call_args.args[0].content == "committed content"
        update.reset_mock()
        tasks.update_document_in_llm_index(document_id=document.pk + 1)
        update.assert_not_called()


@pytest.mark.django_db
def test_index_removal_needs_no_document_row(django_assert_num_queries) -> None:
    """A deleted document's index ID is sufficient, without a database lookup."""
    with (
        patch("documents.tasks.AIConfig") as config,
        patch("paperless_ai.indexing.llm_index_remove_document") as remove,
    ):
        config.return_value.llm_index_enabled = True
        with django_assert_num_queries(0):
            tasks.remove_document_from_llm_index(document_id=1234)
        remove.assert_called_once_with(document_id=1234)


@pytest.mark.django_db
@pytest.mark.parametrize("legacy", [False, True])
def test_custom_field_uses_saved_snapshot_after_later_edits(
    django_capture_on_commit_callbacks,
    *,
    legacy: bool,
) -> None:
    """Capture option IDs/labels before commit and prune by that saved snapshot."""
    options = [{"id": "keep", "label": "Kept"}, {"id": "remove", "label": "Removed"}]
    field = CustomField.objects.create(
        name="selection",
        data_type=CustomField.FieldDataType.SELECT,
        extra_data={"select_options": options},
    )
    instance = CustomFieldInstance.objects.create(
        document=DocumentFactory(),
        field=field,
        value_select="remove",
    )
    with (
        patch.object(process_cf_select_update, "apply_async") as publish,
        django_capture_on_commit_callbacks(execute=True),
    ):
        field.extra_data = {"select_options": [options[0]]}
        field.save()
        publish.assert_not_called()
        field.extra_data["select_options"][0]["label"] = "mutated before commit"
    payload = publish.call_args.kwargs["kwargs"]
    assert json.loads(json.dumps(payload)) == {
        "custom_field_id": field.pk,
        "select_options": [{"id": "keep", "label": "Kept"}],
    }
    CustomField.objects.filter(pk=field.pk).update(
        extra_data={"select_options": [{"id": "remove", "label": "Restored later"}]},
    )
    with pytest.raises(ValueError, match="snapshot"):
        process_cf_select_update(custom_field_id=field.pk)
    instance.refresh_from_db()
    assert instance.value_select == "remove"
    if legacy:
        field.extra_data = {"select_options": payload["select_options"]}
        process_cf_select_update(field)
    else:
        process_cf_select_update(**payload)
    instance.refresh_from_db()
    assert instance.value_select is None


@pytest.mark.django_db
def test_rolled_back_custom_field_save_publishes_nothing(
    django_capture_on_commit_callbacks,
) -> None:
    """Do not prune values for an option update that was rolled back."""
    field = CustomField.objects.create(
        name="selection",
        data_type=CustomField.FieldDataType.SELECT,
        extra_data={"select_options": [{"id": "keep", "label": "Kept"}]},
    )
    CustomFieldInstance.objects.create(
        document=DocumentFactory(),
        field=field,
        value_select="keep",
    )
    with (
        patch.object(process_cf_select_update, "apply_async") as publish,
        django_capture_on_commit_callbacks(execute=True),
    ):
        with pytest.raises(RuntimeError, match="rollback"), transaction.atomic():
            field.extra_data = {"select_options": []}
            field.save()
            raise RuntimeError("rollback")
    publish.assert_not_called()
