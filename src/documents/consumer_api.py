"""Queue shared consumption-directory files through the authenticated API."""

import hashlib
import json
from pathlib import Path

from django.conf import settings
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import extend_schema
from filelock import FileLock
from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from documents.consumption import _consume_file
from documents.models import PaperlessTask
from documents.parsers import get_supported_file_extensions
from documents.scheduler_api import SuperuserPermission
from paperless_services.filtering import FileFilter


class ConsumptionSerializer(serializers.Serializer):
    """Identify a file without accepting task arguments or metadata overrides."""

    path = serializers.CharField(trim_whitespace=False)
    request_id = serializers.UUIDField()
    size = serializers.IntegerField(min_value=0)
    mtime_ns = serializers.IntegerField(min_value=0)

    def validate(self, attrs: dict) -> dict:
        """Reject extra fields and paths that can escape the consumption root."""
        if set(self.initial_data) != set(self.fields):
            raise ValidationError(
                "Only path, request_id, size and mtime_ns are accepted.",
            )
        path = Path(attrs["path"])
        if path.is_absolute() or ".." in path.parts or "\x00" in attrs["path"]:
            raise ValidationError(
                {"path": "A path inside the consumption directory is required."},
            )
        attrs["path"] = path.as_posix()
        return attrs


def _receipt(task: PaperlessTask) -> Response:
    """Report a previous admission without publishing another Celery message."""
    uncertain = task.status == PaperlessTask.Status.PENDING and (
        task.result_data or {}
    ).get("api_publication_uncertain", False)
    return Response(
        {
            "task_id": task.task_id,
            "status": "publication_uncertain" if uncertain else "queued",
        },
        status=202,
    )


class ConsumerAPIView(APIView):
    """Preserve folder-consumption behavior while using an external watcher."""

    permission_classes = (SuperuserPermission,)

    @extend_schema(responses=OpenApiTypes.OBJECT)
    def get(self, request: Request) -> Response:
        """Return the server's effective detection policy."""
        return Response(
            {
                "recursive": settings.CONSUMER_RECURSIVE,
                "subdirs_as_tags": settings.CONSUMER_SUBDIRS_AS_TAGS,
                "polling_interval": settings.CONSUMER_POLLING_INTERVAL,
                "stability_delay": settings.CONSUMER_STABILITY_DELAY,
                "ignore_patterns": settings.CONSUMER_IGNORE_PATTERNS,
                "ignore_dirs": settings.CONSUMER_IGNORE_DIRS,
                "supported_extensions": sorted(get_supported_file_extensions()),
            },
        )

    @extend_schema(request=ConsumptionSerializer, responses=OpenApiTypes.OBJECT)
    def post(self, request: Request) -> Response:
        """Validate and admit a file once, retaining its original folder context."""
        serializer = ConsumptionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        task_id = str(data["request_id"])
        submission_key = hashlib.sha256(
            json.dumps([data["path"], data["size"], data["mtime_ns"]]).encode(),
        ).hexdigest()
        if not settings.CONSUMPTION_DIR:
            raise ValidationError("The consumption directory is not configured.")
        root = Path(settings.CONSUMPTION_DIR).resolve()
        path = root / data["path"]
        # The deployment has one shared data directory, including across web workers.
        with FileLock(settings.DATA_DIR / "api-consumer.lock"):
            previous = PaperlessTask.objects.filter(task_id=task_id).first()
            if previous is not None:
                if previous.input_data.get("api_consumer_key") != submission_key:
                    return Response(
                        {"detail": "Request ID already used for another submission."},
                        status=409,
                    )
                return _receipt(previous)
            if not path.resolve().is_relative_to(root):
                raise ValidationError(
                    {"path": "The file must remain inside the consumption directory."},
                )
            if not settings.CONSUMER_RECURSIVE and path.parent != root:
                return Response({"status": "ignored", "task_id": None})
            file_filter = FileFilter(
                supported_extensions=frozenset(get_supported_file_extensions()),
                ignore_patterns=settings.CONSUMER_IGNORE_PATTERNS,
                ignore_dirs=settings.CONSUMER_IGNORE_DIRS,
            )
            if not file_filter.matches(str(path)):
                return Response({"status": "ignored", "task_id": None})
            try:
                stat = path.stat()
                if not path.is_file():
                    return Response({"status": "ignored", "task_id": None})
            except FileNotFoundError:
                return Response({"status": "missing", "task_id": None})
            if (stat.st_size, stat.st_mtime_ns) != (data["size"], data["mtime_ns"]):
                return Response({"status": "changed", "task_id": None})
            _consume_file(
                path,
                root,
                subdirs_as_tags=settings.CONSUMER_SUBDIRS_AS_TAGS,
                task_id=task_id,
                submission_key=submission_key,
            )
            task = PaperlessTask.objects.filter(task_id=task_id).first()
            if task is None:
                return Response(
                    {"detail": "File could not be queued; retry this request."},
                    status=503,
                )
            # shortcut: a crash during publication needs manual review, not automatic republication.
            return _receipt(task)
