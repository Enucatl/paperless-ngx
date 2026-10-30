"""Publish configured periodic jobs for an external scheduler."""

from zoneinfo import ZoneInfo

from django.conf import settings
from django.utils import timezone
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import extend_schema
from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import BasePermission
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from paperless.celery import app


class SuperuserPermission(BasePermission):
    """Restrict service scheduling to active superusers."""

    def has_permission(self, request: Request, view: APIView) -> bool:
        return request.user.is_active and request.user.is_superuser


class ScheduledJobSerializer(serializers.Serializer):
    """Accept only the name of an enabled server-owned schedule entry."""

    name = serializers.CharField()

    def validate(self, attrs: dict) -> dict:
        if set(self.initial_data) != {"name"}:
            raise ValidationError("Only the schedule name may be supplied.")
        return attrs


def _cron_field(values: set[int], all_values: range) -> str:
    """Render an expanded Celery cron field without changing its matches."""
    return "*" if values == set(all_values) else ",".join(map(str, sorted(values)))


class SchedulerAPIView(APIView):
    """Expose enabled schedules and publish them with their existing options."""

    permission_classes = (SuperuserPermission,)

    @extend_schema(responses=OpenApiTypes.OBJECT)
    def get(self, request: Request) -> Response:
        """Return effective cron times without exposing the secret email offset seed."""
        jobs = []
        for name, entry in settings.CELERY_BEAT_SCHEDULE.items():
            schedule = entry["schedule"]
            jobs.append(
                {
                    "name": name,
                    "cron": " ".join(
                        (
                            _cron_field(schedule.minute, range(60)),
                            _cron_field(schedule.hour, range(24)),
                            "*",
                            _cron_field(schedule.month_of_year, range(1, 13)),
                            _cron_field(schedule.day_of_week, range(7)),
                        ),
                    ),
                },
            )
        return Response({"timezone": settings.CELERY_TIMEZONE, "jobs": jobs})

    @extend_schema(request=ScheduledJobSerializer, responses=OpenApiTypes.OBJECT)
    def post(self, request: Request) -> Response:
        """Dispatch an enabled job only within its configured calendar minute."""
        serializer = ScheduledJobSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        name = serializer.validated_data["name"]
        entry = settings.CELERY_BEAT_SCHEDULE.get(name)
        if entry is None:
            raise ValidationError({"name": "Unknown or disabled schedule."})
        schedule = entry["schedule"]
        now = timezone.localtime(timezone.now(), ZoneInfo(settings.CELERY_TIMEZONE))
        # Celery combines day-of-month AND weekday; Supercronic uses OR.
        if not (
            now.minute in schedule.minute
            and now.hour in schedule.hour
            and now.day in schedule.day_of_month
            and now.month in schedule.month_of_year
            and now.isoweekday() % 7 in schedule.day_of_week
        ):
            return Response({"task_id": None, "skipped": True})
        result = app.send_task(
            entry["task"],
            args=entry.get("args", ()),
            kwargs=entry.get("kwargs", {}),
            **entry.get("options", {}),
        )
        return Response({"task_id": result.id})
