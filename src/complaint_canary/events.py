"""Operational events. Cloud Monitoring alert policies are built on these."""

from __future__ import annotations

import json
import logging
from typing import Any

from complaint_canary.config import Settings

LOG_NAME = "complaint-canary"
logger = logging.getLogger(LOG_NAME)

SEVERITY = {
    "run_failure": "ERROR",
    "snapshot_rejected": "ERROR",
    "snapshot_refused": "ERROR",
    "dq_warning": "WARNING",
    "backup_failed": "WARNING",
    "snapshot_stale": "ERROR",
    "heartbeat": "INFO",
    "snapshot_published": "INFO",
    "snapshot_skipped": "INFO",
}


def emit_event(settings: Settings, event: str, **fields: Any) -> dict[str, Any]:
    """Record a structured event locally and, in the cloud, in Cloud Logging."""
    payload = {"event": event, "env": settings.env, **{k: _plain(v) for k, v in fields.items()}}
    severity = SEVERITY.get(event, "INFO")
    logger.log(logging.getLevelName(severity), "CANARY_EVENT %s", json.dumps(payload, sort_keys=True))
    if settings.event_sink == "cloud_logging":
        from google.cloud import logging as cloud_logging
        from google.cloud.logging_v2.resource import Resource

        # Pinned to the "global" resource: on a VM the client would otherwise label
        # entries as gce_instance, and the alert policies filter on the resource type.
        client = cloud_logging.Client(project=settings.gcp_project or None)
        client.logger(LOG_NAME).log_struct(payload, severity=severity, resource=Resource(type="global", labels={}))
    return payload


def _plain(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
