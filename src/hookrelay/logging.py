"""Small structured-logging foundation using the Python standard library."""

import json
import logging
from datetime import UTC, datetime
from typing import Any

from hookrelay.config import Settings
from hookrelay.observability import ServiceRole, current_correlation_id, current_trace_fields

_STANDARD_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)


class JsonFormatter(logging.Formatter):
    """Serialize log records as one JSON object per line."""

    def __init__(self, settings: Settings, service_role: ServiceRole) -> None:
        super().__init__()
        self._service = settings.service_name
        self._version = settings.version
        self._environment = settings.environment
        self._service_role = service_role

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            "service": self._service,
            "version": self._version,
            "environment": self._environment,
            "service_role": self._service_role,
        }
        correlation_id = current_correlation_id()
        if correlation_id is not None:
            payload["correlation_id"] = correlation_id
        payload.update(current_trace_fields())
        for key, value in record.__dict__.items():
            if (
                key not in _STANDARD_LOG_RECORD_FIELDS
                and not key.startswith("_")
                and key not in payload
            ):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(
    settings: Settings,
    *,
    service_role: ServiceRole = "api",
) -> logging.Logger:
    """Configure and return HookRelay's process logger."""

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(settings, service_role))

    logger = logging.getLogger("hookrelay")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(settings.log_level)
    logger.propagate = False
    return logger
