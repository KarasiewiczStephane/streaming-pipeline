"""Structured JSON logging for the streaming pipeline.

Provides a consistent logging interface with JSON-formatted output
for structured log aggregation and analysis.
"""

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Formats log records as JSON for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string.

        Args:
            record: The log record to format.

        Returns:
            JSON-encoded string with timestamp, level, logger name,
            message, and optional exception info.
        """
        log_entry = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        return json.dumps(log_entry)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create a logger with JSON-formatted output.

    Args:
        name: Name for the logger, typically ``__name__``.
        level: Logging level. Defaults to ``logging.INFO``.

    Returns:
        Configured logger instance with JSON output to stderr.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)

    logger.setLevel(level)
    return logger
