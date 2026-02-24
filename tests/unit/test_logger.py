"""Tests for structured logging utilities."""

import json
import logging

from src.utils.logger import JsonFormatter, get_logger


class TestJsonFormatter:
    """Tests for the JSON log formatter."""

    def test_formats_as_json(self) -> None:
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "test message"
        assert parsed["logger"] == "test"
        assert "timestamp" in parsed

    def test_includes_exception_info(self) -> None:
        formatter = JsonFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys

            record = logging.LogRecord(
                name="test",
                level=logging.ERROR,
                pathname="test.py",
                lineno=1,
                msg="error occurred",
                args=(),
                exc_info=sys.exc_info(),
            )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]


class TestGetLogger:
    """Tests for the get_logger factory function."""

    def test_returns_logger(self) -> None:
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_logger_has_json_handler(self) -> None:
        logger = get_logger("test_json_handler")
        assert len(logger.handlers) > 0
        assert isinstance(logger.handlers[0].formatter, JsonFormatter)

    def test_logger_level(self) -> None:
        logger = get_logger("test_level", level=logging.DEBUG)
        assert logger.level == logging.DEBUG

    def test_no_duplicate_handlers(self) -> None:
        logger_name = "test_no_dup"
        logger1 = get_logger(logger_name)
        handler_count = len(logger1.handlers)
        logger2 = get_logger(logger_name)
        assert len(logger2.handlers) == handler_count
