"""Integration tests for Kafka pipeline (requires running Kafka)."""

import pytest

from src.producer.schemas import CLICKSTREAM_SCHEMA, validate_event


@pytest.mark.integration
class TestKafkaPipeline:
    """Integration tests requiring a live Kafka broker."""

    def test_schema_validation_with_full_event(self) -> None:
        event = {
            "event_id": "e-int-001",
            "user_id": "u-int-001",
            "timestamp": "2024-06-15T12:00:00.000000Z",
            "event_type": "page_view",
            "product_id": "p-001",
            "session_id": "s-int-001",
            "metadata": {"page_url": "/", "device_type": "desktop"},
        }
        is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)
        assert is_valid, f"Schema validation failed: {error}"
