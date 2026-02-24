"""Kafka producer with schema validation and dead letter queue support.

Validates events before publishing and routes invalid events to a
dead letter queue (DLQ) for later inspection and reprocessing.
"""

import json
from datetime import datetime, timezone
from typing import Any

from src.producer.schemas import (
    CDC_SCHEMA,
    CLICKSTREAM_SCHEMA,
    validate_event,
)
from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StreamingProducer:
    """Kafka producer with schema validation and DLQ support.

    Validates events against their JSON schemas before sending to Kafka.
    Invalid events are routed to the dead letter queue topic with error
    context for debugging.

    Args:
        config_path: Path to YAML configuration file.
    """

    def __init__(self, config_path: str = "configs/config.yaml") -> None:
        self.config = load_config(config_path)
        kafka_config = self.config["kafka"]

        self._producer = self._create_producer(kafka_config["bootstrap_servers"])

        self.clickstream_topic = kafka_config["topics"]["clickstream"]
        self.cdc_topic = kafka_config["topics"]["cdc"]
        self.dlq_topic = kafka_config["topics"]["dlq"]

        self._metrics: dict[str, int] = {
            "sent": 0,
            "failed": 0,
            "dlq": 0,
        }

    def _create_producer(self, bootstrap_servers: str) -> Any:
        """Create and return a KafkaProducer instance.

        Args:
            bootstrap_servers: Kafka broker connection string.

        Returns:
            Configured KafkaProducer instance.
        """
        from kafka import KafkaProducer

        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            retry_backoff_ms=100,
        )

    def _get_partition_key(self, event: dict[str, Any]) -> str:
        """Extract partition key from an event for ordering guarantees.

        Args:
            event: Event dictionary.

        Returns:
            User ID string to use as partition key.
        """
        return event.get("user_id", "unknown")

    def _on_send_success(self, record_metadata: Any) -> None:
        """Callback for successful sends.

        Args:
            record_metadata: Kafka record metadata from the broker.
        """
        self._metrics["sent"] += 1
        logger.debug(
            "Sent to %s:%s:%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def _on_send_error(self, exc: Exception) -> None:
        """Callback for failed sends.

        Args:
            exc: Exception that caused the send failure.
        """
        self._metrics["failed"] += 1
        logger.error("Send failed: %s", exc)

    def send_to_dlq(
        self, event: dict[str, Any], error: str, original_topic: str
    ) -> None:
        """Send a malformed event to the dead letter queue.

        Args:
            event: The original event that failed validation.
            error: Validation error message.
            original_topic: Topic the event was intended for.
        """
        dlq_event = {
            "original_event": event,
            "error": error,
            "original_topic": original_topic,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            self._producer.send(self.dlq_topic, value=dlq_event)
            self._metrics["dlq"] += 1
            logger.warning("Event sent to DLQ: %s", error)
        except Exception as e:
            logger.error("Failed to send to DLQ: %s", e)

    def send_clickstream_event(self, event: dict[str, Any]) -> bool:
        """Validate and send a clickstream event to Kafka.

        Args:
            event: Clickstream event dictionary.

        Returns:
            True if the event was sent, False if it was routed to DLQ.
        """
        is_valid, error = validate_event(event, CLICKSTREAM_SCHEMA)

        if not is_valid:
            self.send_to_dlq(event, error, self.clickstream_topic)
            return False

        key = self._get_partition_key(event)
        future = self._producer.send(self.clickstream_topic, key=key, value=event)
        future.add_callback(self._on_send_success)
        future.add_errback(self._on_send_error)
        return True

    def send_cdc_event(self, event: dict[str, Any]) -> bool:
        """Validate and send a CDC event to Kafka.

        Args:
            event: CDC event dictionary.

        Returns:
            True if the event was sent, False if it was routed to DLQ.
        """
        is_valid, error = validate_event(event, CDC_SCHEMA)

        if not is_valid:
            self.send_to_dlq(event, error, self.cdc_topic)
            return False

        user_id = (event.get("after") or event.get("before") or {}).get(
            "user_id", "unknown"
        )
        future = self._producer.send(self.cdc_topic, key=user_id, value=event)
        future.add_callback(self._on_send_success)
        future.add_errback(self._on_send_error)
        return True

    def flush(self) -> None:
        """Flush all buffered messages to Kafka."""
        self._producer.flush()

    def close(self) -> None:
        """Close the producer connection."""
        self._producer.close()

    @property
    def metrics(self) -> dict[str, int]:
        """Return a copy of the current producer metrics.

        Returns:
            Dictionary with sent, failed, and dlq counts.
        """
        return self._metrics.copy()
