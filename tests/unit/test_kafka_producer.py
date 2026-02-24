"""Tests for the Kafka producer with DLQ support."""

from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.producer.kafka_producer import StreamingProducer


@pytest.fixture()
def config_file(tmp_path):
    """Create a minimal config file for the producer."""
    config = {
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topics": {
                "clickstream": "clickstream-events",
                "cdc": "cdc-events",
                "dlq": "dead-letter-queue",
            },
        },
    }
    path = tmp_path / "config.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return str(path)


@pytest.fixture()
def mock_kafka_producer():
    """Patch KafkaProducer so we never need a real broker."""
    with patch(
        "src.producer.kafka_producer.StreamingProducer._create_producer"
    ) as mock:
        mock_producer = MagicMock()
        future = MagicMock()
        future.add_callback = MagicMock()
        future.add_errback = MagicMock()
        mock_producer.send.return_value = future
        mock.return_value = mock_producer
        yield mock_producer


@pytest.fixture()
def producer(config_file, mock_kafka_producer):
    """Create a StreamingProducer with mocked Kafka."""
    return StreamingProducer(config_file)


def _valid_clickstream_event():
    return {
        "event_id": "e1",
        "user_id": "USER-001",
        "timestamp": "2024-01-01T12:00:00Z",
        "event_type": "page_view",
        "product_id": "PROD-001",
        "session_id": "s1",
        "metadata": {"page_url": "/"},
    }


def _valid_cdc_event():
    return {
        "op": "c",
        "ts_ms": 1704067200000,
        "source": {"table": "users", "db": "ecommerce"},
        "before": None,
        "after": {"user_id": "USER-001", "name": "Alice"},
    }


class TestStreamingProducer:
    """Tests for the StreamingProducer class."""

    def test_send_valid_clickstream_event(self, producer, mock_kafka_producer) -> None:
        result = producer.send_clickstream_event(_valid_clickstream_event())
        assert result is True
        mock_kafka_producer.send.assert_called_once()
        call_args = mock_kafka_producer.send.call_args
        assert call_args[1]["key"] == "USER-001"

    def test_send_invalid_clickstream_to_dlq(
        self, producer, mock_kafka_producer
    ) -> None:
        invalid_event = {"event_id": "e1", "user_id": "u1"}
        result = producer.send_clickstream_event(invalid_event)
        assert result is False
        # Should have sent to DLQ
        calls = mock_kafka_producer.send.call_args_list
        assert len(calls) == 1
        assert calls[0][0][0] == "dead-letter-queue"

    def test_send_valid_cdc_event(self, producer, mock_kafka_producer) -> None:
        result = producer.send_cdc_event(_valid_cdc_event())
        assert result is True
        mock_kafka_producer.send.assert_called_once()

    def test_send_invalid_cdc_to_dlq(self, producer, mock_kafka_producer) -> None:
        invalid_event = {"op": "x", "ts_ms": 123}
        result = producer.send_cdc_event(invalid_event)
        assert result is False

    def test_partition_key_from_user_id(self, producer) -> None:
        event = {"user_id": "USER-123"}
        assert producer._get_partition_key(event) == "USER-123"

    def test_partition_key_default(self, producer) -> None:
        event = {}
        assert producer._get_partition_key(event) == "unknown"

    def test_metrics_tracking(self, producer, mock_kafka_producer) -> None:
        producer.send_clickstream_event(_valid_clickstream_event())
        producer.send_clickstream_event({"bad": "event"})
        metrics = producer.metrics
        assert metrics["dlq"] == 1

    def test_metrics_returns_copy(self, producer) -> None:
        m1 = producer.metrics
        m1["sent"] = 999
        assert producer.metrics["sent"] != 999

    def test_flush(self, producer, mock_kafka_producer) -> None:
        producer.flush()
        mock_kafka_producer.flush.assert_called_once()

    def test_close(self, producer, mock_kafka_producer) -> None:
        producer.close()
        mock_kafka_producer.close.assert_called_once()

    def test_cdc_partition_key_from_after(self, producer, mock_kafka_producer) -> None:
        event = _valid_cdc_event()
        producer.send_cdc_event(event)
        call_args = mock_kafka_producer.send.call_args
        assert call_args[1]["key"] == "USER-001"

    def test_cdc_partition_key_from_before_on_delete(
        self, producer, mock_kafka_producer
    ) -> None:
        event = {
            "op": "d",
            "ts_ms": 1704067200000,
            "source": {"table": "users", "db": "ecommerce"},
            "before": {"user_id": "USER-002", "name": "Bob"},
            "after": None,
        }
        producer.send_cdc_event(event)
        call_args = mock_kafka_producer.send.call_args
        assert call_args[1]["key"] == "USER-002"

    def test_dlq_send_failure_logged(self, producer, mock_kafka_producer) -> None:
        mock_kafka_producer.send.side_effect = Exception("Kafka down")
        # Should not raise
        producer.send_to_dlq({"bad": "event"}, "error", "test-topic")

    def test_on_send_success_increments_metric(self, producer) -> None:
        mock_metadata = MagicMock()
        mock_metadata.topic = "test"
        mock_metadata.partition = 0
        mock_metadata.offset = 1
        producer._on_send_success(mock_metadata)
        assert producer._metrics["sent"] == 1

    def test_on_send_error_increments_metric(self, producer) -> None:
        producer._on_send_error(Exception("test"))
        assert producer._metrics["failed"] == 1
