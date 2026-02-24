"""Tests for the main entry point."""

from unittest.mock import MagicMock, patch

import pytest

from src.main import parse_args


class TestParseArgs:
    """Tests for CLI argument parsing."""

    def test_default_mode_is_all(self) -> None:
        with patch("sys.argv", ["main"]):
            args = parse_args()
        assert args.mode == "all"

    def test_producer_mode(self) -> None:
        with patch("sys.argv", ["main", "--mode", "producer"]):
            args = parse_args()
        assert args.mode == "producer"

    def test_consumer_mode(self) -> None:
        with patch("sys.argv", ["main", "--mode", "consumer"]):
            args = parse_args()
        assert args.mode == "consumer"

    def test_custom_config_path(self) -> None:
        with patch("sys.argv", ["main", "--config", "/custom/config.yaml"]):
            args = parse_args()
        assert args.config == "/custom/config.yaml"

    def test_default_config_path(self) -> None:
        with patch("sys.argv", ["main"]):
            args = parse_args()
        assert args.config == "configs/config.yaml"

    def test_invalid_mode_exits(self) -> None:
        with (
            patch("sys.argv", ["main", "--mode", "invalid"]),
            pytest.raises(SystemExit),
        ):
            parse_args()


class TestMain:
    """Tests for the main function."""

    def test_producer_mode_starts_generator(self, tmp_path) -> None:
        cfg = tmp_path / "config.yaml"
        cfg.write_text(
            """\
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    clickstream: "clickstream-events"
    cdc: "cdc-events"
    dlq: "dead-letter-queue"
producer:
  events_per_second: 10
  batch_size: 10
  num_users: 5
spark:
  app_name: "Test"
  checkpoint_location: "/tmp/ckpt"
delta:
  bronze_path: "/tmp/bronze"
  silver_path: "/tmp/silver"
  gold_path: "/tmp/gold"
quality:
  db_path: "data/quality_metrics.db"
  thresholds:
    bronze_schema: 0.99
logging:
  level: "INFO"
  format: "json"
"""
        )

        mock_producer = MagicMock()
        mock_generator = MagicMock()
        mock_generator.stream_events.return_value = iter([{"event_id": "e1"}])
        mock_producer.send_clickstream_event.side_effect = KeyboardInterrupt

        with (
            patch("sys.argv", ["main", "--mode", "producer", "--config", str(cfg)]),
            patch(
                "src.producer.event_generator.EventGenerator",
                return_value=mock_generator,
            ),
            patch(
                "src.producer.kafka_producer.StreamingProducer",
                return_value=mock_producer,
            ),
        ):
            from src.main import main

            main()

        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()
