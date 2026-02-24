"""Tests for the backfill manager."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.consumer.backfill import BackfillManager


@pytest.fixture()
def config_file(tmp_path):
    """Write a minimal config.yaml and return its path."""
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
  events_per_second: 100
  batch_size: 100
  num_users: 1000
spark:
  app_name: "TestPipeline"
  checkpoint_location: "/tmp/checkpoints"
delta:
  bronze_path: "/data/delta/bronze"
  silver_path: "/data/delta/silver"
  gold_path: "/data/delta/gold"
quality:
  db_path: "data/quality_metrics.db"
  thresholds:
    bronze_schema: 0.99
logging:
  level: "INFO"
  format: "json"
"""
    )
    return str(cfg)


class TestGetStartingOffsets:
    """Tests for offset/timestamp resolution."""

    def test_returns_earliest_when_no_args(self, config_file) -> None:
        mgr = BackfillManager(MagicMock(), config_path=config_file)
        assert mgr.get_starting_offsets("topic") == "earliest"

    def test_returns_offset_json(self, config_file) -> None:
        mgr = BackfillManager(MagicMock(), config_path=config_file)
        result = mgr.get_starting_offsets("my-topic", offset=42)
        parsed = json.loads(result)
        assert parsed == {"my-topic": {"0": 42}}

    def test_returns_timestamp_json(self, config_file) -> None:
        mgr = BackfillManager(MagicMock(), config_path=config_file)
        ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        result = mgr.get_starting_offsets("t", timestamp=ts)
        parsed = json.loads(result)
        assert parsed["t"]["0"] == int(ts.timestamp() * 1000)

    def test_offset_takes_precedence(self, config_file) -> None:
        mgr = BackfillManager(MagicMock(), config_path=config_file)
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = mgr.get_starting_offsets("t", offset=10, timestamp=ts)
        parsed = json.loads(result)
        assert parsed == {"t": {"0": 10}}


class TestGetEndingOffsets:
    """Tests for ending offset resolution."""

    def test_returns_latest_when_none(self, config_file) -> None:
        mgr = BackfillManager(MagicMock(), config_path=config_file)
        assert mgr.get_ending_offsets("topic") == "latest"

    def test_returns_offset_json(self, config_file) -> None:
        mgr = BackfillManager(MagicMock(), config_path=config_file)
        result = mgr.get_ending_offsets("t", end_offset=100)
        parsed = json.loads(result)
        assert parsed == {"t": {"0": 100}}


class TestBackfillPaths:
    """Tests for full_backfill path computation."""

    def test_full_backfill_returns_paths(self, config_file) -> None:
        spark_mock = MagicMock()
        mgr = BackfillManager(spark_mock, config_path=config_file)

        with (
            patch.object(mgr, "backfill_bronze") as bronze_mock,
            patch.object(mgr, "backfill_silver") as silver_mock,
            patch.object(mgr, "backfill_gold") as gold_mock,
        ):
            paths = mgr.full_backfill(start_offset=0)

        assert "bronze" in paths
        assert "silver" in paths
        assert "gold" in paths
        bronze_mock.assert_called_once()
        silver_mock.assert_called_once()
        gold_mock.assert_called_once()

    def test_full_backfill_derives_from_config(self, config_file) -> None:
        mgr = BackfillManager(MagicMock(), config_path=config_file)
        with (
            patch.object(mgr, "backfill_bronze"),
            patch.object(mgr, "backfill_silver"),
            patch.object(mgr, "backfill_gold"),
        ):
            paths = mgr.full_backfill()
        assert "/backfill/" in paths["bronze"]
        assert "/backfill/" in paths["silver"]
        assert "/backfill/" in paths["gold"]


class TestReadKafkaBatch:
    """Tests for the Kafka batch reader."""

    def test_passes_options_to_spark(self, config_file) -> None:
        mock_spark = MagicMock()
        mgr = BackfillManager(mock_spark, config_path=config_file)
        mgr.read_kafka_batch("topic-a", "earliest", "latest")

        read = mock_spark.read.format.return_value
        read.option.return_value.option.return_value.option.return_value.option.return_value.load.assert_called_once()


class TestBackfillBronze:
    """Tests for the bronze backfill method."""

    def test_writes_delta_to_output_path(self, config_file) -> None:
        mock_spark = MagicMock()
        mgr = BackfillManager(mock_spark, config_path=config_file)

        with patch.object(mgr, "read_kafka_batch") as read_mock:
            mock_df = MagicMock()
            read_mock.return_value = mock_df
            mgr.backfill_bronze("/tmp/bronze_out", start_offset=0)

        mock_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "/tmp/bronze_out"
        )

    def test_uses_timestamp_start(self, config_file) -> None:
        mock_spark = MagicMock()
        mgr = BackfillManager(mock_spark, config_path=config_file)
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with patch.object(mgr, "read_kafka_batch") as read_mock:
            read_mock.return_value = MagicMock()
            mgr.backfill_bronze("/tmp/out", start_timestamp=ts)

        call_args = read_mock.call_args
        assert "clickstream-events" in call_args[0][0]


class TestBackfillSilver:
    """Tests for the silver backfill method."""

    def test_reads_and_writes_delta(self, config_file) -> None:
        mock_spark = MagicMock()
        mgr = BackfillManager(mock_spark, config_path=config_file)

        with patch("src.consumer.backfill.process_silver") as mock_silver:
            mock_silver.return_value = MagicMock()
            mgr.backfill_silver("/tmp/bronze", "/tmp/silver")

        mock_spark.read.format.return_value.load.assert_called_once_with("/tmp/bronze")
        mock_silver.return_value.write.format.return_value.mode.return_value.save.assert_called_once_with(
            "/tmp/silver"
        )


class TestBackfillGold:
    """Tests for the gold backfill method."""

    def test_writes_three_aggregation_tables(self, config_file) -> None:
        mock_spark = MagicMock()
        mgr = BackfillManager(mock_spark, config_path=config_file)

        with patch("src.consumer.aggregations.GoldAggregations") as mock_aggs_cls:
            mock_aggs = MagicMock()
            mock_aggs_cls.return_value = mock_aggs
            mgr.backfill_gold("/tmp/silver", "/tmp/gold")

        mock_aggs.events_per_minute.assert_called_once()
        mock_aggs.conversion_rate.assert_called_once()
        mock_aggs.popular_products.assert_called_once()
