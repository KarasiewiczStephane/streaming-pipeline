"""Tests for the PySpark streaming consumer."""

import json

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.consumer.spark_consumer import SparkStreamingConsumer


class TestProcessBronze:
    """Tests for Bronze layer processing logic."""

    def test_process_bronze_extracts_fields(self, spark: SparkSession) -> None:
        """Verify process_bronze parses JSON and adds metadata columns."""
        event = {
            "event_id": "e1",
            "user_id": "u1",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_type": "page_view",
            "product_id": "p1",
            "session_id": "s1",
            "metadata": {"page_url": "/"},
        }
        kafka_schema = StructType(
            [
                StructField("key", StringType()),
                StructField("value", StringType()),
                StructField("topic", StringType()),
                StructField("partition", LongType()),
                StructField("offset", LongType()),
                StructField("timestamp", TimestampType()),
            ]
        )
        from datetime import datetime

        row = ("u1", json.dumps(event), "test-topic", 0, 1, datetime(2024, 1, 1))
        kafka_df = spark.createDataFrame([row], schema=kafka_schema)

        consumer = SparkStreamingConsumer.__new__(SparkStreamingConsumer)
        consumer.config = {
            "kafka": {"bootstrap_servers": "localhost:9092", "topics": {}},
            "spark": {"app_name": "test", "checkpoint_location": "/tmp"},
            "delta": {"bronze_path": "/tmp/bronze"},
        }
        consumer.spark = spark

        bronze_df = consumer.process_bronze(kafka_df)
        result = bronze_df.collect()

        assert len(result) == 1
        row = result[0]
        assert row["event_id"] == "e1"
        assert row["user_id"] == "u1"
        assert row["event_type"] == "page_view"
        assert row["kafka_partition"] == 0
        assert row["kafka_offset"] == 1
        assert row["ingestion_timestamp"] is not None

    def test_process_bronze_handles_null_product_id(self, spark: SparkSession) -> None:
        event = {
            "event_id": "e2",
            "user_id": "u2",
            "timestamp": "2024-01-01T12:00:00Z",
            "event_type": "search",
            "product_id": None,
            "session_id": "s2",
            "metadata": {"page_url": "/search"},
        }
        kafka_schema = StructType(
            [
                StructField("key", StringType()),
                StructField("value", StringType()),
                StructField("topic", StringType()),
                StructField("partition", LongType()),
                StructField("offset", LongType()),
                StructField("timestamp", TimestampType()),
            ]
        )
        from datetime import datetime

        row = ("u2", json.dumps(event), "test-topic", 0, 2, datetime(2024, 1, 1))
        kafka_df = spark.createDataFrame([row], schema=kafka_schema)

        consumer = SparkStreamingConsumer.__new__(SparkStreamingConsumer)
        consumer.config = {
            "kafka": {"bootstrap_servers": "localhost:9092", "topics": {}},
            "spark": {"app_name": "test", "checkpoint_location": "/tmp"},
            "delta": {"bronze_path": "/tmp/bronze"},
        }
        consumer.spark = spark

        bronze_df = consumer.process_bronze(kafka_df)
        row = bronze_df.collect()[0]
        assert row["product_id"] is None

    def test_clickstream_schema_has_expected_fields(self) -> None:
        field_names = [f.name for f in SparkStreamingConsumer.CLICKSTREAM_SCHEMA.fields]
        expected = [
            "event_id",
            "user_id",
            "timestamp",
            "event_type",
            "product_id",
            "session_id",
            "metadata",
        ]
        assert field_names == expected
