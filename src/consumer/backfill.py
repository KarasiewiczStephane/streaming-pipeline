"""Historical data reprocessing (backfill) manager.

Replays events from Kafka at a specific offset or timestamp and
reprocesses them through the Bronze, Silver, and Gold layers in
batch mode for data recovery and recomputation scenarios.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.consumer.transformations import process_silver
from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BackfillManager:
    """Manages historical data reprocessing from Kafka.

    Reads Kafka topics in batch mode and replays events through the
    medallion architecture layers.

    Args:
        spark: Active SparkSession.
        config_path: Path to the YAML configuration file.
    """

    def __init__(
        self,
        spark: SparkSession,
        config_path: str = "configs/config.yaml",
    ) -> None:
        self.spark = spark
        self.config = load_config(config_path)

    def get_starting_offsets(
        self,
        topic: str,
        offset: int | None = None,
        timestamp: datetime | None = None,
    ) -> str:
        """Build Kafka starting offsets JSON string.

        Args:
            topic: Kafka topic name.
            offset: Specific partition-0 offset to start from.
            timestamp: Timestamp to seek to (Kafka resolves nearest offset).

        Returns:
            JSON string suitable for Spark's ``startingOffsets`` option.
        """
        if offset is not None:
            return json.dumps({topic: {"0": offset}})
        if timestamp is not None:
            ts_ms = int(timestamp.timestamp() * 1000)
            return json.dumps({topic: {"0": ts_ms}})
        return "earliest"

    def get_ending_offsets(
        self,
        topic: str,
        end_offset: int | None = None,
    ) -> str:
        """Build Kafka ending offsets JSON string.

        Args:
            topic: Kafka topic name.
            end_offset: Specific partition-0 offset to end at.

        Returns:
            JSON string or ``"latest"``.
        """
        if end_offset is not None:
            return json.dumps({topic: {"0": end_offset}})
        return "latest"

    def read_kafka_batch(
        self,
        topic: str,
        starting_offsets: str,
        ending_offsets: str,
    ) -> DataFrame:
        """Read a batch of records from Kafka.

        Args:
            topic: Kafka topic to read.
            starting_offsets: Starting offsets JSON or ``"earliest"``.
            ending_offsets: Ending offsets JSON or ``"latest"``.

        Returns:
            DataFrame with Kafka records.
        """
        bootstrap = self.config["kafka"]["bootstrap_servers"]
        return (
            self.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("endingOffsets", ending_offsets)
            .load()
        )

    def backfill_bronze(
        self,
        output_path: str,
        start_offset: int | None = None,
        start_timestamp: datetime | None = None,
        end_offset: int | None = None,
    ) -> None:
        """Reprocess Kafka events to the Bronze layer in batch mode.

        Args:
            output_path: Delta Lake path for backfilled Bronze data.
            start_offset: Starting Kafka offset.
            start_timestamp: Starting timestamp (alternative to offset).
            end_offset: Ending Kafka offset.
        """
        topic = self.config["kafka"]["topics"]["clickstream"]
        starting = self.get_starting_offsets(topic, start_offset, start_timestamp)
        ending = self.get_ending_offsets(topic, end_offset)

        logger.info("Starting Bronze backfill from %s to %s", starting, ending)

        kafka_df = self.read_kafka_batch(topic, starting, ending)
        kafka_df.write.format("delta").mode("overwrite").save(output_path)

        logger.info("Bronze backfill complete at %s", output_path)

    def backfill_silver(self, bronze_path: str, output_path: str) -> None:
        """Reprocess Bronze data through Silver transformations.

        Args:
            bronze_path: Path to backfilled Bronze Delta table.
            output_path: Delta Lake path for backfilled Silver data.
        """
        logger.info("Starting Silver backfill from %s", bronze_path)

        bronze_df = self.spark.read.format("delta").load(bronze_path)
        silver_df = process_silver(bronze_df)
        silver_df.write.format("delta").mode("overwrite").save(output_path)

        logger.info("Silver backfill complete at %s", output_path)

    def backfill_gold(self, silver_path: str, output_base_path: str) -> None:
        """Recompute Gold aggregations from Silver data.

        Args:
            silver_path: Path to backfilled Silver Delta table.
            output_base_path: Base path for Gold aggregation tables.
        """
        from src.consumer.aggregations import GoldAggregations

        logger.info("Starting Gold backfill from %s", silver_path)

        silver_df = self.spark.read.format("delta").load(silver_path)
        aggs = GoldAggregations()

        aggs.events_per_minute(silver_df).write.format("delta").mode("overwrite").save(
            f"{output_base_path}/events_per_minute"
        )

        aggs.conversion_rate(silver_df).write.format("delta").mode("overwrite").save(
            f"{output_base_path}/conversion_rate"
        )

        aggs.popular_products(silver_df).write.format("delta").mode("overwrite").save(
            f"{output_base_path}/popular_products"
        )

        logger.info("Gold backfill complete at %s", output_base_path)

    def full_backfill(
        self,
        start_offset: int | None = None,
        start_timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        """Run full pipeline backfill from Kafka through all layers.

        Args:
            start_offset: Starting Kafka offset.
            start_timestamp: Starting timestamp.

        Returns:
            Dictionary with paths for each backfilled layer.
        """
        base = self.config["delta"]["bronze_path"].replace("/bronze", "/backfill")
        paths = {
            "bronze": f"{base}/bronze",
            "silver": f"{base}/silver",
            "gold": f"{base}/gold",
        }

        self.backfill_bronze(paths["bronze"], start_offset, start_timestamp)
        self.backfill_silver(paths["bronze"], paths["silver"])
        self.backfill_gold(paths["silver"], paths["gold"])

        logger.info("Full backfill pipeline complete")
        return paths
