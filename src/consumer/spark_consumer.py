"""PySpark Structured Streaming consumer for Kafka.

Reads events from Kafka topics and writes raw data to
Delta Lake Bronze layer with exactly-once semantics via checkpointing.
"""

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import MapType, StringType, StructField, StructType

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SparkStreamingConsumer:
    """PySpark Structured Streaming consumer for Kafka.

    Reads from Kafka topics, parses JSON payloads, and writes
    raw events to Delta Lake Bronze layer.

    Args:
        config_path: Path to the YAML configuration file.
    """

    CLICKSTREAM_SCHEMA = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("product_id", StringType(), True),
            StructField("session_id", StringType(), False),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )

    def __init__(self, config_path: str = "configs/config.yaml") -> None:
        self.config = load_config(config_path)
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession with Delta Lake support.

        Returns:
            Configured SparkSession.
        """
        builder = (
            SparkSession.builder.appName(self.config["spark"]["app_name"])
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        return builder.getOrCreate()

    def read_kafka_stream(self, topic: str) -> DataFrame:
        """Create a streaming DataFrame from a Kafka topic.

        Args:
            topic: Kafka topic to subscribe to.

        Returns:
            Streaming DataFrame with raw Kafka records.
        """
        kafka_config = self.config["kafka"]
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def process_bronze(self, kafka_df: DataFrame) -> DataFrame:
        """Parse JSON and prepare data for the Bronze layer.

        Extracts the raw JSON payload from Kafka records and adds
        ingestion metadata columns.

        Args:
            kafka_df: Raw Kafka DataFrame with key/value/topic/partition/offset.

        Returns:
            Parsed DataFrame with event fields and Kafka metadata.
        """
        return (
            kafka_df.selectExpr(
                "CAST(key AS STRING)",
                "CAST(value AS STRING)",
                "topic",
                "partition",
                "offset",
                "timestamp as kafka_timestamp",
            )
            .withColumn("parsed", from_json(col("value"), self.CLICKSTREAM_SCHEMA))
            .select(
                col("parsed.*"),
                col("kafka_timestamp"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                current_timestamp().alias("ingestion_timestamp"),
            )
        )

    def write_bronze(
        self, df: DataFrame, output_path: str, checkpoint_path: str
    ) -> Any:
        """Write a streaming DataFrame to Delta Lake Bronze layer.

        Uses checkpointing for exactly-once semantics.

        Args:
            df: Parsed DataFrame to write.
            output_path: Delta Lake table path.
            checkpoint_path: Checkpoint directory for streaming recovery.

        Returns:
            StreamingQuery handle.
        """
        return (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("mergeSchema", "true")
            .start(output_path)
        )

    def run_bronze_pipeline(self) -> Any:
        """Execute the Bronze layer pipeline.

        Returns:
            StreamingQuery handle for the running pipeline.
        """
        kafka_df = self.read_kafka_stream(self.config["kafka"]["topics"]["clickstream"])
        bronze_df = self.process_bronze(kafka_df)

        query = self.write_bronze(
            bronze_df,
            self.config["delta"]["bronze_path"],
            f"{self.config['spark']['checkpoint_location']}/bronze",
        )

        logger.info("Bronze pipeline started")
        return query
