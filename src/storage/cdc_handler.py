"""CDC handler with SCD Type 2 dimension table management.

Applies Debezium-style CDC events to a Delta Lake dimension table,
maintaining full history with effective dates and version tracking.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    when,
)
from pyspark.sql.functions import max as spark_max

from src.utils.logger import get_logger

if TYPE_CHECKING:
    from delta.tables import DeltaTable

logger = get_logger(__name__)


class CDCHandler:
    """Handles CDC events with SCD Type 2 implementation.

    Maintains a slowly-changing dimension table where updates create
    new versioned records and close previous ones with effective dates.

    Args:
        spark: Active SparkSession.
        user_dim_path: Path to the user dimension Delta table.
    """

    def __init__(self, spark: SparkSession, user_dim_path: str) -> None:
        self.spark = spark
        self.user_dim_path = user_dim_path

    def initialize_dimension_table(self, initial_df: DataFrame) -> None:
        """Create the initial dimension table with SCD columns.

        Args:
            initial_df: DataFrame with user profile data.
        """
        (
            initial_df.withColumn("effective_from", current_timestamp())
            .withColumn("effective_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
            .withColumn("version", lit(1))
            .write.format("delta")
            .mode("overwrite")
            .save(self.user_dim_path)
        )
        logger.info("Dimension table initialized at %s", self.user_dim_path)

    def apply_cdc_batch(self, cdc_df: DataFrame) -> None:
        """Apply a batch of CDC changes with SCD Type 2 logic.

        Processes creates, updates, and deletes in a single batch.

        Args:
            cdc_df: DataFrame with op, before, and after columns.

        Raises:
            ValueError: If the dimension table does not exist.
        """
        from delta.tables import DeltaTable

        if not DeltaTable.isDeltaTable(self.spark, self.user_dim_path):
            raise ValueError(f"Dimension table not found at {self.user_dim_path}")

        dim_table = DeltaTable.forPath(self.spark, self.user_dim_path)

        self._process_updates(cdc_df, dim_table)
        self._process_creates(cdc_df)
        self._process_deletes(cdc_df, dim_table)

    def _process_updates(self, cdc_df: DataFrame, dim_table: "DeltaTable") -> None:
        """Close old records and insert new versions for updates.

        Args:
            cdc_df: CDC events DataFrame.
            dim_table: Target DeltaTable.
        """
        updates_df = cdc_df.filter(col("op") == "u").select(
            col("after.user_id").alias("user_id"),
            col("after.name").alias("name"),
            col("after.email").alias("email"),
        )

        if updates_df.head() is None:
            return

        # Close current records
        dim_table.alias("dim").merge(
            updates_df.alias("updates"),
            "dim.user_id = updates.user_id AND dim.is_current = true",
        ).whenMatchedUpdate(
            set={
                "effective_to": current_timestamp(),
                "is_current": lit(False),
            }
        ).execute()

        # Get max versions
        max_versions = (
            self.spark.read.format("delta")
            .load(self.user_dim_path)
            .groupBy("user_id")
            .agg(spark_max("version").alias("max_version"))
        )

        new_records = (
            updates_df.join(max_versions, "user_id", "left")
            .withColumn(
                "version",
                when(col("max_version").isNull(), 1).otherwise(col("max_version") + 1),
            )
            .drop("max_version")
            .withColumn("effective_from", current_timestamp())
            .withColumn("effective_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
        )

        new_records.write.format("delta").mode("append").save(self.user_dim_path)
        logger.info("Processed %d updates", updates_df.count())

    def _process_creates(self, cdc_df: DataFrame) -> None:
        """Insert new records for create operations.

        Args:
            cdc_df: CDC events DataFrame.
        """
        creates_df = cdc_df.filter(col("op") == "c").select(
            col("after.user_id").alias("user_id"),
            col("after.name").alias("name"),
            col("after.email").alias("email"),
        )

        if creates_df.head() is None:
            return

        creates_with_scd = (
            creates_df.withColumn("effective_from", current_timestamp())
            .withColumn("effective_to", lit(None).cast("timestamp"))
            .withColumn("is_current", lit(True))
            .withColumn("version", lit(1))
        )

        creates_with_scd.write.format("delta").mode("append").save(self.user_dim_path)
        logger.info("Processed %d creates", creates_df.count())

    def _process_deletes(self, cdc_df: DataFrame, dim_table: "DeltaTable") -> None:
        """Mark records as non-current for delete operations.

        Args:
            cdc_df: CDC events DataFrame.
            dim_table: Target DeltaTable.
        """
        deletes_df = cdc_df.filter(col("op") == "d").select(
            col("before.user_id").alias("user_id")
        )

        if deletes_df.head() is None:
            return

        dim_table.alias("dim").merge(
            deletes_df.alias("deletes"),
            "dim.user_id = deletes.user_id AND dim.is_current = true",
        ).whenMatchedUpdate(
            set={
                "effective_to": current_timestamp(),
                "is_current": lit(False),
            }
        ).execute()
        logger.info("Processed %d deletes", deletes_df.count())
