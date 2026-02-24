"""Silver layer data cleaning and enrichment transformations.

Applies deduplication, string normalization, timestamp parsing,
metadata flattening, and business-relevant derived columns.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    row_number,
    to_timestamp,
    trim,
    when,
)
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window


class SilverTransformations:
    """Data cleaning and enrichment transformations for the Silver layer."""

    @staticmethod
    def clean_strings(df: DataFrame, columns: list[str]) -> DataFrame:
        """Trim whitespace and lowercase string columns.

        Args:
            df: Input DataFrame.
            columns: List of column names to clean.

        Returns:
            DataFrame with cleaned string columns.
        """
        for col_name in columns:
            df = df.withColumn(col_name, trim(lower(col(col_name))))
        return df

    @staticmethod
    def deduplicate_events(df: DataFrame) -> DataFrame:
        """Remove duplicate events based on event_id, keeping earliest.

        Args:
            df: Input DataFrame with potential duplicates.

        Returns:
            DataFrame with duplicates removed.
        """
        window = Window.partitionBy("event_id").orderBy("kafka_timestamp")
        return (
            df.withColumn("row_num", row_number().over(window))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )

    @staticmethod
    def parse_timestamp(df: DataFrame) -> DataFrame:
        """Convert ISO timestamp string to proper timestamp type.

        Args:
            df: Input DataFrame with string timestamp column.

        Returns:
            DataFrame with event_timestamp column replacing timestamp.
        """
        return df.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        ).drop("timestamp")

    @staticmethod
    def extract_metadata(df: DataFrame) -> DataFrame:
        """Flatten metadata map into individual columns.

        Args:
            df: Input DataFrame with nested metadata map.

        Returns:
            DataFrame with metadata fields as top-level columns.
        """
        return (
            df.withColumn("page_url", col("metadata").getItem("page_url"))
            .withColumn("device_type", col("metadata").getItem("device_type"))
            .withColumn("referrer", col("metadata").getItem("referrer"))
            .withColumn("search_query", col("metadata").getItem("search_query"))
            .withColumn(
                "cart_value",
                col("metadata").getItem("cart_value").cast(DoubleType()),
            )
            .withColumn(
                "quantity",
                col("metadata").getItem("quantity").cast(IntegerType()),
            )
            .drop("metadata")
        )

    @staticmethod
    def add_derived_columns(df: DataFrame) -> DataFrame:
        """Add business-relevant derived columns.

        Args:
            df: Input DataFrame with event_type column.

        Returns:
            DataFrame with is_conversion and is_cart_event boolean columns.
        """
        return df.withColumn(
            "is_conversion",
            when(col("event_type") == "purchase", True).otherwise(False),
        ).withColumn(
            "is_cart_event",
            when(col("event_type").isin(["add_to_cart", "purchase"]), True).otherwise(
                False
            ),
        )

    @staticmethod
    def validate_required_fields(df: DataFrame) -> DataFrame:
        """Filter out records with null required fields.

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with only valid records.
        """
        required = ["event_id", "user_id", "session_id", "event_type"]
        condition = col(required[0]).isNotNull()
        for field_name in required[1:]:
            condition = condition & col(field_name).isNotNull()
        return df.filter(condition)


def process_silver(bronze_df: DataFrame) -> DataFrame:
    """Apply all Silver layer transformations to a Bronze DataFrame.

    Args:
        bronze_df: Raw Bronze layer DataFrame.

    Returns:
        Cleaned and enriched Silver layer DataFrame.
    """
    transforms = SilverTransformations()

    return (
        bronze_df.transform(transforms.validate_required_fields)
        .transform(transforms.deduplicate_events)
        .transform(transforms.parse_timestamp)
        .transform(transforms.extract_metadata)
        .transform(lambda df: transforms.clean_strings(df, ["user_id", "session_id"]))
        .transform(transforms.add_derived_columns)
    )
