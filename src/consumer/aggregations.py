"""Gold layer windowed aggregations for business metrics.

Provides tumbling and sliding window aggregations for events per minute,
conversion rates, popular products, and hourly active users.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    when,
    window,
)
from pyspark.sql.functions import sum as spark_sum


class GoldAggregations:
    """Windowed aggregations for Gold layer metrics."""

    @staticmethod
    def events_per_minute(df: DataFrame, watermark: str = "2 minutes") -> DataFrame:
        """Count events per minute with a tumbling window.

        Args:
            df: Silver DataFrame with event_timestamp.
            watermark: Late arrival tolerance.

        Returns:
            DataFrame with event counts per minute per event type.
        """
        return (
            df.withWatermark("event_timestamp", watermark)
            .groupBy(
                window(col("event_timestamp"), "1 minute"),
                "event_type",
            )
            .agg(
                count("*").alias("event_count"),
                approx_count_distinct("user_id").alias("unique_users"),
                approx_count_distinct("session_id").alias("unique_sessions"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "event_type",
                "event_count",
                "unique_users",
                "unique_sessions",
            )
        )

    @staticmethod
    def conversion_rate(df: DataFrame, watermark: str = "5 minutes") -> DataFrame:
        """Calculate conversion rate with a 5-minute tumbling window.

        Args:
            df: Silver DataFrame with event_timestamp and event_type.
            watermark: Late arrival tolerance.

        Returns:
            DataFrame with conversion and cart-to-purchase rates.
        """
        return (
            df.withWatermark("event_timestamp", watermark)
            .groupBy(window(col("event_timestamp"), "5 minutes"))
            .agg(
                count("*").alias("total_events"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias(
                    "purchases"
                ),
                spark_sum(
                    when(col("event_type") == "add_to_cart", 1).otherwise(0)
                ).alias("add_to_carts"),
                approx_count_distinct("session_id").alias("sessions"),
            )
            .withColumn(
                "conversion_rate",
                when(col("sessions") > 0, col("purchases") / col("sessions")).otherwise(
                    0
                ),
            )
            .withColumn(
                "cart_to_purchase_rate",
                when(
                    col("add_to_carts") > 0,
                    col("purchases") / col("add_to_carts"),
                ).otherwise(0),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "total_events",
                "purchases",
                "add_to_carts",
                "sessions",
                "conversion_rate",
                "cart_to_purchase_rate",
            )
        )

    @staticmethod
    def popular_products(df: DataFrame, watermark: str = "10 minutes") -> DataFrame:
        """Top products by interactions with a 1-hour sliding window.

        Uses a 1-hour window with 5-minute slides for trend detection.

        Args:
            df: Silver DataFrame with product_id and event_type.
            watermark: Late arrival tolerance.

        Returns:
            DataFrame with product interaction metrics per window.
        """
        return (
            df.filter(col("product_id").isNotNull())
            .withWatermark("event_timestamp", watermark)
            .groupBy(
                window(col("event_timestamp"), "1 hour", "5 minutes"),
                "product_id",
            )
            .agg(
                count("*").alias("total_interactions"),
                spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias(
                    "views"
                ),
                spark_sum(
                    when(col("event_type") == "add_to_cart", 1).otherwise(0)
                ).alias("cart_adds"),
                spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias(
                    "purchases"
                ),
                spark_sum(col("cart_value")).alias("total_revenue"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "product_id",
                "total_interactions",
                "views",
                "cart_adds",
                "purchases",
                "total_revenue",
            )
        )

    @staticmethod
    def active_users_hourly(df: DataFrame, watermark: str = "10 minutes") -> DataFrame:
        """Hourly active user metrics.

        Args:
            df: Silver DataFrame with user_id and session_id.
            watermark: Late arrival tolerance.

        Returns:
            DataFrame with hourly user activity metrics.
        """
        return (
            df.withWatermark("event_timestamp", watermark)
            .groupBy(window(col("event_timestamp"), "1 hour"))
            .agg(
                approx_count_distinct("user_id").alias("active_users"),
                approx_count_distinct("session_id").alias("total_sessions"),
                count("*").alias("total_events"),
                avg(when(col("cart_value").isNotNull(), col("cart_value"))).alias(
                    "avg_cart_value"
                ),
            )
            .select(
                col("window.start").alias("hour_start"),
                col("window.end").alias("hour_end"),
                "active_users",
                "total_sessions",
                "total_events",
                "avg_cart_value",
            )
        )
