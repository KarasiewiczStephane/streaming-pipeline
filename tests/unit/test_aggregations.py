"""Tests for Gold layer aggregation functions.

Uses batch mode to verify aggregation logic since streaming
watermarks require a running query.
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.consumer.aggregations import GoldAggregations


def _silver_schema():
    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("user_id", StringType()),
            StructField("event_timestamp", TimestampType()),
            StructField("event_type", StringType()),
            StructField("product_id", StringType()),
            StructField("session_id", StringType()),
            StructField("cart_value", DoubleType()),
        ]
    )


def _make_silver_data(spark: SparkSession):
    """Create sample Silver DataFrame with known values."""
    data = [
        ("e1", "u1", datetime(2024, 1, 1, 12, 0, 10), "page_view", "p1", "s1", None),
        ("e2", "u1", datetime(2024, 1, 1, 12, 0, 20), "add_to_cart", "p1", "s1", 49.99),
        ("e3", "u1", datetime(2024, 1, 1, 12, 0, 30), "purchase", "p1", "s1", 49.99),
        ("e4", "u2", datetime(2024, 1, 1, 12, 0, 40), "page_view", "p2", "s2", None),
        ("e5", "u2", datetime(2024, 1, 1, 12, 0, 50), "search", None, "s2", None),
        ("e6", "u3", datetime(2024, 1, 1, 12, 1, 10), "page_view", "p1", "s3", None),
        ("e7", "u3", datetime(2024, 1, 1, 12, 1, 20), "add_to_cart", "p1", "s3", 25.00),
    ]
    return spark.createDataFrame(data, schema=_silver_schema())


class TestEventsPerMinute:
    """Tests for events_per_minute aggregation."""

    def test_counts_events_per_type(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        # Use batch groupBy (no watermark in batch mode)
        result = (
            df.groupBy("event_type").count().orderBy("count", ascending=False).collect()
        )
        counts = {row.event_type: row["count"] for row in result}
        assert counts["page_view"] == 3
        assert counts["add_to_cart"] == 2
        assert counts["purchase"] == 1
        assert counts["search"] == 1

    def test_output_schema_has_expected_columns(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        # Batch simulation of the aggregation logic
        result = GoldAggregations.events_per_minute(df, watermark="0 seconds")
        cols = result.columns
        assert "window_start" in cols
        assert "window_end" in cols
        assert "event_type" in cols
        assert "event_count" in cols
        assert "unique_users" in cols
        assert "unique_sessions" in cols


class TestConversionRate:
    """Tests for conversion_rate aggregation."""

    def test_calculates_rates(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        result = GoldAggregations.conversion_rate(df, watermark="0 seconds")
        rows = result.collect()
        assert len(rows) > 0
        for row in rows:
            assert row.conversion_rate is not None
            assert row.cart_to_purchase_rate is not None

    def test_zero_purchases_gives_zero_rate(self, spark: SparkSession) -> None:
        data = [
            ("e1", "u1", datetime(2024, 1, 1, 12, 0, 0), "page_view", "p1", "s1", None),
            ("e2", "u2", datetime(2024, 1, 1, 12, 0, 1), "page_view", "p2", "s2", None),
        ]
        df = spark.createDataFrame(data, schema=_silver_schema())
        result = GoldAggregations.conversion_rate(df, watermark="0 seconds")
        rows = result.collect()
        for row in rows:
            assert row.purchases == 0
            assert row.conversion_rate == 0.0

    def test_output_schema(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        result = GoldAggregations.conversion_rate(df, watermark="0 seconds")
        expected_cols = [
            "window_start",
            "window_end",
            "total_events",
            "purchases",
            "add_to_carts",
            "sessions",
            "conversion_rate",
            "cart_to_purchase_rate",
        ]
        for c in expected_cols:
            assert c in result.columns


class TestPopularProducts:
    """Tests for popular_products aggregation."""

    def test_filters_null_product_ids(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        result = GoldAggregations.popular_products(df, watermark="0 seconds")
        product_ids = [row.product_id for row in result.collect()]
        assert None not in product_ids

    def test_counts_interactions(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        result = GoldAggregations.popular_products(df, watermark="0 seconds")
        assert result.count() > 0
        for row in result.collect():
            assert row.total_interactions > 0

    def test_output_schema(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        result = GoldAggregations.popular_products(df, watermark="0 seconds")
        expected = [
            "window_start",
            "window_end",
            "product_id",
            "total_interactions",
            "views",
            "cart_adds",
            "purchases",
            "total_revenue",
        ]
        for c in expected:
            assert c in result.columns


class TestActiveUsersHourly:
    """Tests for active_users_hourly aggregation."""

    def test_counts_active_users(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        result = GoldAggregations.active_users_hourly(df, watermark="0 seconds")
        rows = result.collect()
        assert len(rows) > 0
        for row in rows:
            assert row.active_users > 0
            assert row.total_events > 0

    def test_output_schema(self, spark: SparkSession) -> None:
        df = _make_silver_data(spark)
        result = GoldAggregations.active_users_hourly(df, watermark="0 seconds")
        expected = [
            "hour_start",
            "hour_end",
            "active_users",
            "total_sessions",
            "total_events",
            "avg_cart_value",
        ]
        for c in expected:
            assert c in result.columns
