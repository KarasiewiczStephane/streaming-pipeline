"""Tests for Silver layer transformations."""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.consumer.transformations import SilverTransformations, process_silver


def _bronze_schema():
    return StructType(
        [
            StructField("event_id", StringType()),
            StructField("user_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("event_type", StringType()),
            StructField("product_id", StringType()),
            StructField("session_id", StringType()),
            StructField("metadata", MapType(StringType(), StringType())),
            StructField("kafka_timestamp", TimestampType()),
            StructField("kafka_partition", StringType()),
            StructField("kafka_offset", StringType()),
        ]
    )


def _make_bronze_row(
    event_id="e1",
    user_id="USER-001",
    timestamp="2024-01-01T12:00:00.000000Z",
    event_type="page_view",
    product_id="PROD-001",
    session_id="s1",
    metadata=None,
    kafka_ts=None,
):
    if metadata is None:
        metadata = {"page_url": "/", "device_type": "desktop", "referrer": "google"}
    if kafka_ts is None:
        kafka_ts = datetime(2024, 1, 1, 12, 0, 0)
    return (
        event_id,
        user_id,
        timestamp,
        event_type,
        product_id,
        session_id,
        metadata,
        kafka_ts,
        "0",
        "1",
    )


class TestCleanStrings:
    """Tests for string cleaning transformation."""

    def test_trims_and_lowercases(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("USER-001 ", "Session-1")], ["user_id", "session_id"]
        )
        result = SilverTransformations.clean_strings(df, ["user_id", "session_id"])
        row = result.collect()[0]
        assert row.user_id == "user-001"
        assert row.session_id == "session-1"

    def test_handles_already_clean(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("user-001",)], ["user_id"])
        result = SilverTransformations.clean_strings(df, ["user_id"])
        assert result.collect()[0].user_id == "user-001"


class TestDeduplicateEvents:
    """Tests for event deduplication."""

    def test_removes_duplicates(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                ("e1", datetime(2024, 1, 1, 12, 0)),
                ("e1", datetime(2024, 1, 1, 12, 1)),
                ("e2", datetime(2024, 1, 1, 12, 0)),
            ],
            ["event_id", "kafka_timestamp"],
        )
        result = SilverTransformations.deduplicate_events(df)
        assert result.count() == 2

    def test_keeps_earliest(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                ("e1", datetime(2024, 1, 1, 12, 1)),
                ("e1", datetime(2024, 1, 1, 12, 0)),
            ],
            ["event_id", "kafka_timestamp"],
        )
        result = SilverTransformations.deduplicate_events(df)
        row = result.collect()[0]
        assert row.kafka_timestamp == datetime(2024, 1, 1, 12, 0)


class TestParseTimestamp:
    """Tests for timestamp parsing."""

    def test_parses_iso_timestamp(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("2024-01-01T12:00:00.000000Z",)], ["timestamp"])
        result = SilverTransformations.parse_timestamp(df)
        row = result.collect()[0]
        assert row.event_timestamp is not None
        assert "timestamp" not in result.columns


class TestExtractMetadata:
    """Tests for metadata extraction."""

    def test_extracts_base_fields(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [({"page_url": "/home", "device_type": "mobile", "referrer": "google"},)],
            StructType([StructField("metadata", MapType(StringType(), StringType()))]),
        )
        result = SilverTransformations.extract_metadata(df)
        row = result.collect()[0]
        assert row.page_url == "/home"
        assert row.device_type == "mobile"
        assert row.referrer == "google"
        assert "metadata" not in result.columns

    def test_extracts_cart_fields(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [({"cart_value": "49.99", "quantity": "2"},)],
            StructType([StructField("metadata", MapType(StringType(), StringType()))]),
        )
        result = SilverTransformations.extract_metadata(df)
        row = result.collect()[0]
        assert row.cart_value == 49.99
        assert row.quantity == 2

    def test_handles_empty_metadata(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [({},)],
            StructType([StructField("metadata", MapType(StringType(), StringType()))]),
        )
        result = SilverTransformations.extract_metadata(df)
        row = result.collect()[0]
        assert row.page_url is None
        assert row.cart_value is None


class TestAddDerivedColumns:
    """Tests for derived column generation."""

    def test_purchase_is_conversion(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("purchase",)], ["event_type"])
        result = SilverTransformations.add_derived_columns(df)
        row = result.collect()[0]
        assert row.is_conversion is True
        assert row.is_cart_event is True

    def test_page_view_not_conversion(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("page_view",)], ["event_type"])
        result = SilverTransformations.add_derived_columns(df)
        row = result.collect()[0]
        assert row.is_conversion is False
        assert row.is_cart_event is False

    def test_add_to_cart_is_cart_event(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("add_to_cart",)], ["event_type"])
        result = SilverTransformations.add_derived_columns(df)
        row = result.collect()[0]
        assert row.is_cart_event is True
        assert row.is_conversion is False


class TestValidateRequiredFields:
    """Tests for required field validation."""

    def test_filters_null_user_id(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [
                ("e1", "u1", "s1", "page_view"),
                ("e2", None, "s2", "page_view"),
            ],
            ["event_id", "user_id", "session_id", "event_type"],
        )
        result = SilverTransformations.validate_required_fields(df)
        assert result.count() == 1

    def test_keeps_valid_records(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("e1", "u1", "s1", "page_view")],
            ["event_id", "user_id", "session_id", "event_type"],
        )
        result = SilverTransformations.validate_required_fields(df)
        assert result.count() == 1


class TestProcessSilver:
    """Integration tests for the full Silver pipeline."""

    def test_full_pipeline(self, spark: SparkSession) -> None:
        rows = [
            _make_bronze_row(event_id="e1", user_id="USER-001 "),
            _make_bronze_row(
                event_id="e2",
                event_type="purchase",
                metadata={
                    "page_url": "/checkout",
                    "device_type": "mobile",
                    "cart_value": "99.99",
                    "quantity": "1",
                },
            ),
            _make_bronze_row(
                event_id="e1",
                user_id="USER-001 ",
                kafka_ts=datetime(2024, 1, 1, 12, 1),
            ),  # Duplicate
        ]
        df = spark.createDataFrame(rows, schema=_bronze_schema())
        result = process_silver(df)

        assert result.count() == 2  # Duplicate removed
        cols = result.columns
        assert "page_url" in cols
        assert "is_conversion" in cols
        assert "metadata" not in cols
        assert "timestamp" not in cols

    def test_null_required_field_filtered(self, spark: SparkSession) -> None:
        rows = [
            _make_bronze_row(event_id="e1"),
            _make_bronze_row(event_id=None),
        ]
        df = spark.createDataFrame(rows, schema=_bronze_schema())
        result = process_silver(df)
        assert result.count() == 1
