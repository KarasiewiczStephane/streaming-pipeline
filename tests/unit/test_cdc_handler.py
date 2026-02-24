"""Tests for the CDC handler with SCD Type 2."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


from src.storage.cdc_handler import CDCHandler


def _user_schema():
    return StructType(
        [
            StructField("user_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
        ]
    )


def _cdc_schema():
    return StructType(
        [
            StructField("op", StringType()),
            StructField(
                "before",
                StructType(
                    [
                        StructField("user_id", StringType()),
                        StructField("name", StringType()),
                        StructField("email", StringType()),
                    ]
                ),
            ),
            StructField(
                "after",
                StructType(
                    [
                        StructField("user_id", StringType()),
                        StructField("name", StringType()),
                        StructField("email", StringType()),
                    ]
                ),
            ),
        ]
    )


class TestCDCHandler:
    """Tests for CDCHandler SCD Type 2 operations."""

    def test_initialize_dimension_table(self, spark: SparkSession, tmp_path) -> None:
        path = str(tmp_path / "dim_users")
        handler = CDCHandler(spark, path)
        initial_df = spark.createDataFrame(
            [("u1", "Alice", "alice@test.com")],
            schema=_user_schema(),
        )
        handler.initialize_dimension_table(initial_df)

        result = spark.read.format("delta").load(path)
        assert result.count() == 1
        row = result.collect()[0]
        assert row["is_current"] is True
        assert row["version"] == 1
        assert row["effective_to"] is None

    def test_apply_create(self, spark: SparkSession, tmp_path) -> None:
        path = str(tmp_path / "dim_create")
        handler = CDCHandler(spark, path)
        initial_df = spark.createDataFrame(
            [("u1", "Alice", "alice@test.com")],
            schema=_user_schema(),
        )
        handler.initialize_dimension_table(initial_df)

        cdc_df = spark.createDataFrame(
            [("c", None, ("u2", "Bob", "bob@test.com"))],
            schema=_cdc_schema(),
        )
        handler.apply_cdc_batch(cdc_df)

        result = spark.read.format("delta").load(path)
        assert result.filter("is_current = true").count() == 2

    def test_apply_update_creates_new_version(
        self, spark: SparkSession, tmp_path
    ) -> None:
        path = str(tmp_path / "dim_update")
        handler = CDCHandler(spark, path)
        initial_df = spark.createDataFrame(
            [("u1", "Alice", "alice@test.com")],
            schema=_user_schema(),
        )
        handler.initialize_dimension_table(initial_df)

        cdc_df = spark.createDataFrame(
            [
                (
                    "u",
                    ("u1", "Alice", "alice@test.com"),
                    ("u1", "Alice Smith", "alice.smith@test.com"),
                )
            ],
            schema=_cdc_schema(),
        )
        handler.apply_cdc_batch(cdc_df)

        result = spark.read.format("delta").load(path)
        # Should have 2 rows: old (closed) + new (current)
        assert result.count() == 2
        current = result.filter("is_current = true").collect()
        assert len(current) == 1
        assert current[0]["name"] == "Alice Smith"
        assert current[0]["version"] == 2

        closed = result.filter("is_current = false").collect()
        assert len(closed) == 1
        assert closed[0]["effective_to"] is not None

    def test_apply_delete_closes_record(self, spark: SparkSession, tmp_path) -> None:
        path = str(tmp_path / "dim_delete")
        handler = CDCHandler(spark, path)
        initial_df = spark.createDataFrame(
            [("u1", "Alice", "alice@test.com")],
            schema=_user_schema(),
        )
        handler.initialize_dimension_table(initial_df)

        cdc_df = spark.createDataFrame(
            [("d", ("u1", "Alice", "alice@test.com"), None)],
            schema=_cdc_schema(),
        )
        handler.apply_cdc_batch(cdc_df)

        result = spark.read.format("delta").load(path)
        current = result.filter("is_current = true")
        assert current.count() == 0

    def test_raises_on_missing_table(self, spark: SparkSession, tmp_path) -> None:
        path = str(tmp_path / "nonexistent")
        handler = CDCHandler(spark, path)
        cdc_df = spark.createDataFrame(
            [("c", None, ("u1", "Alice", "alice@test.com"))],
            schema=_cdc_schema(),
        )
        with pytest.raises(ValueError, match="Dimension table not found"):
            handler.apply_cdc_batch(cdc_df)
