"""Tests for the data quality framework."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from src.quality.checker import DataQualityChecker, QualityCheck


class TestDataQualityChecker:
    """Tests for the DataQualityChecker."""

    @pytest.fixture()
    def checker(self, tmp_path) -> DataQualityChecker:
        db_path = str(tmp_path / "test_quality.db")
        return DataQualityChecker(db_path=db_path)

    def test_init_creates_db(self, checker: DataQualityChecker) -> None:
        import sqlite3

        conn = sqlite3.connect(checker.db_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='quality_metrics'"
        )
        assert cursor.fetchone() is not None
        conn.close()

    def test_default_checks_registered(self, checker: DataQualityChecker) -> None:
        check_names = [c.name for c in checker.checks]
        assert "bronze_schema_validation" in check_names
        assert "bronze_null_check" in check_names
        assert "silver_dedup_check" in check_names
        assert "silver_timestamp_validity" in check_names
        assert "gold_aggregation_completeness" in check_names

    def test_register_custom_check(self, checker: DataQualityChecker) -> None:
        checker.register_check(
            QualityCheck(
                name="custom_check",
                layer="bronze",
                check_fn=lambda df: 1.0,
                threshold=0.9,
                description="Custom test check",
            )
        )
        names = [c.name for c in checker.checks]
        assert "custom_check" in names


class TestBronzeChecks:
    """Tests for Bronze layer quality checks."""

    @pytest.fixture()
    def checker(self, tmp_path) -> DataQualityChecker:
        return DataQualityChecker(db_path=str(tmp_path / "q.db"))

    def test_all_fields_present_score_1(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        df = spark.createDataFrame(
            [("e1", "u1", "2024-01-01", "page_view", "s1")],
            ["event_id", "user_id", "timestamp", "event_type", "session_id"],
        )
        results = checker.run_checks(df, "bronze")
        schema_result = next(
            r for r in results if r.check_name == "bronze_schema_validation"
        )
        assert schema_result.score == 1.0
        assert schema_result.passed is True

    def test_missing_fields_lower_score(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        df = spark.createDataFrame(
            [
                ("e1", "u1", "2024-01-01", "page_view", "s1"),
                (None, "u2", "2024-01-01", "page_view", "s2"),
            ],
            ["event_id", "user_id", "timestamp", "event_type", "session_id"],
        )
        results = checker.run_checks(df, "bronze")
        schema_result = next(
            r for r in results if r.check_name == "bronze_schema_validation"
        )
        assert schema_result.score == 0.5

    def test_null_check_perfect_score(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        df = spark.createDataFrame(
            [("e1", "u1", "ts", "pv", "s1")],
            ["event_id", "user_id", "timestamp", "event_type", "session_id"],
        )
        results = checker.run_checks(df, "bronze")
        null_result = next(r for r in results if r.check_name == "bronze_null_check")
        assert null_result.score == 1.0

    def test_empty_df_returns_1(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        schema = StructType(
            [
                StructField("event_id", StringType()),
                StructField("user_id", StringType()),
                StructField("timestamp", StringType()),
                StructField("event_type", StringType()),
                StructField("session_id", StringType()),
            ]
        )
        df = spark.createDataFrame([], schema=schema)
        results = checker.run_checks(df, "bronze")
        for result in results:
            assert result.score == 1.0


class TestSilverChecks:
    """Tests for Silver layer quality checks."""

    @pytest.fixture()
    def checker(self, tmp_path) -> DataQualityChecker:
        return DataQualityChecker(db_path=str(tmp_path / "q.db"))

    def test_no_duplicates_score_1(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        df = spark.createDataFrame(
            [
                ("e1", datetime(2024, 1, 1)),
                ("e2", datetime(2024, 1, 1)),
            ],
            ["event_id", "event_timestamp"],
        )
        results = checker.run_checks(df, "silver")
        dedup = next(r for r in results if r.check_name == "silver_dedup_check")
        assert dedup.score == 1.0

    def test_duplicates_lower_score(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        df = spark.createDataFrame(
            [
                ("e1", datetime(2024, 1, 1)),
                ("e1", datetime(2024, 1, 1)),
            ],
            ["event_id", "event_timestamp"],
        )
        results = checker.run_checks(df, "silver")
        dedup = next(r for r in results if r.check_name == "silver_dedup_check")
        assert dedup.score == 0.5


class TestQualityHistory:
    """Tests for quality metric persistence."""

    @pytest.fixture()
    def checker(self, tmp_path) -> DataQualityChecker:
        return DataQualityChecker(db_path=str(tmp_path / "q.db"))

    def test_results_persisted(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        df = spark.createDataFrame(
            [("e1", "u1", "ts", "pv", "s1")],
            ["event_id", "user_id", "timestamp", "event_type", "session_id"],
        )
        checker.run_checks(df, "bronze")
        history = checker.get_quality_history()
        assert len(history) >= 2  # At least schema + null checks

    def test_filter_by_check_name(
        self, spark: SparkSession, checker: DataQualityChecker
    ) -> None:
        df = spark.createDataFrame(
            [("e1", "u1", "ts", "pv", "s1")],
            ["event_id", "user_id", "timestamp", "event_type", "session_id"],
        )
        checker.run_checks(df, "bronze")
        history = checker.get_quality_history(check_name="bronze_null_check")
        assert len(history) == 1
        assert history[0]["check_name"] == "bronze_null_check"
