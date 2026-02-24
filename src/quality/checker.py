"""Data quality framework with per-layer checks and metric logging.

Provides configurable quality checks for Bronze, Silver, and Gold layers
with threshold-based alerting and SQLite metric persistence.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, current_timestamp, when

from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class QualityCheck:
    """Definition of a single data quality check.

    Attributes:
        name: Unique identifier for this check.
        layer: Target layer (bronze, silver, gold).
        check_fn: Function that scores data quality from 0 to 1.
        threshold: Minimum acceptable score.
        description: Human-readable description.
    """

    name: str
    layer: str
    check_fn: Callable[[DataFrame], float]
    threshold: float
    description: str


@dataclass
class QualityResult:
    """Result of a quality check execution.

    Attributes:
        check_name: Name of the check that was run.
        layer: Layer the check targets.
        score: Quality score between 0 and 1.
        passed: Whether the score met the threshold.
        timestamp: When the check was executed.
        record_count: Number of records evaluated.
        details: Additional context about the result.
    """

    check_name: str
    layer: str
    score: float
    passed: bool
    timestamp: datetime
    record_count: int
    details: str | None = None


class DataQualityChecker:
    """Data quality framework with per-layer checks.

    Registers and runs quality checks against DataFrames, logs results
    to SQLite for historical tracking, and alerts on threshold breaches.

    Args:
        db_path: Path to the SQLite database for metric storage.
    """

    def __init__(self, db_path: str = "data/quality_metrics.db") -> None:
        self.db_path = db_path
        self.checks: list[QualityCheck] = []
        self._init_db()
        self._register_default_checks()

    def _init_db(self) -> None:
        """Initialize the SQLite database and metrics table."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                check_name TEXT,
                layer TEXT,
                score REAL,
                passed INTEGER,
                record_count INTEGER,
                details TEXT,
                timestamp TEXT
            )
            """
        )
        conn.commit()
        conn.close()

    def _register_default_checks(self) -> None:
        """Register the built-in quality checks for each layer."""
        self.register_check(
            QualityCheck(
                name="bronze_schema_validation",
                layer="bronze",
                check_fn=self._check_required_fields(
                    ["event_id", "user_id", "timestamp", "event_type", "session_id"]
                ),
                threshold=0.99,
                description="Verify required fields are present",
            )
        )
        self.register_check(
            QualityCheck(
                name="bronze_null_check",
                layer="bronze",
                check_fn=self._check_null_rate(["event_id", "user_id"]),
                threshold=0.99,
                description="Check null rate for critical fields",
            )
        )
        self.register_check(
            QualityCheck(
                name="silver_dedup_check",
                layer="silver",
                check_fn=self._check_duplicates("event_id"),
                threshold=1.0,
                description="Verify no duplicate event_ids",
            )
        )
        self.register_check(
            QualityCheck(
                name="silver_timestamp_validity",
                layer="silver",
                check_fn=self._check_timestamp_validity("event_timestamp"),
                threshold=0.99,
                description="Verify timestamps are valid and not in future",
            )
        )
        self.register_check(
            QualityCheck(
                name="gold_aggregation_completeness",
                layer="gold",
                check_fn=self._check_required_fields(["event_count", "unique_users"]),
                threshold=1.0,
                description="Verify aggregated metrics are non-null",
            )
        )

    def _check_required_fields(self, fields: list[str]) -> Callable[[DataFrame], float]:
        """Create a check function for required field presence.

        Args:
            fields: Column names that must be non-null.

        Returns:
            Check function returning score from 0 to 1.
        """

        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            valid = df
            for field in fields:
                if field in df.columns:
                    valid = valid.filter(col(field).isNotNull())
            return valid.count() / total

        return check

    def _check_null_rate(self, fields: list[str]) -> Callable[[DataFrame], float]:
        """Create a check function for null rate across fields.

        Args:
            fields: Column names to check for nulls.

        Returns:
            Check function returning score from 0 to 1.
        """

        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            present_fields = [f for f in fields if f in df.columns]
            if not present_fields:
                return 1.0
            null_exprs = [
                count(when(col(f).isNull(), f)).alias(f) for f in present_fields
            ]
            null_counts = df.select(null_exprs).collect()[0]
            total_nulls = sum(null_counts[f] for f in present_fields)
            return 1 - (total_nulls / (total * len(present_fields)))

        return check

    def _check_duplicates(self, key_field: str) -> Callable[[DataFrame], float]:
        """Create a check function for duplicate detection.

        Args:
            key_field: Column name that should be unique.

        Returns:
            Check function returning 1.0 if no duplicates.
        """

        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            distinct = df.select(key_field).distinct().count()
            return distinct / total

        return check

    def _check_timestamp_validity(self, ts_field: str) -> Callable[[DataFrame], float]:
        """Create a check function for timestamp validity.

        Args:
            ts_field: Timestamp column name.

        Returns:
            Check function returning fraction of valid timestamps.
        """

        def check(df: DataFrame) -> float:
            total = df.count()
            if total == 0:
                return 1.0
            if ts_field not in df.columns:
                return 1.0
            valid = df.filter(
                (col(ts_field).isNotNull()) & (col(ts_field) <= current_timestamp())
            ).count()
            return valid / total

        return check

    def register_check(self, check: QualityCheck) -> None:
        """Register a quality check.

        Args:
            check: QualityCheck instance to add.
        """
        self.checks.append(check)

    def run_checks(self, df: DataFrame, layer: str) -> list[QualityResult]:
        """Run all checks for a specified layer.

        Args:
            df: DataFrame to validate.
            layer: Layer name to filter checks (bronze, silver, gold).

        Returns:
            List of QualityResult objects.
        """
        results = []
        layer_checks = [c for c in self.checks if c.layer == layer]
        record_count = df.count()

        for check in layer_checks:
            score = check.check_fn(df)
            passed = score >= check.threshold

            result = QualityResult(
                check_name=check.name,
                layer=layer,
                score=score,
                passed=passed,
                timestamp=datetime.now(tz=timezone.utc),
                record_count=record_count,
                details=f"Threshold: {check.threshold}",
            )
            results.append(result)
            self._log_result(result)

            if not passed:
                logger.warning(
                    "Quality check FAILED: %s (score=%.3f, threshold=%.3f)",
                    check.name,
                    score,
                    check.threshold,
                )

        return results

    def _log_result(self, result: QualityResult) -> None:
        """Persist a quality result to SQLite.

        Args:
            result: QualityResult to log.
        """
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO quality_metrics "
            "(check_name, layer, score, passed, record_count, details, timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                result.check_name,
                result.layer,
                result.score,
                int(result.passed),
                result.record_count,
                result.details,
                result.timestamp.isoformat(),
            ),
        )
        conn.commit()
        conn.close()

    def get_quality_history(
        self, check_name: str | None = None, limit: int = 100
    ) -> list[dict]:
        """Retrieve quality check history from SQLite.

        Args:
            check_name: Filter by check name, or None for all.
            limit: Maximum number of results.

        Returns:
            List of result dictionaries.
        """
        conn = sqlite3.connect(self.db_path)
        if check_name:
            cursor = conn.execute(
                "SELECT * FROM quality_metrics WHERE check_name = ? "
                "ORDER BY timestamp DESC LIMIT ?",
                (check_name, limit),
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM quality_metrics ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            )
        columns = [
            "id",
            "check_name",
            "layer",
            "score",
            "passed",
            "record_count",
            "details",
            "timestamp",
        ]
        rows = cursor.fetchall()
        conn.close()
        return [dict(zip(columns, row)) for row in rows]
