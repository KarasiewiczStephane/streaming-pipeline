"""Tests for dashboard data helpers and chart builders."""

import sqlite3

import pandas as pd
import pytest

from src.dashboard.app import (
    build_conversion_chart,
    build_quality_layer_chart,
    build_quality_trend_chart,
    build_throughput_chart,
    build_top_products_chart,
    compute_overview_metrics,
    load_quality_metrics,
)


class TestLoadQualityMetrics:
    """Tests for the SQLite quality metrics loader."""

    def test_returns_empty_when_file_missing(self, tmp_path) -> None:
        result = load_quality_metrics(str(tmp_path / "no_such.db"))
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_loads_rows_from_db(self, tmp_path) -> None:
        db = str(tmp_path / "metrics.db")
        conn = sqlite3.connect(db)
        conn.execute(
            "CREATE TABLE quality_metrics "
            "(id INTEGER PRIMARY KEY, check_name TEXT, layer TEXT, "
            "score REAL, passed INTEGER, record_count INTEGER, "
            "details TEXT, timestamp TEXT)"
        )
        conn.execute(
            "INSERT INTO quality_metrics VALUES (1,'chk','bronze',0.95,1,100,'ok','2024-01-01T00:00:00')"
        )
        conn.commit()
        conn.close()

        df = load_quality_metrics(db)
        assert len(df) == 1
        assert df.iloc[0]["check_name"] == "chk"


class TestComputeOverviewMetrics:
    """Tests for the overview metric aggregation."""

    def test_all_empty(self) -> None:
        result = compute_overview_metrics(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        )
        assert result["total_events"] == 0
        assert result["conversion_rate"] == 0.0
        assert result["avg_quality"] == 0.0
        assert result["failed_checks"] == 0

    def test_with_data(self) -> None:
        events = pd.DataFrame({"event_count": [10, 20, 30]})
        conversion = pd.DataFrame({"conversion_rate": [0.05, 0.10]})
        quality = pd.DataFrame({"score": [0.9, 1.0], "passed": [1, 0]})

        result = compute_overview_metrics(events, conversion, quality)
        assert result["total_events"] == 60
        assert result["conversion_rate"] == pytest.approx(10.0)
        assert result["avg_quality"] == pytest.approx(95.0)
        assert result["failed_checks"] == 1


class TestBuildThroughputChart:
    """Tests for the throughput chart builder."""

    def test_empty_df_returns_none(self) -> None:
        assert build_throughput_chart(pd.DataFrame()) is None

    def test_missing_column_returns_none(self) -> None:
        df = pd.DataFrame({"event_count": [1]})
        assert build_throughput_chart(df) is None

    def test_returns_figure(self) -> None:
        df = pd.DataFrame(
            {
                "window_start": ["2024-01-01"],
                "event_count": [100],
                "event_type": ["page_view"],
            }
        )
        fig = build_throughput_chart(df)
        assert fig is not None


class TestBuildQualityCharts:
    """Tests for quality chart builders."""

    def test_layer_chart_empty(self) -> None:
        assert build_quality_layer_chart(pd.DataFrame()) is None

    def test_layer_chart_with_data(self) -> None:
        df = pd.DataFrame({"layer": ["bronze", "silver"], "score": [0.95, 1.0]})
        fig = build_quality_layer_chart(df)
        assert fig is not None

    def test_trend_chart_empty(self) -> None:
        assert build_quality_trend_chart(pd.DataFrame()) is None

    def test_trend_chart_with_data(self) -> None:
        df = pd.DataFrame(
            {
                "timestamp": ["2024-01-01T00:00:00"],
                "score": [0.99],
                "check_name": ["bronze_null"],
            }
        )
        fig = build_quality_trend_chart(df)
        assert fig is not None


class TestBuildBusinessCharts:
    """Tests for business metric chart builders."""

    def test_conversion_empty(self) -> None:
        assert build_conversion_chart(pd.DataFrame()) is None

    def test_conversion_with_data(self) -> None:
        df = pd.DataFrame({"window_start": ["2024-01-01"], "conversion_rate": [0.05]})
        assert build_conversion_chart(df) is not None

    def test_top_products_empty(self) -> None:
        assert build_top_products_chart(pd.DataFrame()) is None

    def test_top_products_with_data(self) -> None:
        df = pd.DataFrame({"product_id": ["p1", "p2"], "total_revenue": [100.0, 200.0]})
        fig = build_top_products_chart(df)
        assert fig is not None
