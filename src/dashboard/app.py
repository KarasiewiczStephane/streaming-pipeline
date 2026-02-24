"""Streamlit monitoring dashboard for the streaming pipeline.

Displays real-time throughput metrics, data quality scores,
business metrics, and infrastructure health indicators.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------


def load_quality_metrics(db_path: str) -> pd.DataFrame:
    """Load quality metrics from the SQLite database.

    Args:
        db_path: Path to the quality metrics SQLite file.

    Returns:
        DataFrame with quality metric rows, or empty DataFrame on error.
    """
    if not Path(db_path).exists():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql(
            "SELECT * FROM quality_metrics ORDER BY timestamp DESC LIMIT 1000",
            conn,
        )
        conn.close()
        return df
    except Exception:
        logger.exception("Failed to load quality metrics from %s", db_path)
        return pd.DataFrame()


def load_delta_table(spark: Any, path: str) -> pd.DataFrame:
    """Load a Delta table into a Pandas DataFrame.

    Args:
        spark: Active SparkSession.
        path: File path to the Delta table.

    Returns:
        Pandas DataFrame, or empty DataFrame if table cannot be read.
    """
    try:
        return spark.read.format("delta").load(path).toPandas()
    except Exception:
        logger.exception("Failed to load Delta table at %s", path)
        return pd.DataFrame()


def compute_overview_metrics(
    events_df: pd.DataFrame,
    conversion_df: pd.DataFrame,
    quality_df: pd.DataFrame,
) -> dict[str, Any]:
    """Compute high-level overview numbers for the dashboard.

    Args:
        events_df: Gold events-per-minute DataFrame.
        conversion_df: Gold conversion-rate DataFrame.
        quality_df: Quality metrics DataFrame.

    Returns:
        Dictionary with total_events, conversion_rate, avg_quality, failed_checks.
    """
    total_events = (
        int(events_df["event_count"].sum())
        if not events_df.empty and "event_count" in events_df.columns
        else 0
    )

    conversion_rate = (
        float(conversion_df["conversion_rate"].iloc[-1] * 100)
        if not conversion_df.empty and "conversion_rate" in conversion_df.columns
        else 0.0
    )

    avg_quality = (
        float(quality_df["score"].mean() * 100)
        if not quality_df.empty and "score" in quality_df.columns
        else 0.0
    )

    failed_checks = (
        int((quality_df["passed"] == 0).sum())
        if not quality_df.empty and "passed" in quality_df.columns
        else 0
    )

    return {
        "total_events": total_events,
        "conversion_rate": conversion_rate,
        "avg_quality": avg_quality,
        "failed_checks": failed_checks,
    }


def build_throughput_chart(events_df: pd.DataFrame) -> Any | None:
    """Build a Plotly line chart for events per minute.

    Args:
        events_df: Gold events-per-minute DataFrame.

    Returns:
        Plotly Figure or None if data is unavailable.
    """
    if events_df.empty or "window_start" not in events_df.columns:
        return None
    return px.line(
        events_df,
        x="window_start",
        y="event_count",
        color="event_type",
        title="Events Per Minute by Type",
    )


def build_quality_layer_chart(quality_df: pd.DataFrame) -> Any | None:
    """Build a Plotly bar chart of average quality score by layer.

    Args:
        quality_df: Quality metrics DataFrame.

    Returns:
        Plotly Figure or None if data is unavailable.
    """
    if quality_df.empty or "layer" not in quality_df.columns:
        return None
    layer_scores = quality_df.groupby("layer")["score"].mean().reset_index()
    return px.bar(
        layer_scores,
        x="layer",
        y="score",
        title="Average Quality Score by Layer",
        color="score",
        color_continuous_scale="RdYlGn",
    )


def build_quality_trend_chart(quality_df: pd.DataFrame) -> Any | None:
    """Build a Plotly line chart for quality score trend over time.

    Args:
        quality_df: Quality metrics DataFrame with a ``timestamp`` column.

    Returns:
        Plotly Figure or None if data is unavailable.
    """
    if quality_df.empty or "timestamp" not in quality_df.columns:
        return None
    df = quality_df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return px.line(
        df,
        x="timestamp",
        y="score",
        color="check_name",
        title="Quality Score Trend",
    )


def build_conversion_chart(conversion_df: pd.DataFrame) -> Any | None:
    """Build a Plotly line chart for conversion rate over time.

    Args:
        conversion_df: Gold conversion-rate DataFrame.

    Returns:
        Plotly Figure or None.
    """
    if conversion_df.empty or "window_start" not in conversion_df.columns:
        return None
    return px.line(
        conversion_df,
        x="window_start",
        y="conversion_rate",
        title="Conversion Rate Over Time",
    )


def build_top_products_chart(products_df: pd.DataFrame) -> Any | None:
    """Build a Plotly bar chart for top products by revenue.

    Args:
        products_df: Gold popular-products DataFrame.

    Returns:
        Plotly Figure or None.
    """
    if products_df.empty or "product_id" not in products_df.columns:
        return None
    top = (
        products_df.groupby("product_id")
        .agg({"total_revenue": "sum"})
        .nlargest(10, "total_revenue")
        .reset_index()
    )
    return px.bar(
        top,
        x="product_id",
        y="total_revenue",
        title="Top 10 Products by Revenue",
    )


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------


def render_overview(
    events_df: pd.DataFrame,
    conversion_df: pd.DataFrame,
    quality_df: pd.DataFrame,
) -> None:
    """Render the Overview page.

    Args:
        events_df: Gold events-per-minute DataFrame.
        conversion_df: Gold conversion-rate DataFrame.
        quality_df: Quality metrics DataFrame.
    """
    st.title("Pipeline Overview")
    metrics = compute_overview_metrics(events_df, conversion_df, quality_df)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Events (Last Hour)", f"{metrics['total_events']:,}")
    with col2:
        st.metric("Conversion Rate", f"{metrics['conversion_rate']:.2f}%")
    with col3:
        st.metric("Avg Quality Score", f"{metrics['avg_quality']:.1f}%")
    with col4:
        st.metric("Failed Checks", metrics["failed_checks"])


def render_throughput(events_df: pd.DataFrame) -> None:
    """Render the Throughput page.

    Args:
        events_df: Gold events-per-minute DataFrame.
    """
    st.title("Throughput Metrics")
    fig = build_throughput_chart(events_df)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No throughput data available yet")


def render_quality(quality_df: pd.DataFrame) -> None:
    """Render the Data Quality page.

    Args:
        quality_df: Quality metrics DataFrame.
    """
    st.title("Data Quality Scores")
    layer_fig = build_quality_layer_chart(quality_df)
    if layer_fig:
        st.plotly_chart(layer_fig, use_container_width=True)

    trend_fig = build_quality_trend_chart(quality_df)
    if trend_fig:
        st.plotly_chart(trend_fig, use_container_width=True)

    if not quality_df.empty and "passed" in quality_df.columns:
        failed = quality_df[quality_df["passed"] == 0]
        if not failed.empty:
            st.subheader("Failed Checks")
            st.dataframe(failed[["check_name", "layer", "score", "timestamp"]])
    else:
        st.warning("No quality metrics available yet")


def render_business(
    conversion_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> None:
    """Render the Business Metrics page.

    Args:
        conversion_df: Gold conversion-rate DataFrame.
        products_df: Gold popular-products DataFrame.
    """
    st.title("Business Metrics")
    conv_fig = build_conversion_chart(conversion_df)
    if conv_fig:
        st.plotly_chart(conv_fig, use_container_width=True)

    prod_fig = build_top_products_chart(products_df)
    if prod_fig:
        st.plotly_chart(prod_fig, use_container_width=True)

    if conversion_df.empty and products_df.empty:
        st.warning("No business metrics available yet")


# ---------------------------------------------------------------------------
# Main entry-point (invoked by `streamlit run`)
# ---------------------------------------------------------------------------


def main() -> None:
    """Dashboard entry-point wiring Streamlit UI to data sources."""
    st.set_page_config(
        page_title="Streaming Pipeline Dashboard",
        layout="wide",
    )

    config = load_config()
    quality_db = config["quality"]["db_path"]

    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Page",
        ["Overview", "Throughput", "Data Quality", "Business Metrics"],
    )

    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
    if auto_refresh:
        st.empty()

    quality_df = load_quality_metrics(quality_db)

    if page == "Overview":
        render_overview(pd.DataFrame(), pd.DataFrame(), quality_df)
    elif page == "Throughput":
        render_throughput(pd.DataFrame())
    elif page == "Data Quality":
        render_quality(quality_df)
    elif page == "Business Metrics":
        render_business(pd.DataFrame(), pd.DataFrame())


if __name__ == "__main__":
    main()
