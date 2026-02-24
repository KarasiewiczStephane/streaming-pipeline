"""Shared test fixtures for the streaming pipeline."""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for unit tests.

    Uses local mode with minimal resources and no external dependencies.
    The session is reused across all tests in the session.
    """
    builder = (
        SparkSession.builder.appName("TestSession")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture()
def sample_clickstream_events():
    """Sample clickstream events for testing."""
    return [
        {
            "event_id": "e1",
            "user_id": "USER-000001",
            "timestamp": "2024-01-01T12:00:00.000000Z",
            "event_type": "page_view",
            "product_id": "PROD-0001",
            "session_id": "s1",
            "metadata": {"page_url": "/", "device_type": "desktop"},
        },
        {
            "event_id": "e2",
            "user_id": "USER-000001",
            "timestamp": "2024-01-01T12:01:00.000000Z",
            "event_type": "add_to_cart",
            "product_id": "PROD-0001",
            "session_id": "s1",
            "metadata": {
                "page_url": "/products",
                "device_type": "desktop",
                "cart_value": "49.99",
                "quantity": "1",
            },
        },
        {
            "event_id": "e3",
            "user_id": "USER-000002",
            "timestamp": "2024-01-01T12:02:00.000000Z",
            "event_type": "purchase",
            "product_id": "PROD-0002",
            "session_id": "s2",
            "metadata": {
                "page_url": "/checkout",
                "device_type": "mobile",
                "cart_value": "99.99",
                "quantity": "2",
            },
        },
        {
            "event_id": "e4",
            "user_id": "USER-000003",
            "timestamp": "2024-01-01T12:03:00.000000Z",
            "event_type": "search",
            "product_id": None,
            "session_id": "s3",
            "metadata": {
                "page_url": "/search",
                "device_type": "tablet",
                "search_query": "laptop",
            },
        },
    ]
