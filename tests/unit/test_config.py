"""Tests for configuration loading utilities."""

import os

import pytest
import yaml

from src.utils.config import (
    DeltaConfig,
    KafkaConfig,
    KafkaTopics,
    SparkConfig,
    get_delta_config,
    get_kafka_config,
    get_spark_config,
    load_config,
)


@pytest.fixture()
def config_file(tmp_path):
    """Create a temporary config YAML file."""
    config_data = {
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topics": {
                "clickstream": "clickstream-events",
                "cdc": "cdc-events",
                "dlq": "dead-letter-queue",
            },
        },
        "producer": {
            "events_per_second": 100,
            "batch_size": 100,
            "num_users": 1000,
        },
        "spark": {
            "app_name": "TestPipeline",
            "checkpoint_location": "/tmp/test-checkpoints",
        },
        "delta": {
            "bronze_path": "/data/test/bronze",
            "silver_path": "/data/test/silver",
            "gold_path": "/data/test/gold",
        },
    }
    path = tmp_path / "config.yaml"
    with open(path, "w") as f:
        yaml.dump(config_data, f)
    return str(path)


class TestLoadConfig:
    """Tests for the load_config function."""

    def test_loads_valid_config(self, config_file: str) -> None:
        config = load_config(config_file)
        assert "kafka" in config
        assert "spark" in config
        assert "delta" in config

    def test_returns_correct_kafka_servers(self, config_file: str) -> None:
        config = load_config(config_file)
        assert config["kafka"]["bootstrap_servers"] == "localhost:9092"

    def test_raises_on_missing_file(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/config.yaml")

    def test_env_override_kafka_servers(self, config_file: str) -> None:
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "kafka:29092"
        try:
            config = load_config(config_file)
            assert config["kafka"]["bootstrap_servers"] == "kafka:29092"
        finally:
            del os.environ["KAFKA_BOOTSTRAP_SERVERS"]


class TestGetKafkaConfig:
    """Tests for the get_kafka_config helper."""

    def test_returns_kafka_config(self, config_file: str) -> None:
        config = load_config(config_file)
        kafka = get_kafka_config(config)
        assert isinstance(kafka, KafkaConfig)
        assert kafka.bootstrap_servers == "localhost:9092"
        assert isinstance(kafka.topics, KafkaTopics)

    def test_default_values_on_empty(self) -> None:
        kafka = get_kafka_config({})
        assert kafka.bootstrap_servers == "localhost:9092"
        assert kafka.topics.clickstream == "clickstream-events"


class TestGetSparkConfig:
    """Tests for the get_spark_config helper."""

    def test_returns_spark_config(self, config_file: str) -> None:
        config = load_config(config_file)
        spark = get_spark_config(config)
        assert isinstance(spark, SparkConfig)
        assert spark.app_name == "TestPipeline"

    def test_default_values_on_empty(self) -> None:
        spark = get_spark_config({})
        assert spark.app_name == "StreamingPipeline"


class TestGetDeltaConfig:
    """Tests for the get_delta_config helper."""

    def test_returns_delta_config(self, config_file: str) -> None:
        config = load_config(config_file)
        delta = get_delta_config(config)
        assert isinstance(delta, DeltaConfig)
        assert delta.bronze_path == "/data/test/bronze"

    def test_default_values_on_empty(self) -> None:
        delta = get_delta_config({})
        assert delta.bronze_path == "/data/delta/bronze"
