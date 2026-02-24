"""Configuration loader for the streaming pipeline.

Loads YAML configuration from disk and provides typed access
to Kafka, Spark, Delta Lake, and quality settings.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class KafkaTopics:
    """Kafka topic configuration."""

    clickstream: str = "clickstream-events"
    cdc: str = "cdc-events"
    dlq: str = "dead-letter-queue"


@dataclass
class KafkaConfig:
    """Kafka connection and topic settings."""

    bootstrap_servers: str = "localhost:9092"
    topics: KafkaTopics = field(default_factory=KafkaTopics)


@dataclass
class SparkConfig:
    """Spark session configuration."""

    app_name: str = "StreamingPipeline"
    checkpoint_location: str = "/tmp/checkpoints"


@dataclass
class DeltaConfig:
    """Delta Lake path configuration."""

    bronze_path: str = "/data/delta/bronze"
    silver_path: str = "/data/delta/silver"
    gold_path: str = "/data/delta/gold"


def load_config(path: str = "configs/config.yaml") -> dict[str, Any]:
    """Load configuration from a YAML file.

    Args:
        path: Path to the YAML configuration file. Defaults to
            ``configs/config.yaml``.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the YAML file is malformed.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Allow environment variable overrides
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_servers:
        config.setdefault("kafka", {})["bootstrap_servers"] = kafka_servers

    return config


def get_kafka_config(config: dict[str, Any]) -> KafkaConfig:
    """Extract Kafka configuration from the full config dictionary.

    Args:
        config: Full configuration dictionary loaded from YAML.

    Returns:
        Typed KafkaConfig dataclass instance.
    """
    kafka = config.get("kafka", {})
    topics_raw = kafka.get("topics", {})
    topics = KafkaTopics(
        clickstream=topics_raw.get("clickstream", "clickstream-events"),
        cdc=topics_raw.get("cdc", "cdc-events"),
        dlq=topics_raw.get("dlq", "dead-letter-queue"),
    )
    return KafkaConfig(
        bootstrap_servers=kafka.get("bootstrap_servers", "localhost:9092"),
        topics=topics,
    )


def get_spark_config(config: dict[str, Any]) -> SparkConfig:
    """Extract Spark configuration from the full config dictionary.

    Args:
        config: Full configuration dictionary loaded from YAML.

    Returns:
        Typed SparkConfig dataclass instance.
    """
    spark = config.get("spark", {})
    return SparkConfig(
        app_name=spark.get("app_name", "StreamingPipeline"),
        checkpoint_location=spark.get("checkpoint_location", "/tmp/checkpoints"),
    )


def get_delta_config(config: dict[str, Any]) -> DeltaConfig:
    """Extract Delta Lake configuration from the full config dictionary.

    Args:
        config: Full configuration dictionary loaded from YAML.

    Returns:
        Typed DeltaConfig dataclass instance.
    """
    delta = config.get("delta", {})
    return DeltaConfig(
        bronze_path=delta.get("bronze_path", "/data/delta/bronze"),
        silver_path=delta.get("silver_path", "/data/delta/silver"),
        gold_path=delta.get("gold_path", "/data/delta/gold"),
    )
