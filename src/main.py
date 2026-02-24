"""Main entry point for the streaming pipeline.

Supports running in producer, consumer, or full pipeline mode
based on command-line arguments.
"""

import argparse
import logging

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments with mode selection.
    """
    parser = argparse.ArgumentParser(description="Streaming Pipeline")
    parser.add_argument(
        "--mode",
        choices=["producer", "consumer", "all"],
        default="all",
        help="Pipeline component to run",
    )
    parser.add_argument(
        "--config",
        default="configs/config.yaml",
        help="Path to configuration file",
    )
    return parser.parse_args()


def main() -> None:
    """Run the streaming pipeline based on selected mode."""
    args = parse_args()
    config = load_config(args.config)

    log_level = getattr(logging, config.get("logging", {}).get("level", "INFO"))
    logger.setLevel(log_level)

    logger.info("Starting streaming pipeline in %s mode", args.mode)

    if args.mode == "producer":
        from src.producer.event_generator import EventGenerator
        from src.producer.kafka_producer import StreamingProducer

        generator = EventGenerator(
            events_per_second=config["producer"]["events_per_second"],
            num_users=config["producer"]["num_users"],
        )
        producer = StreamingProducer(args.config)
        try:
            for event in generator.stream_events():
                producer.send_clickstream_event(event)
        except KeyboardInterrupt:
            producer.flush()
            producer.close()

    elif args.mode == "consumer":
        from src.consumer.spark_consumer import SparkStreamingConsumer

        consumer = SparkStreamingConsumer(args.config)
        bronze_query = consumer.run_bronze_pipeline()
        bronze_query.awaitTermination()

    logger.info("Pipeline stopped")


if __name__ == "__main__":
    main()
