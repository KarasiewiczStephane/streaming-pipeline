# Real-Time Streaming Pipeline

A scalable streaming architecture using Apache Kafka, PySpark Structured Streaming, and Delta Lake for processing e-commerce clickstream events with exactly-once semantics.

## Architecture

```
┌──────────────┐     ┌─────────────┐     ┌──────────────────────────────────┐
│   Producer   │────>│    Kafka    │────>│     Spark Streaming Consumer     │
│  (Events +   │     │  (Topics)   │     │                                  │
│   CDC)       │     └─────────────┘     │  ┌────────┐ ┌────────┐ ┌──────┐ │
└──────────────┘            │            │  │ Bronze │>│ Silver │>│ Gold │ │
                            │            │  └────────┘ └────────┘ └──────┘ │
                     ┌──────v──────┐     └──────────────────────────────────┘
                     │     DLQ     │                    │
                     └─────────────┘                    v
                                         ┌──────────────────────────────────┐
                                         │          Delta Lake              │
                                         │   (Bronze -> Silver -> Gold)     │
                                         └──────────────────────────────────┘
                                                        │
                                                        v
                                         ┌──────────────────────────────────┐
                                         │     Streamlit Dashboard          │
                                         │  (Metrics, Quality, Business)    │
                                         └──────────────────────────────────┘
```

## Features

- **Event Producer** — Simulated e-commerce clickstream with Markov chain transitions and cart abandonment modeling
- **CDC Pipeline** — Debezium-style change data capture with SCD Type 2 user dimension tracking
- **Medallion Architecture** — Bronze (raw) -> Silver (cleaned/deduped) -> Gold (aggregated) data layers on Delta Lake
- **Exactly-Once Semantics** — Spark Structured Streaming with checkpointing and idempotent Delta writes
- **Data Quality Framework** — Per-layer quality checks with threshold alerting and SQLite metric persistence
- **Dead Letter Queue** — Schema-invalid events routed to a DLQ topic for inspection
- **Backfill Capability** — Replay events from any Kafka offset or timestamp through all layers
- **Real-Time Dashboard** — Streamlit UI with throughput, quality scores, and business metrics (conversion rate, top products)

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Java 21+ (for Spark)

### Run with Docker

```bash
git clone https://github.com/KarasiewiczStephane/streaming-pipeline.git
cd streaming-pipeline

# Start all services (Kafka, Spark, Producer, Consumer, Dashboard)
make docker-up

# View logs
make docker-logs

# Access dashboard at http://localhost:8501
# Access Spark UI at http://localhost:8080

# Stop services
make docker-down
```

### Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Start Kafka (requires Docker)
docker compose -f docker/docker-compose.yml up -d zookeeper kafka kafka-init

# Run producer (terminal 1)
python -m src.main --mode producer

# Run consumer (terminal 2)
python -m src.main --mode consumer

# Run dashboard (terminal 3)
streamlit run src/dashboard/app.py
```

## Configuration

All settings live in `configs/config.yaml`:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topics:
    clickstream: "clickstream-events"
    cdc: "cdc-events"
    dlq: "dead-letter-queue"

producer:
  events_per_second: 100
  num_users: 1000

delta:
  bronze_path: "/data/delta/bronze"
  silver_path: "/data/delta/silver"
  gold_path: "/data/delta/gold"
```

Environment variable overrides are supported (e.g., `KAFKA_BOOTSTRAP_SERVERS`).

## Project Structure

```
streaming-pipeline/
├── src/
│   ├── producer/         # Event generation, CDC, Kafka producer
│   │   ├── event_generator.py    # Clickstream with Markov chains
│   │   ├── cdc_generator.py      # Debezium-style CDC events
│   │   ├── kafka_producer.py     # Producer with DLQ routing
│   │   └── schemas.py            # JSON schema definitions
│   ├── consumer/         # Spark streaming and transformations
│   │   ├── spark_consumer.py     # Kafka -> Bronze layer
│   │   ├── transformations.py    # Silver layer transforms
│   │   ├── aggregations.py       # Gold layer aggregations
│   │   └── backfill.py           # Historical reprocessing
│   ├── storage/          # Delta Lake and CDC handlers
│   │   ├── delta_writer.py       # Delta Lake read/write utilities
│   │   └── cdc_handler.py        # SCD Type 2 dimension management
│   ├── quality/          # Data quality framework
│   │   └── checker.py            # Per-layer checks with SQLite logging
│   ├── dashboard/        # Monitoring UI
│   │   └── app.py                # Streamlit dashboard
│   └── utils/            # Config loader and structured logging
├── tests/
│   ├── unit/             # 173 unit tests (no Kafka required)
│   └── integration/      # Integration tests (require Kafka)
├── docker/               # Docker Compose and Dockerfiles
├── configs/              # YAML configuration and JSON schemas
├── .github/workflows/    # CI pipeline (lint + test + integration)
├── Makefile
└── requirements.txt
```

## Testing

```bash
# Run unit tests with coverage
make test

# Run all tests including integration
make test-all

# Lint and format
make lint
```

Current coverage: **93%** across 173 unit tests.

## Troubleshooting

| Issue | Solution |
|---|---|
| Kafka connection refused | Ensure Kafka container is running: `make docker-ps` |
| Spark OOM | Increase `SPARK_WORKER_MEMORY` in `docker/docker-compose.yml` |
| Dashboard shows no data | Check consumer logs; verify Delta paths in `configs/config.yaml` |
| Java not found | Install JDK 21+ and set `JAVA_HOME` |
| Delta Lake not found | Ensure `delta-spark` is installed: `pip install delta-spark` |

## License

MIT
