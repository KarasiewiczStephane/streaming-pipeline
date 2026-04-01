# Real-Time Streaming Pipeline

> Kafka + PySpark + Delta Lake streaming architecture — medallion pattern processing of e-commerce clickstream events with exactly-once semantics.

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

- **Event Producer** -- Simulated e-commerce clickstream with Markov chain transitions and cart abandonment modeling
- **CDC Pipeline** -- Debezium-style change data capture with SCD Type 2 user dimension tracking
- **Medallion Architecture** -- Bronze (raw) -> Silver (cleaned/deduped) -> Gold (aggregated) data layers on Delta Lake
- **Exactly-Once Semantics** -- Spark Structured Streaming with checkpointing and idempotent Delta writes
- **Data Quality Framework** -- Per-layer quality checks with threshold alerting and SQLite metric persistence
- **Dead Letter Queue** -- Schema-invalid events routed to a DLQ topic for inspection
- **Backfill Capability** -- Replay events from any Kafka offset or timestamp through all layers
- **Real-Time Dashboard** -- Streamlit UI with throughput, quality scores, and business metrics (conversion rate, top products)

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Java 21+ (for Spark -- set `JAVA_HOME`)

### 1. Install dependencies

```bash
git clone https://github.com/KarasiewiczStephane/streaming-pipeline.git
cd streaming-pipeline
make install
```

### 2. Start Kafka (Docker required)

```bash
docker compose -f docker/docker-compose.yml up -d zookeeper kafka kafka-init
```

Wait until `kafka-init` exits successfully (topics are created automatically):

```bash
docker compose -f docker/docker-compose.yml ps
```

### 3. Run the pipeline

Open two terminals from the project root:

```bash
# Terminal 1 -- start the event producer
python -m src.main --mode producer

# Terminal 2 -- start the Spark streaming consumer
python -m src.main --mode consumer
```

The producer generates clickstream events and pushes them to Kafka. The consumer reads from Kafka, writes through the Bronze -> Silver -> Gold medallion layers in Delta Lake, and logs quality metrics to `data/quality_metrics.db`.

### 4. Launch the dashboard

```bash
make dashboard
# Opens at http://localhost:8501
```

The dashboard reads from the SQLite quality database (`data/quality_metrics.db`) and shows four pages: **Overview**, **Throughput**, **Data Quality**, and **Business Metrics**.

### Run everything with Docker (alternative)

To start all services (Kafka, Spark, Producer, Consumer, Dashboard) in containers:

```bash
make docker-up

# View logs
make docker-logs

# Access dashboard at http://localhost:8501
# Access Spark UI at http://localhost:8080

# Stop services
make docker-down
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
  batch_size: 100
  num_users: 1000

spark:
  app_name: "StreamingPipeline"
  checkpoint_location: "/tmp/checkpoints"

delta:
  bronze_path: "/data/delta/bronze"
  silver_path: "/data/delta/silver"
  gold_path: "/data/delta/gold"

quality:
  db_path: "data/quality_metrics.db"
  thresholds:
    bronze_schema: 0.99
    bronze_null: 0.99
    silver_dedup: 1.0
    silver_timestamp: 0.99
    gold_completeness: 1.0
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
├── tests/                # pytest unit and integration tests
├── docker/               # Docker Compose and Dockerfiles
├── configs/              # YAML configuration
├── data/                 # Local data (quality_metrics.db)
├── .github/workflows/    # CI pipeline (lint + test + integration)
├── Makefile
└── requirements.txt
```

## Makefile Targets

| Target | Description |
|---|---|
| `make install` | Install Python dependencies |
| `make test` | Run unit tests with coverage |
| `make test-all` | Run all tests including integration |
| `make lint` | Lint and format with Ruff |
| `make clean` | Remove caches and build artifacts |
| `make run` | Run pipeline in default (all) mode |
| `make dashboard` | Launch Streamlit dashboard on port 8501 |
| `make docker-up` | Start all services in Docker |
| `make docker-down` | Stop and remove containers |
| `make docker-logs` | Tail container logs |
| `make docker-ps` | Show running containers |

## Troubleshooting

| Issue | Solution |
|---|---|
| Kafka connection refused | Ensure Kafka container is running: `make docker-ps` |
| Spark OOM | Increase `SPARK_WORKER_MEMORY` in `docker/docker-compose.yml` |
| Dashboard shows no data | Check consumer logs; verify Delta paths in `configs/config.yaml` |
| Java not found | Install JDK 21+ and set `JAVA_HOME` |
| Delta Lake not found | Ensure `delta-spark` is installed: `pip install delta-spark` |


## Author

**Stéphane Karasiewicz** — [skarazdata.com](https://skarazdata.com) | [LinkedIn](https://www.linkedin.com/in/stephane-karasiewicz/)

## License

MIT
