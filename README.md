# End-to-End Banking Datastack

A fully containerised, real-time data engineering stack simulating a banking data platform. Covers the complete data journey from transactional Postgres → CDC with Debezium → Kafka streaming → data lake (MinIO) → Snowflake → dbt transformations → Airflow orchestration.

---

## Architecture

```
PostgreSQL 15 (source)
      │
      │  pgoutput (logical replication)
      ▼
Debezium Connect 2.2
      │
      │  CDC events
      ▼
Kafka (Confluent 7.4) + Schema Registry
      │
      │  Python consumer
      ▼
MinIO (S3-compatible raw lake)
      │
      │  COPY INTO / Snowpipe
      ▼
Snowflake (cloud warehouse)
      │
      │  dbt-snowflake
      ▼
dbt Models (staging → marts)
      │
      │  DAGs
      ▼
Apache Airflow 2.9.3
```

All components run via **Docker Compose** on a single host.

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Source DB | PostgreSQL | 15 |
| CDC | Debezium Connect | 2.2 |
| Message Broker | Apache Kafka (Confluent) | 7.4 |
| Schema Registry | Confluent Schema Registry | 7.4 |
| Data Lake | MinIO (S3-compatible) | latest |
| Consumer | Python | 3.11 |
| Cloud Warehouse | Snowflake | — |
| Transformation | dbt-core + dbt-snowflake | — |
| Orchestration | Apache Airflow | 2.9.3 |
| Containerisation | Docker + Docker Compose | 24+ |

---

## Project Structure

```
End-to-End_Banking_datastack/
├── docker-compose.yml          # All services defined here
├── .env.example                # Environment variable template
├── run_pipeline.sh             # One-shot pipeline runner
├── data-generator/             # Synthetic banking transaction generator
│   ├── generate_data.py
│   └── requirements.txt
├── kafka-consumer/             # Python consumer: Kafka → MinIO
│   ├── consumer.py
│   └── requirements.txt
├── debezium/
│   └── register-connector.sh  # Registers the Postgres CDC connector
├── dbt/                        # dbt project (Snowflake target)
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/            # Raw Snowflake → typed/cleaned
│   │   └── marts/              # Business-level aggregations
│   └── tests/
└── airflow/
    └── dags/                   # Airflow DAGs (orchestration)
```

---

## Services & Ports

| Service | Port | Description |
|---|---|---|
| PostgreSQL | 5432 | Source transactional database |
| Kafka Broker | 9092 | Message broker |
| Schema Registry | 8081 | Avro/JSON schema store |
| Kafka Connect (Debezium) | 8083 | CDC connector REST API |
| MinIO | 9000 / 9001 | S3-compatible object store + console |
| Airflow Webserver | 8080 | DAG management UI |
| Airflow Scheduler | — | Background DAG runner |

---

## Prerequisites

- **Docker** 24+ and **Docker Compose** v2
- **16 GB RAM** recommended (8 containers running simultaneously)
- **Snowflake** account with a warehouse and database provisioned
- Python 3.11+ (for local development only — containers handle runtime)

---

## Setup

### 1 — Configure environment

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials and MinIO secrets
```

`.env` template:

```env
# Postgres
POSTGRES_USER=banking_user
POSTGRES_PASSWORD=banking_pass
POSTGRES_DB=banking_db

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Snowflake
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_DATABASE=BANKING
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_SCHEMA=RAW
```

### 2 — Start all services

```bash
docker compose up -d
```

Wait ~60 seconds for Kafka and Connect to fully initialise.

### 3 — Register the Debezium connector

```bash
bash debezium/register-connector.sh
```

### 4 — Run the pipeline

```bash
bash run_pipeline.sh
```

This generates synthetic transactions, streams them through Kafka, lands them in MinIO, and loads them into Snowflake.

---

## Data Flow (Step by Step)

1. **Data Generator** inserts synthetic banking transactions into PostgreSQL
2. **Debezium** captures row-level changes via PostgreSQL logical replication (`pgoutput`)
3. **Kafka** receives CDC events as topics (one topic per table)
4. **Python Consumer** reads from Kafka topics and writes Parquet files to MinIO
5. **MinIO** acts as the raw data lake (S3-compatible bucket)
6. **Snowflake** ingests files from MinIO via `COPY INTO` or Snowpipe
7. **dbt** runs staging and mart models on top of the raw Snowflake tables
8. **Airflow** orchestrates the full pipeline on a schedule

---

## dbt Project

```bash
# Install dependencies
pip install dbt-snowflake

# Configure ~/.dbt/profiles.yml with your Snowflake credentials

# Run transformations
dbt deps
dbt run
dbt test
```

---

## Branches

| Branch | Purpose |
|---|---|
| `main` | Stable release |
| `dev` | Active development (CI workflow added) |

---

## Possible Improvements

- Add Kafka Connect S3 Sink connector to bypass the Python consumer
- Use Snowpipe for continuous file ingestion from MinIO
- Add Great Expectations for data quality checks before dbt
- Containerise the dbt runner and add it as an Airflow operator
- Add Grafana + Prometheus monitoring for Kafka lag
