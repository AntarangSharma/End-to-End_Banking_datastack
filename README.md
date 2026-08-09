# End-to-End Banking Datastack

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL_15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Debezium](https://img.shields.io/badge/Debezium-Connect_2.2-D32F2F)](https://debezium.io/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-C72E49?logo=minio&logoColor=white)](https://min.io/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Apache_Airflow_2.9-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)

## Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Services \& Ports](#services--ports)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Data Flow](#data-flow-step-by-step)
- [dbt Project](#dbt-project)
- [Branches](#branches)

---

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
Kafka (Confluent 7.4)
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
| Coordination | ZooKeeper (Confluent) | 7.4 |
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
├── docker-compose.yml                 # All services defined here
├── dockerfile-airflow.dockerfile      # Airflow image (webserver + scheduler)
├── .env.example                       # Environment variable template
├── requirements.txt                   # Python deps for the local scripts
├── run_pipeline.sh                    # Runs consumer + generator together
├── data-generator/
│   └── fake_generator.py              # Synthetic banking transactions → Postgres
├── consumer/
│   ├── kafka_to_minio.py              # Kafka → MinIO landing
│   └── list_topics.py                 # Debug helper: list Kafka topics
├── kafka-debezium/
│   └── generate_and_post_connector.py # Registers the Postgres CDC connector
├── postgres/
│   └── schema.sql                     # customers / accounts / transactions DDL
├── banking_dbt/                       # dbt project (Snowflake target)
│   ├── dbt_project.yml
│   ├── models/                        # staging → marts
│   └── snapshots/
├── docker/
│   └── dags/                          # Airflow DAGs (mounted into containers)
└── .github/workflows/
    └── ci.yml
```

> `docker/` also holds runtime state mounted by Compose (`docker/logs`, `docker/plugins`,
> `docker/postgres/data`, `docker/minio/data`). These are created on first run.

---

## Services & Ports

| Service | Port | Description |
|---|---|---|
| PostgreSQL (banking) | 5432 | Source transactional database |
| ZooKeeper | 2181 | Kafka coordination |
| Kafka Broker | 9092 | Internal listener (Docker network) |
| Kafka Broker | 29092 | Host listener — use this from your machine |
| Kafka Connect (Debezium) | 8083 | CDC connector REST API |
| MinIO | 9000 / 9001 | S3-compatible object store + console |
| Airflow Webserver | 8080 | DAG management UI |
| Airflow Scheduler | — | Background DAG runner |
| PostgreSQL (Airflow metadata) | 5433 | Separate from the banking DB |

---

## Prerequisites

- **Docker** 24+ and **Docker Compose** v2
- **16 GB RAM** recommended (8 containers running simultaneously)
- **Snowflake** account with a warehouse and database provisioned
- Python 3.11+ — the consumer, generator, and connector scripts run on the host

---

## Setup

### 1 — Configure environment

```bash
cp .env.example .env
# Edit .env: Postgres, Kafka, MinIO, and Airflow metadata-DB settings
```

Every variable in `.env.example` is read by `docker-compose.yml` or by one of the
Python scripts. Compose will start with blank credentials if they are missing, so
fill the file in before the first run.

### 2 — Install the Python dependencies

```bash
pip install -r requirements.txt
```

### 3 — Start all services

```bash
docker compose up -d
```

Wait ~60 seconds for Kafka and Connect to fully initialise.

### 4 — Load the source schema

```bash
psql -h localhost -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f postgres/schema.sql
```

### 5 — Register the Debezium connector

```bash
python kafka-debezium/generate_and_post_connector.py
```

This POSTs the connector config to Kafka Connect at `http://localhost:8083/connectors`,
capturing `public.customers`, `public.accounts`, and `public.transactions` under the
`banking_server` topic prefix.

### 6 — Run the pipeline

```bash
bash run_pipeline.sh
```

This starts the synthetic transaction generator and the Kafka → MinIO consumer
together, and stops both on `Ctrl-C`.

---

## Data Flow (Step by Step)

1. **Data Generator** inserts synthetic banking transactions into PostgreSQL
2. **Debezium** captures row-level changes via PostgreSQL logical replication (`pgoutput`)
3. **Kafka** receives CDC events as topics (one topic per table)
4. **Python Consumer** reads from Kafka topics and writes the events to MinIO
5. **MinIO** acts as the raw data lake (S3-compatible bucket)
6. **Snowflake** ingests files from MinIO via `COPY INTO` or Snowpipe
7. **dbt** runs staging and mart models on top of the raw Snowflake tables
8. **Airflow** orchestrates the full pipeline on a schedule

---

## dbt Project

The dbt project lives in [`banking_dbt/`](banking_dbt). Compose mounts it into the
Airflow containers at `/opt/airflow/banking_dbt`, with the profile directory taken
from `banking_dbt/.dbt`.

```bash
# Install dependencies
pip install dbt-snowflake

# Configure banking_dbt/.dbt/profiles.yml with your Snowflake credentials
# (this directory is git-ignored — never commit credentials)

cd banking_dbt
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
