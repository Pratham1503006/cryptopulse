# 🚀 CryptoPulse

> **A production-inspired data engineering project built to understand how modern real-time streaming platforms actually work.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache-Spark-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-4169E1.svg)](https://www.postgresql.org/)
[![Grafana](https://img.shields.io/badge/Grafana-F46800.svg)](https://grafana.com/)
[![Prometheus](https://img.shields.io/badge/Prometheus-E6522C.svg)](https://prometheus.io/)

<p align="center">
  <img src="docs/assets/images/architecture/overview.png" alt="CryptoPulse Architecture">
</p>



## Why I built this

Most data engineering portfolio projects prove that data can move from an API to a dashboard.

I became much more interested in everything that happens before that.

How do you continuously ingest streaming data? What happens when the connection drops? How do you validate events before they reach analytics? How do you monitor the health of a streaming pipeline? How do you build something another engineer can actually reproduce?

CryptoPulse is my attempt to answer those questions by building the platform instead of only reading about it.

The project uses live cryptocurrency market data because it's freely available and produces a continuous stream of real-time events. The engineering ideas behind the platform aren't specific to crypto—they're the same kinds of problems you'd encounter when working with application logs, IoT sensors, financial transactions, manufacturing telemetry, or other streaming systems.



## What is CryptoPulse?

CryptoPulse is an end-to-end streaming data platform that continuously ingests live market events, processes them through an event-driven pipeline, validates and transforms them into analytics-ready datasets, and exposes both business insights and operational health.

The dashboard is simply the final consumer.

The pipeline itself is the interesting part.



## Features

* 📡 Consume live cryptocurrency market events using public WebSocket APIs
* ⚡ Stream events through Apache Kafka
* 🔄 Process data using Spark Structured Streaming
* ✅ Validate events and isolate invalid records using a Dead Letter Queue
* 🥉🥈🥇 Organize data using a Bronze → Silver → Gold (Medallion) architecture
* 🗄️ Store curated datasets in PostgreSQL
* 📊 Build business dashboards using Power BI
* 📈 Monitor pipeline health with Prometheus and Grafana
* 🐳 Run the complete platform locally with Docker Compose
* 📖 Document the engineering decisions behind the implementation



## Architecture

The complete architecture, component interactions, and data flow are documented under **`docs/architecture/`**.

```text
Exchange Connector
        │
        ▼
Python Producer
        │
        ▼
Apache Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
Validation
        │
        ▼
Bronze → Silver → Gold
        │
        ▼
PostgreSQL
   ┌──────────┴──────────┐
   ▼                     ▼
Power BI             Grafana
```

---

## Technology Stack

| Category              | Technology                        |
| --------------------- | --------------------------------- |
| Language              | Python 3.11+                      |
| Streaming             | Apache Kafka                      |
| Stream Processing     | Apache Spark Structured Streaming |
| Storage               | PostgreSQL                        |
| File Format           | Apache Parquet                    |
| Infrastructure        | Docker & Docker Compose           |
| Monitoring            | Prometheus, Grafana               |
| Analytics             | SQL, Pandas                       |
| Business Intelligence | Power BI                          |
| Testing               | Pytest                            |



## Repository Structure

```text
cryptopulse/
│
├── producer/              # Exchange connectors & Kafka producer
├── spark/                 # Stream processing jobs
├── warehouse/             # Database schema & persistence
├── analytics/             # SQL models & business metrics
├── monitoring/            # Prometheus & Grafana
├── docker/                # Docker configuration
├── tests/                 # Unit, integration & end-to-end tests
│
├── docs/
│   ├── overview/
│   ├── architecture/
│   ├── engineering/
│   ├── data/
│   └── adr/
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```



## Quick Start

### Prerequisites

* Docker & Docker Compose
* Python 3.11+

### Clone the repository

```bash
git clone https://github.com/<your-username>/cryptopulse.git

cd cryptopulse
```

### Configure the environment

```bash
cp configs/.env.example .env
```

### Install Python dependencies

```bash
pip install -r requirements.txt
```

### Start the infrastructure

```bash
docker compose up -d
```

### Start the streaming pipeline

```bash
make run-producer

make run-spark
```

Once everything is running:

| Service               | URL                   |
| --------------------- | --------------------- |
| Grafana               | http://localhost:3000 |
| Spark UI              | http://localhost:4040 |
| Kafka UI *(optional)* | http://localhost:8080 |



## Documentation

One of the goals of this repository is to document the engineering process—not just the finished implementation.

The documentation is organised by responsibility.

| Directory            | Description                                                              |
| -------------------- | ------------------------------------------------------------------------ |
| `docs/overview/`     | Project overview, design goals, scope, and roadmap                       |
| `docs/architecture/` | System architecture, deployment, components, and data flow               |
| `docs/engineering/`  | Development workflow, testing, monitoring, deployment, and operations    |
| `docs/data/`         | Schemas, validation rules, Medallion architecture, and analytical models |
| `docs/adr/`          | Architecture Decision Records documenting important design choices       |

If you're exploring the repository for the first time, I'd recommend starting with **`docs/overview/`** before diving into the implementation.


## Current Status

🚧 **Version 1 — In Development**

The current focus is building a complete streaming platform that can:

* continuously ingest live events
* validate streaming data
* process events in real time
* produce analytics-ready datasets
* expose operational metrics
* be reproduced by another developer using only the documentation

The implementation boundaries for Version 1 are documented in **`docs/overview/project-scope.md`**.

## Contributing

Suggestions, discussions, and improvements are always welcome.

If you'd like to contribute:

1. Fork the repository.
2. Create a feature branch.
3. Make your changes.
4. Add or update tests where appropriate.
5. Open a pull request with a clear description of what changed and why.



## License

This project is licensed under the **MIT License**.

See the **LICENSE** file for details.



<p align="center">
Built while learning how modern streaming data platforms are engineered—from ingestion to analytics.
</p>
