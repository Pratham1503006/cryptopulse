# 🚀 CryptoPulse

**A Production-Grade Real-Time Cryptocurrency Market Intelligence Platform**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache-Kafka-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache-Spark-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

CryptoPulse is an end-to-end **real-time data engineering, analytics engineering, and data science** platform. It continuously ingests cryptocurrency market events, processes them through a scalable streaming architecture, validates data quality, stores historical and analytical datasets using a Medallion Architecture (Bronze → Silver → Gold), and delivers actionable business insights through interactive dashboards.

Unlike typical portfolio projects that stop at streaming data into Kafka, CryptoPulse is designed as a **production-inspired analytics platform** demonstrating software engineering, data engineering, analytics engineering, and data science best practices — together, end to end.

---

## Table of Contents

- [Business Problem](#-business-problem)
- [Project Objectives](#-project-objectives)
- [System Architecture](#-system-architecture)
- [Core Features](#-core-features)
- [Technology Stack](#-technology-stack)
- [Project Structure](#-project-structure)
- [Data Architecture](#-data-architecture)
- [Business KPIs](#-business-kpis)
- [Data Quality](#-data-quality)
- [Monitoring](#-monitoring)
- [Getting Started](#-getting-started)
- [Testing Strategy](#-testing-strategy)
- [Agile Development](#-agile-development)
- [Documentation](#-documentation)
- [Learning Outcomes](#-learning-outcomes)
- [Future Enhancements](#-future-enhancements)
- [License](#-license)
- [Author](#-author)

---

## 📌 Business Problem

Cryptocurrency markets generate thousands of trades every second. Analysts, traders, and fintech companies often struggle to answer questions such as:

- Which assets are becoming unusually volatile?
- Is trading volume increasing abnormally?
- Which markets require immediate attention?
- Are there unusual trading patterns indicating market manipulation?
- How has today's activity changed compared to historical trends?

Traditional dashboards primarily visualize prices but provide limited operational intelligence. CryptoPulse addresses this by continuously collecting live market events, transforming raw data into business-ready datasets, and producing actionable insights in real time.

---

## 🎯 Project Objectives

The project demonstrates an end-to-end production analytics platform by implementing:

- Real-time event ingestion
- Distributed stream processing
- Data quality validation
- Medallion data architecture
- Analytical data warehouse
- Business intelligence dashboards
- Operational monitoring
- Production-ready software engineering practices

### Target Users

| Audience | Use Case |
|---|---|
| Data Engineers | Reference architecture for streaming pipelines |
| Analytics Engineers | Medallion modeling and warehouse design |
| Data Scientists | Time-series feature engineering and anomaly detection |
| Market Analysts | Live and historical trading insight |
| Trading Teams | Operational and volatility signals |
| FinTech Startups | Production-inspired blueprint |
| Engineering Managers | Delivery process and architecture patterns |

### Business Value

- Monitor live market conditions
- Detect abnormal trading activity
- Analyze historical trading behavior
- Build business KPIs in real time
- Reduce manual data processing
- Improve operational visibility
- Support data-driven trading decisions

---

## 🏗 System Architecture

```text
                 Exchange Connector
        (Coinbase / Binance / Kraken)
                       │
                       ▼
             Python Streaming Producer
                       │
                       ▼
                 Apache Kafka Cluster
                       │
                       ▼
          Spark Structured Streaming
                       │
         ┌─────────────┴─────────────┐
         ▼                           ▼
  Data Validation              Dead Letter Queue
         │
         ▼
  Bronze Data Layer (Raw)
         │
         ▼
  Silver Data Layer (Validated)
         │
         ▼
  Gold Data Layer (Business Metrics)
         │
         ▼
  PostgreSQL Data Warehouse
         │
         ├──────────────┐
         ▼              ▼
  Business Analytics   Operational Metrics
         │              │
         ▼              ▼
    Power BI         Grafana
```

---

## ✨ Core Features

### Real-Time Data Streaming
- Live cryptocurrency market ingestion
- WebSocket streaming with automatic reconnection
- Fault tolerance and retry strategy
- Configurable exchange connectors

### Data Engineering
- Apache Kafka event streaming
- Spark Structured Streaming
- Event validation and schema enforcement
- Window aggregations and feature engineering
- Dead Letter Queue for invalid records
- Bronze / Silver / Gold architecture

### Data Storage
- Bronze Layer (raw)
- Silver Layer (validated)
- Gold Layer (business-ready)
- PostgreSQL data warehouse
- Historical replay capability

### Business Analytics
Computed metrics include trading volume, VWAP, moving average, price momentum, volatility, trade frequency, liquidity metrics, market trends, and rolling aggregations.

### Data Science
- Statistical anomaly detection
- Volatility detection
- Trend analysis
- Time-series feature engineering
- Market event identification

### Dashboards

| Dashboard | Contents |
|---|---|
| **Executive** | Market overview, trading volume, top gainers/losers, market health |
| **Analyst** | Candlestick charts, VWAP, volatility, rolling averages, trading distribution |
| **Engineering** | Kafka health, consumer lag, pipeline status, data quality, system metrics |

---

## 🛠 Technology Stack

| Category | Tools |
|---|---|
| Programming | Python |
| Streaming | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Storage | PostgreSQL, Apache Iceberg, Parquet |
| Infrastructure | Docker, Docker Compose |
| Analytics | SQL, Pandas |
| Monitoring | Grafana, Prometheus |
| Business Intelligence | Power BI |
| Version Control | Git, GitHub |

---

## 📂 Project Structure

```text
cryptopulse/
├── configs/
├── producer/
├── consumer/
├── spark/
├── warehouse/
├── analytics/
├── dashboard/
├── monitoring/
├── docker/
├── docs/
│   ├── architecture/
│   ├── diagrams/
│   ├── adr/
│   └── reports/
├── scripts/
├── tests/
├── .github/
├── docker-compose.yml
├── requirements.txt
├── Makefile
└── README.md
```

---

## 🏛 Data Architecture

CryptoPulse follows the **Medallion Architecture**.

### Bronze Layer — Raw
Raw, immutable market events, kept for historical archive, replay capability, and auditability.

### Silver Layer — Validated
Cleaned and validated data: schema validation, deduplication, missing-value handling, and data quality checks.

### Gold Layer — Business Ready
Business-ready datasets, including minute candles, hourly aggregations, daily metrics, trading KPIs, and analytical tables.

---

## 📈 Business KPIs

The platform continuously computes:

- Average price
- Trading volume
- Trades per minute
- VWAP
- Moving average
- Volatility index
- Largest trades
- Market momentum
- Liquidity indicators

---

## 🔍 Data Quality

Every incoming event is validated for:

- Required fields
- Schema compliance
- Duplicate events
- Invalid timestamps
- Invalid prices
- Missing values
- Negative quantities

Invalid records are redirected to a **Dead Letter Queue** for investigation.

---

## 📊 Monitoring

The platform exposes operational metrics including:

- Producer health
- Kafka consumer lag
- Messages per second
- Failed messages
- Processing latency
- Data quality score
- Storage utilization
- Spark job status

---

## ⚡ Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Make (optional, for convenience commands)

### Setup

```bash
# Clone the repository
git clone https://github.com/<your-username>/cryptopulse.git
cd cryptopulse

# Copy and configure environment variables
cp configs/.env.example .env

# Install Python dependencies
pip install -r requirements.txt

# Start the full stack (Kafka, Spark, PostgreSQL, Grafana, Prometheus)
docker compose up -d

# Verify services are healthy
docker compose ps
```

### Running the Pipeline

```bash
# Start the exchange connector + producer
make run-producer

# Start the Spark Structured Streaming job
make run-spark

# View dashboards
# Grafana:   http://localhost:3000
# Power BI:  connect to the PostgreSQL warehouse
```

> Update the commands above to match your actual `Makefile` targets and service ports once implemented.

---

## 🧪 Testing Strategy

- Unit tests
- Integration tests
- End-to-end tests
- Data validation tests
- Pipeline verification
- Replay testing

---

## 📅 Agile Development

Development follows a Scrum-inspired sprint structure:

| Sprint | Focus |
|---|---|
| Sprint 0 | Product Discovery |
| Sprint 1 | Infrastructure |
| Sprint 2 | Streaming Ingestion |
| Sprint 3 | Stream Processing |
| Sprint 4 | Data Lake & Warehouse |
| Sprint 5 | Analytics & Data Science |
| Sprint 6 | Business Intelligence |
| Sprint 7 | Production Readiness |
| Sprint 8 | Documentation & Portfolio |

---

## 📚 Documentation

The `/docs` directory contains:

- Product Requirements Document (PRD)
- Architecture diagrams
- Data flow diagrams
- Sequence diagrams
- Deployment architecture
- ADRs (Architecture Decision Records)
- Data dictionary
- Business reports
- Sprint documentation

---

## 🎯 Learning Outcomes

This project demonstrates practical experience with:

- Data engineering
- Streaming systems
- Analytics engineering
- Data warehousing
- Data quality
- Distributed processing
- Software engineering
- Business intelligence
- Data science
- Production monitoring
- Agile development

---

## 🔮 Future Enhancements

- [ ] Multi-exchange support
- [ ] Machine learning-based anomaly detection
- [ ] Real-time alerting
- [ ] Stream processing with Apache Flink
- [ ] Kubernetes deployment
- [ ] Cloud-native deployment (AWS/Azure/GCP)
- [ ] dbt transformation layer
- [ ] Data lineage with OpenMetadata
- [ ] Feature store integration

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## 👨‍💻 Author

**Pratham Gavadia**
Computer Science Engineering Student | Data Engineering | Data Science | Machine Learning | AI Systems

---

> CryptoPulse is a production-inspired streaming analytics platform built to demonstrate real-world data engineering, analytics engineering, and data science workflows using modern open-source technologies.
