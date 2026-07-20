# 🚀 CryptoPulse

**Real-time cryptocurrency market intelligence platform**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache-Kafka-black.svg)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache-Spark-orange.svg)](https://spark.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

CryptoPulse ingests live cryptocurrency market events, streams them through Kafka and Spark Structured Streaming, validates and stores them using a Bronze → Silver → Gold (Medallion) architecture in PostgreSQL, and surfaces business and operational metrics through Power BI and Grafana.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Data Layers](#data-layers)
- [Metrics](#metrics)
- [Data Quality](#data-quality)
- [Monitoring](#monitoring)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Architecture

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
  Bronze Layer (Raw)
         │
         ▼
  Silver Layer (Validated)
         │
         ▼
  Gold Layer (Business Metrics)
         │
         ▼
  PostgreSQL Data Warehouse
         │
         ├──────────────┐
         ▼              ▼
  Power BI          Grafana
```

**Core components:**

- **Exchange connector** — WebSocket clients for Coinbase/Binance/Kraken with automatic reconnection and retry logic
- **Producer** — publishes normalized market events to Kafka
- **Spark Structured Streaming** — consumes, validates, and aggregates events in real time
- **Dead Letter Queue** — captures records that fail schema/data-quality checks
- **PostgreSQL warehouse** — stores Silver/Gold tables for analytics and BI

---

## Tech Stack

| Category | Tools |
|---|---|
| Language | Python |
| Streaming | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Storage | PostgreSQL, Apache Iceberg, Parquet |
| Infrastructure | Docker, Docker Compose |
| Analytics | SQL, Pandas |
| Monitoring | Grafana, Prometheus |
| BI | Power BI |

---

## Project Structure

```text
cryptopulse/
├── configs/          # environment & connector configs
├── producer/         # exchange connectors + Kafka producer
├── consumer/         # Kafka consumers
├── spark/            # Structured Streaming jobs (validation, aggregation)
├── warehouse/        # PostgreSQL schema, migrations
├── analytics/        # SQL models, KPI queries
├── dashboard/        # Power BI / Grafana dashboard definitions
├── monitoring/       # Prometheus config, Grafana provisioning
├── docker/           # Dockerfiles
├── docs/             # architecture, ADRs, data dictionary
├── scripts/          # utility scripts
├── tests/            # unit/integration/e2e tests
├── docker-compose.yml
├── requirements.txt
├── Makefile
└── README.md
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Make (optional)

### Setup

```bash
# Clone the repository
git clone https://github.com/<your-username>/cryptopulse.git
cd cryptopulse

# Copy and configure environment variables
cp configs/.env.example .env

# Install Python dependencies
pip install -r requirements.txt

# Start the stack (Kafka, Spark, PostgreSQL, Grafana, Prometheus)
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
```

- Grafana: `http://localhost:3000`
- Power BI: connect directly to the PostgreSQL warehouse

> Update commands, targets, and ports above to match your actual `Makefile` and `docker-compose.yml` once implemented.

---

## Configuration

Environment variables are defined in `configs/.env.example`. Key settings typically include:

```bash
# Exchange connector
EXCHANGE=coinbase
SYMBOLS=BTC-USD,ETH-USD

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=market-events

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=cryptopulse
POSTGRES_USER=cryptopulse
POSTGRES_PASSWORD=changeme
```

---

## Data Layers

CryptoPulse follows a Medallion architecture:

| Layer | Purpose | Contents |
|---|---|---|
| **Bronze** | Raw, immutable ingestion | Unmodified exchange events, kept for replay/audit |
| **Silver** | Validated, cleaned data | Schema-checked, deduplicated, missing values handled |
| **Gold** | Business-ready datasets | Minute candles, hourly/daily aggregations, trading KPIs |

---

## Metrics

Computed continuously from the Gold layer:

- Average price, trading volume, trades/minute
- VWAP, moving average
- Volatility index, market momentum
- Largest trades, liquidity indicators

---

## Data Quality

Every incoming event is validated for:

- Required fields and schema compliance
- Duplicate events
- Invalid timestamps or prices
- Missing values, negative quantities

Records that fail validation are routed to a Dead Letter Queue rather than dropped.

---

## Monitoring

Exposed operational metrics:

- Producer health, Kafka consumer lag
- Messages/sec, failed messages, processing latency
- Data quality score
- Storage utilization, Spark job status

---

## Testing

```bash
# Run the full test suite
make test

# Or with pytest directly
pytest tests/
```

Coverage includes unit tests, integration tests, end-to-end pipeline tests, data validation tests, and replay testing.

---

## Contributing

1. Fork the repo and create a feature branch
2. Follow the existing project structure and code style
3. Add/update tests for any new functionality
4. Open a pull request with a clear description of the change

---

## License

Licensed under the [MIT License](LICENSE).

---

<p align="center">Made with ❤️ by an aspiring developer</p>
