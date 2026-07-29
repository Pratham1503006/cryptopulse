# Repository Structure

## Why this document exists

The Architecture documents describe the logical design of CryptoPulse.

This document describes how that design maps to the repository.

Rather than explaining individual files or implementation details, it explains the purpose of every top-level directory, how they relate to each other, and where each architectural responsibility lives in the codebase.

## Repository Design Principles

These principles explain how the directory structure is organised and why it is organised that way.

### Directories follow architectural boundaries

Every directory maps to one or more closely related responsibilities from the architecture. For example, **processing/** implements both Validation Engine and Processing Engine because they execute as the same runtime. If a directory cannot be explained by the architecture, its purpose is probably unclear.

### Dependencies flow in one direction

Directories depend only on directories to their left in the dependency chain. A directory should never need to know about directories further downstream. This keeps the codebase modular and prevents implicit coupling.

### Technology choices are contained, not exposed

A directory is named after its architectural responsibility, not the technology used to implement it. This makes it possible to change technologies without reorganising the repository.

### Tests mirror the source structure

The tests directory mirrors the top-level layout so that finding tests for a given component requires no guesswork. The testing strategy itself is described separately.

### Project documentation lives under docs/. Component-specific documentation may exist alongside source code where it directly supports implementation.

Narrative project documentation lives under `docs/`, organised by topic. Source directories may contain component-level README files where they directly support implementation, but should not duplicate content that belongs in `docs/`.

## Top-level layout

```text
cryptopulse/
│
├── producer/               # Exchange connectors & event preparation
├── processing/             # Validation & stream processing
├── warehouse/              # Database schema & storage
├── analytics/              # Analytical models & business metrics
├── monitoring/             # Operational telemetry infrastructure
├── docker/                 # Infrastructure configuration
├── tests/                  # Unit, integration & end-to-end tests
│
├── docs/
│   ├── overview/
│   └── architecture/
│
├── docker-compose.yml
├── requirements.txt
├── LICENSE
└── README.md
```

## Directory responsibilities

### producer/

Implements the **Exchange Connector** and **Event Preparation** responsibilities.

Establishes connections to external event sources, receives market events, and translates them into the platform's internal event model.

Produces internal events that are written to the Bronze Layer.

### processing/
Implements the **Validation Engine** and **Processing Engine** responsibilities.

The Validation Engine consumes internal events from the Bronze Layer, applies validation rules, and routes trusted events to the Silver Layer. Rejected events are preserved in the Quarantine Layer.

The Processing Engine consumes trusted events from the Silver Layer and transforms them into business-ready analytical information stored in the Gold Layer.
### warehouse/

Implements the physical persistence of the Bronze, Silver, Gold, and Quarantine Layers

Contains database schemas, migration scripts, and persistence configuration. Does not contain business logic or processing code.

### analytics/

Presents the Gold Layer for consumption by dashboards, reports, and analytical tools.

Contains analytical models, metric definitions, and reporting queries. Depends only on the warehouse and never writes to it.

### monitoring/

Implements the **Operational Telemetry** responsibility.

Contains Prometheus configuration, Grafana dashboards, and any monitoring infrastructure. Is independent of the business data pipeline.

### docker/

Contains Docker Compose files, Dockerfiles, and infrastructure configuration required to run the platform locally.

### tests/

Contains all test suites, organised to mirror the source tree.

Includes unit tests, integration tests, and end-to-end tests that validate the pipeline end to end.

## Dependency direction

Dependencies flow in one direction.

```text
producer
    │
    ▼
processing
    │
    ▼
warehouse
    │
    ▼
analytics

monitoring/   (no dependencies in either direction)
tests/        mirrors source tree, no production dependencies
```

No directory should depend on directories to its right in this flow.

- **producer/** depends on nothing inside the repository.
- **processing/** depends on the event model defined in **producer/** and writes to **warehouse/**.
- **warehouse/** is a dependency of **processing/** and **analytics/**, not the reverse.
- **analytics/** depends only on **warehouse/**.
- **monitoring/** depends on nothing and nothing depends on it.

## Architectural boundaries

The directory structure preserves the same boundaries described in the architecture.

| Directory    | Architectural Responsibility          | Produces                        | Consumes From           |
| ------------ | ------------------------------------- | ------------------------------- | ----------------------- |
| producer/    | Exchange Connector + Event Preparation| Internal Events                 | External event sources  |
| processing/  | Validation Engine + Processing Engine | Trusted Events, Business Info   | Bronze Layer, Silver Layer |
| warehouse/   | All four layers (storage)             | —                               | processing/             |
| analytics/   | Data Consumption                      | Dashboards, reports             | Gold Layer              |
| monitoring/  | Operational Telemetry                 | Operational signals             | Every component         |

## Where future features belong

| New feature                              | Where it goes |
| ---------------------------------------- | ------------- |
| A new exchange connector                 | **producer/** |
| A new validation rule                    | **processing/** |
| A new business metric or aggregation     | **processing/** or **analytics/** |
| A new analytical dashboard or report     | **analytics/** |
| A new storage layer or schema change     | **warehouse/** |
| A monitoring or observability improvement| **monitoring/** |
| Infrastructure or deployment change      | **docker/** |
| A test for any of the above              | **tests/** |
