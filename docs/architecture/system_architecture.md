# System Architecture

## Why this document exists

Before looking at individual components or technologies, it's useful to understand the platform as a whole.

This document describes the logical architecture of CryptoPulse. It explains the major responsibilities that make up the platform, the boundaries between them, and how data moves at a high level.

It deliberately avoids implementation details.

Whether a responsibility is implemented using Kafka, Spark, or something completely different is a separate decision. The goal here is to describe the architecture in a way that remains valid even if the implementation changes.

 

# A quick overview

CryptoPulse is organised as a sequence of responsibilities that gradually increase the trustworthiness and usefulness of incoming data.

An event enters the platform as an external market message.

As it moves through the system, it becomes:

* successfully received,
* safely preserved,
* validated,
* transformed into analytical information,
* and finally consumed by people or downstream systems.

Alongside this journey, every major component emits operational telemetry so the health of the platform can be observed independently of the business data it produces.

 

# High-level architecture

```text
                    External Event Sources
                              │
                              ▼
                    Exchange Connector
                              │
                              ▼
                     Event Preparation
                              │
                              ▼
                     Streaming Backbone
                              │
                              ▼
                      Bronze Layer
                  (Raw Landing Zone)
                              │
                              ▼
                    Validation Engine
                    ┌─────────┴──────────┐
                    ▼                    ▼
          Silver Layer        Quarantine Layer
          (Trusted Data)      (Rejected Events)
                    │
                    ▼
               Processing Engine
                    │
                    ▼
                Gold Layer
          (Business-Ready Data)
                    │
                    ▼
            Business Consumers


────────────────────────────────────────────────────

Every Component
        │
        ▼
Operational Telemetry
        │
        ▼
Operational Monitoring
```

The architecture intentionally separates the business data pipeline from operational telemetry.

Business datasets describe **what is happening in the market**.

Operational telemetry describes **how well the platform itself is operating**.

Although both are valuable, they serve different purposes and evolve independently.

 

# Architectural responsibilities

The platform is organised around six logical responsibilities.

Each responsibility solves one problem before handing work to the next stage.

## 1. Event Ingestion

The platform establishes communication with external event sources and receives live market events.

Its responsibility is simply to bring events into CryptoPulse reliably.

 

## 2. Event Standardisation

External providers rarely describe data in the same way.

Before an event can move through the platform, it is converted into a single internal representation that every downstream component understands.

From this point onward, the rest of the platform no longer depends on exchange-specific message formats.

 

## 3. Data Preservation

Once an event has entered the platform successfully, it is preserved in the  Bronze Layer.

Bronze acts as the platform's landing zone.

It represents the earliest version of an event that CryptoPulse has successfully received and allows the platform to replay or audit historical activity without depending on the original data source.

Preserving data before applying quality rules ensures that the platform always retains an accurate record of what it received.

 

## 4. Data Validation

The Validation Engine determines whether an event is suitable for trusted analytical use.

Events that satisfy the platform's quality expectations continue into the Silver Layer.

Events that fail validation are routed to the Quarantine Layer where they remain available for investigation.

Removing an event from the trusted pipeline should never mean silently losing it.

 

## 5. Data Processing

Only trusted data is processed into higher-level analytical datasets.

Business calculations, aggregations, and derived metrics are performed after validation rather than before it.

This separation keeps business logic independent from data quality rules and ensures analytical outputs are always derived from trusted inputs.

The result of this stage is the Gold Layer.

 

## 6. Data Consumption

The Gold Layer is consumed by dashboards, reports, analytical tools, or future downstream services.

Consumers should not need to understand how the platform produced the data.

They interact only with datasets that have already passed through the previous architectural stages.

 

# Operational telemetry

Operational visibility is treated as a parallel concern rather than the final stage of the business pipeline.

Every major component emits telemetry describing its own behaviour.

Examples include:

* connection health,
* throughput,
* processing latency,
* validation failures,
* consumer lag,
* storage health.

These signals are collected independently from business data and are used to understand the health of the platform rather than market activity.

Keeping operational telemetry separate prevents monitoring concerns from becoming tightly coupled to analytical datasets.

 

# Architectural characteristics

Several principles influenced the way the platform is organised.

## Event-driven

The platform reacts to incoming events rather than scheduled batches.

Each event progresses independently through the pipeline.

 

## Layered

Data becomes progressively more trustworthy as it moves from Bronze to Silver to Gold.

Each dataset exists for a different purpose rather than representing different copies of the same information.

 

## Observable

Every major responsibility contributes operational telemetry.

Understanding whether the platform is healthy should not require reading application logs.

 

## Modular

Each responsibility owns one part of the problem.

Changing one part of the platform should require minimal changes elsewhere.

 

## Reproducible

The platform is intended to be built and run by another developer using only the repository and its documentation.

If that cannot be done, the platform is incomplete regardless of whether the software itself works.

 

# System boundaries

CryptoPulse intentionally focuses on the data platform.

Several concerns sit outside those boundaries.

The platform does not manage:

* user authentication,
* portfolio management,
* order execution,
* trading strategies,
* wallet integration,
* account management.

Similarly, the platform can only process events that it successfully receives.

Events published while an external source is unavailable or while the platform is disconnected are outside the platform's control and are not recoverable in Version 1. This is a known limitation of the system boundary rather than a failure of the internal pipeline.

 

# A note on evolution

The architecture described here represents Version 1 of CryptoPulse.

As the project evolves, technologies, deployment strategies, and individual implementations may change.

The responsibilities described in this document should remain considerably more stable.

If a future architectural decision requires changing these responsibilities or their boundaries, this document should be updated before any lower-level architecture documents are revised.
