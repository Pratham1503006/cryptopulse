# System Architecture

## Why this document exists

CryptoPulse is made up of several independent parts working together to solve a single problem.

Looking at the source code alone makes it difficult to understand where one responsibility ends and another begins.

This document provides a high-level view of the platform before diving into individual components or implementation details. It describes how the system is organised, how information moves through it, and the responsibilities assigned to each stage of the platform.

It intentionally avoids implementation details. Those belong in the more detailed architecture documents that build on top of this one.

---

## At a glance

CryptoPulse is a production-inspired streaming data platform that continuously ingests live market events and transforms them into trustworthy analytical datasets through a sequence of independent processing stages.

---

# Architectural Overview

At a high level, CryptoPulse follows an event-driven streaming architecture.

Live events enter the platform, move through a sequence of well-defined stages, become progressively more trustworthy as they pass through the system, and are finally made available for downstream consumers.

```text
External Event Sources
        │
        ▼
 Event Ingestion
        │
        ▼
 Event Preparation
        │
        ▼
 Event Streaming
        │
        ▼
 Data Validation
        │
        ▼
 Layered Storage
        │
        ▼
 Consumption
```

Each stage has a single responsibility.

Rather than combining multiple concerns into one service, the platform separates ingestion, preparation, streaming, validation, storage, and consumption into distinct architectural layers. This makes the system easier to understand, monitor, test, and evolve over time.

---

# Architectural Style

CryptoPulse follows two architectural styles that complement each other.

### Event-Driven

The platform reacts to events as they arrive instead of processing static datasets on a schedule.

Every market trade is treated as an immutable event that flows through the system exactly once before becoming part of a progressively richer analytical dataset.

---

### Layered Processing

Each architectural layer has one clearly defined responsibility.

Rather than performing multiple unrelated tasks in a single stage, the platform gradually improves the quality and usability of the data as it moves downstream.

Keeping responsibilities separate makes failures easier to isolate and architectural changes easier to introduce without affecting the rest of the system.

---

# Architectural Layers

The platform is organised into six logical layers.

## Event Ingestion

Receives data from external event sources and converts it into a format understood by the platform.

This layer is the only part of the system that communicates directly with external providers.

---

## Event Preparation

Standardises incoming events before they enter the rest of the platform.

Typical responsibilities include normalising field names, converting timestamps, standardising data types, and preparing events for downstream validation.

Separating preparation from validation keeps validation focused on determining whether an event is trustworthy rather than transforming it into the expected format.

---

## Event Streaming

Moves prepared events between independent parts of the platform.

Rather than allowing components to communicate directly, the streaming layer acts as the backbone of the architecture, enabling producers and consumers to evolve independently.

---

## Data Validation

Acts as the quality gate for the platform.

Incoming events are checked against the platform's expected data model before progressing further through the pipeline.

Records that fail validation remain visible for investigation instead of disappearing silently.

---

## Layered Storage

Stores data at different stages of its lifecycle.

Rather than maintaining a single representation of the data, the platform preserves both raw events and progressively refined datasets, making historical replay, auditing, and analytical workloads possible.

---

## Consumption

Makes trustworthy data available to downstream consumers.

Consumers may include analytical dashboards, operational monitoring tools, reporting systems, or future services built on top of the platform.

The architecture deliberately separates data production from data consumption so new consumers can be introduced without changing the upstream pipeline.

---

# External Systems

CryptoPulse intentionally keeps external systems at the edge of the architecture.

External event sources interact with the platform only through clearly defined ingestion interfaces. Once data enters the platform, communication between architectural layers happens through the platform's internal contracts rather than direct dependencies on external services.

Current categories of external systems include:

* External event sources
* Business intelligence tools
* Monitoring and visualisation platforms
* Local infrastructure services

This separation reduces coupling between the platform and external providers while making the internal architecture easier to evolve over time.

---

# System Boundaries

CryptoPulse is responsible for the streaming data platform itself.

The system begins when external events are received and ends when trustworthy datasets and operational information become available for downstream consumers.

Capabilities such as trading, authentication, portfolio management, user accounts, and cloud infrastructure are intentionally outside these boundaries and are documented separately in the project scope.

Maintaining a clear system boundary keeps Version 1 focused on solving one engineering problem well.

---

# Architectural Characteristics

The architecture is designed around a small number of characteristics that should remain stable even if individual technologies change.

The platform is designed to be:

* **Event-driven**, reacting to incoming events rather than scheduled batches.
* **Layered**, with each architectural stage owning one responsibility.
* **Observable**, making operational behaviour visible alongside business outputs.
* **Modular**, allowing individual components to evolve independently.
* **Reproducible**, enabling another developer to build and run the complete platform using only the documented setup process.

These characteristics describe the architecture itself rather than the technologies used to implement it.

---

# Relationship to the Architecture Documentation

This document intentionally stays at the highest level of abstraction.

The remaining architecture documents progressively introduce more detail.

| Document                    | Responsibility                                           |
| --------------------------- | -------------------------------------------------------- |
| **System Architecture**     | Defines the overall structure of the platform            |
| **Component Architecture**  | Explains the responsibility of each individual component |
| **Data Flow**               | Follows a single event through the platform              |
| **Deployment Architecture** | Describes how services are deployed and communicate      |
| **Data Model**              | Defines the information managed throughout the platform  |

Together, these documents describe the architecture from progressively lower levels of abstraction without repeating the same information.

---

# Evolution

This document represents the current understanding of CryptoPulse's architecture.

Implementation details, technologies, and deployment strategies may change as the project evolves.

The overall responsibilities of the architectural layers, however, should remain relatively stable.

If those responsibilities change, this document should be updated first before the more detailed architecture documents are revised.
