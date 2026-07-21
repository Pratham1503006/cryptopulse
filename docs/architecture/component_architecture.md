# Component Architecture

## Why this document exists

The System Architecture document describes CryptoPulse from a distance.

This document zooms in one level and explains what each major part of the platform is responsible for.

Rather than focusing on technologies or deployment, it describes the logical components that make up the platform, how they interact with one another, and the boundaries between their responsibilities.

Each component exists to solve one problem well.

Keeping those responsibilities separate makes the platform easier to understand today and easier to evolve tomorrow.

---

# Relationship to the System Architecture

The System Architecture document describes CryptoPulse as a sequence of architectural layers.

This document refines those layers into the logical components that implement them.

One example is **Event Ingestion**.

At the architectural level it appears as a single layer.

Here, that layer is intentionally split into two logical components:

* **Exchange Connector**, which communicates with external event sources.
* **Event Preparation**, which converts exchange-specific messages into CryptoPulse's internal event model.

Keeping those responsibilities separate means the rest of the platform never needs to understand how individual exchanges structure their data.

The remaining architectural layers follow the same idea. Each one is represented by the logical component responsible for that part of the system.

---

# Component Overview

The platform is organised as a sequence of logical components.

Each component performs one responsibility before handing work to the next stage of the pipeline.

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
Processing Engine
        │
        ▼
Validation Engine
        │
        ▼
Storage Layer
        │
        ▼
Consumption Layer
```

These are logical components rather than implementation details.

A logical component may eventually be implemented by one service, several services, or different technologies as the platform evolves. What should remain stable is the responsibility each component owns.

---

# Cross-Cutting Concerns

Some engineering concerns don't belong to a single component.

Instead, they exist throughout the platform and influence how every component is designed.

## Observability

Observability isn't something that's added once the pipeline is finished.

Every component should expose enough operational information to explain what it's doing and whether it's healthy.

Exactly what that information looks like will vary. An Exchange Connector might expose connection status and reconnect attempts, while the Processing Engine may report throughput and processing latency.

The important idea is that operational visibility is built into the platform from the beginning rather than treated as an afterthought.

---

## Configuration

Every component depends on configuration, but no component owns it.

Connection details, runtime settings, validation rules, and deployment-specific values should remain external to the implementation so the platform behaves consistently across different environments.

How configuration is supplied to each component is described in the Deployment Architecture document.

---

# Exchange Connector

## Purpose

The Exchange Connector forms the boundary between CryptoPulse and external event sources.

Its responsibility is to establish and maintain external connections, receive live market events, and introduce those events into the platform. By keeping all provider-specific communication in one place, the rest of the platform remains independent of how individual exchanges expose their data.

Version 1 communicates with a single exchange, but the boundary is intentionally designed so additional exchanges can be introduced later without changing the rest of the system.

### Responsibilities

* Connect to external event sources.
* Receive live market events.
* Recover from temporary connection failures.
* Forward events to the preparation stage.

### Outside its responsibility

The Exchange Connector does **not**:

* validate incoming data
* transform event structure
* calculate business metrics
* store data

Its responsibility ends once an event has successfully entered the platform.

### Interacts with

* External Event Sources
* Event Preparation

### Failure handling

If connectivity is interrupted, the connector attempts to re-establish the connection and resume receiving events without requiring manual intervention.

### Future evolution

Future versions may support multiple exchanges through a common connector interface without changing the rest of the pipeline.

---

# Event Preparation

## Purpose

Different exchanges describe the same trade in different ways.

The purpose of Event Preparation is to translate those exchange-specific messages into a single internal event model that every downstream component understands.

Creating one consistent representation of an event means validation, processing, storage, and analytics never need to understand exchange-specific payloads. Supporting a new exchange should primarily require changes here rather than throughout the platform.

### Responsibilities

* Normalise incoming events.
* Standardise field names.
* Convert timestamps and data types.
* Produce the platform's internal event model.

### Outside its responsibility

Event Preparation does **not**:

* determine whether an event is valid
* calculate metrics
* persist data

Its job is translation, not validation.

### Interacts with

* Exchange Connector
* Streaming Backbone

### Failure handling

Events that cannot be translated into the platform's internal event model remain visible for investigation rather than being silently discarded. Since they cannot be represented as valid platform events, they never enter the main processing pipeline.

### Future evolution

As additional exchanges are supported, this component should absorb the exchange-specific translation logic while keeping the rest of the platform unchanged.

---

# Streaming Backbone

## Purpose

The Streaming Backbone separates producers from consumers and provides the communication layer of the platform.

This allows individual parts of the system to evolve independently instead of relying on direct point-to-point communication.

### Responsibilities

* Transport events between components.
* Decouple producers and consumers.
* Buffer events while downstream processing catches up.

### Outside its responsibility

The Streaming Backbone does **not**:

* inspect event contents
* validate data
* perform business processing
* store analytical datasets

### Interacts with

* Event Preparation
* Processing Engine

### Failure handling

Temporary downstream interruptions should not require upstream components to stop accepting new events.

### Future evolution

Future versions may introduce additional event streams or support different categories of events without changing the role of the backbone itself.

---

# Processing Engine

## Purpose

The Processing Engine is responsible for turning trustworthy events into information that can be analysed.

Keeping business transformations separate from ingestion and validation makes each stage easier to reason about and change independently.

### Responsibilities

* Transform incoming events.
* Generate derived values.
* Perform aggregations.
* Prepare data for long-term analytical use.

### Outside its responsibility

The Processing Engine does **not**:

* communicate with external providers
* receive raw exchange messages
* expose dashboards
* manage persistence

### Interacts with

* Streaming Backbone
* Validation Engine

### Failure handling

Processing failures should be isolated to the affected events while allowing the rest of the pipeline to continue operating normally.

### Future evolution

Additional analytical transformations may be introduced without changing the overall responsibility of this component.

---

# Validation Engine

## Purpose

The Validation Engine is the platform's quality gate.

Its role is to ensure that only trustworthy data progresses into the datasets consumed by downstream systems.

Keeping validation separate from processing allows business logic and data quality rules to evolve independently.

### Responsibilities

* Validate incoming records.
* Detect malformed events.
* Identify duplicate records.
* Route invalid records for investigation.

### Outside its responsibility

The Validation Engine does **not**:

* transform business data
* calculate analytical metrics
* store records
* communicate with external providers

### Interacts with

* Processing Engine
* Storage Layer

### Failure handling

Records that fail validation are isolated from the main pipeline while valid records continue downstream without interruption.

### Future evolution

Validation rules are expected to grow over time, but the responsibility of this component should remain unchanged.

---

# Storage Layer

## Purpose

The Storage Layer preserves data throughout its lifecycle.

Instead of maintaining only one version of the data, it keeps progressively more refined representations that support replay, auditing, and analytical workloads.

### Responsibilities

* Persist platform data.
* Preserve historical records.
* Store trusted analytical datasets.

### Outside its responsibility

The Storage Layer does **not**:

* validate incoming events
* generate business metrics
* monitor platform health

### Interacts with

* Validation Engine
* Consumption Layer

### Failure handling

Storage failures should be surfaced immediately so they can be investigated before they result in silent data loss.

### Future evolution

Additional storage technologies or optimisation strategies may be introduced without changing the responsibility of the Storage Layer.

---

# Consumption Layer

## Purpose

The Consumption Layer is where the platform delivers value.

It makes trustworthy data available to people and downstream systems without exposing the complexity of the pipeline that produced it.

### Responsibilities

* Expose business datasets.
* Support analytical workloads.
* Provide data for dashboards, reporting, and future downstream services.

### Outside its responsibility

The Consumption Layer does **not**:

* process streaming events
* validate data
* modify stored datasets

### Interacts with

* Storage Layer
* Downstream consumers

### Failure handling

Failures in consuming applications should never interrupt the upstream processing pipeline.

### Future evolution

Future versions may introduce APIs, additional dashboards, machine learning workloads, or other consumers without requiring changes to upstream components.

---

# Relationship to the rest of the architecture

This document explains the responsibility of each logical component.

The remaining architecture documents build on that foundation.

* **Data Flow** follows a single event as it moves through these components.
* **Deployment Architecture** explains where these components run and how they communicate.
* **Data Model** describes the information those components produce and consume.

Together, these documents describe the platform from progressively lower levels of abstraction without repeating the same information.
