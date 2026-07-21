# Component Architecture

## Why this document exists

The System Architecture document describes CryptoPulse from a high level.

This document zooms in one layer and explains the major building blocks that make that architecture work.

Rather than focusing on technologies, it focuses on responsibilities.

Each component exists for one reason. Keeping those responsibilities separate makes the platform easier to understand, easier to test, and easier to change as it grows.



# Relationship to the System Architecture

The responsibilities described in the System Architecture are implemented here as logical components.

Some architectural responsibilities map directly to a single component.

Others are intentionally split into smaller responsibilities.

For example, **Event Ingestion** is represented by two components:

* **Exchange Connector**, responsible for communicating with external event sources.
* **Event Preparation**, responsible for translating external messages into CryptoPulse's internal event model.

Splitting these responsibilities means that supporting a new exchange should only require changes in one part of the platform instead of many.


# Cross-Cutting Concerns

Some responsibilities exist throughout the platform rather than belonging to one component.

## Observability

Every major component should expose enough operational information to explain what it is doing and whether it is healthy.

Operational telemetry is treated as a product of the platform in its own right rather than something added after the system is built.

This keeps operational health separate from business data while making failures easier to understand.



## Configuration

Every component depends on configuration, but no component owns it.

Connection settings, runtime behaviour, validation rules, and environment-specific values remain external to the implementation.

How configuration is supplied is described in the implementation documentation.



# Exchange Connector

## Why this component exists

CryptoPulse depends on external event sources, but the rest of the platform shouldn't depend on how those providers expose their data.

The Exchange Connector forms that boundary.

Its only responsibility is bringing external events into the platform reliably.

### Responsibilities

* Establish connections to external event sources.
* Receive live market events.
* Recover from temporary connection failures.
* Forward received events for preparation.

### It deliberately does **not**

* understand internal business rules
* validate events
* transform event structures
* store data

### Failure behaviour

If the connection is interrupted, the connector attempts to reconnect and continue receiving events.

Events published while the platform is disconnected are outside the platform's control and are a known limitation of Version 1.

### Future evolution

Version 1 supports a single exchange.

Future versions should be able to introduce additional exchanges without changing the rest of the platform.



# Event Preparation

## Why this component exists

Different exchanges describe the same market event differently.

The rest of the platform shouldn't need to understand those differences.

Event Preparation translates every incoming message into a single internal event model so every downstream component works with the same representation.

### Responsibilities

* Translate exchange-specific payloads.
* Standardise field names.
* Convert timestamps and data types.
* Produce the platform's internal event model.

### It deliberately does **not**

* decide whether an event is trustworthy
* calculate business metrics
* persist data

### Failure behaviour

Events that cannot be translated into the internal event model remain visible for investigation rather than disappearing silently.

Because they cannot be represented as valid platform events, they never progress into the trusted pipeline.

### Future evolution

As additional exchanges are introduced, exchange-specific translation logic should remain isolated within this component.



# Bronze Layer

## Why this component exists

Once the platform has successfully received and understood an event, it should preserve that event before making any decisions about its quality.

Bronze acts as the platform's landing zone.

It records what CryptoPulse successfully received, making replay, auditing, and debugging possible even if later stages reject the event.

### Responsibilities

* Preserve successfully ingested events.
* Provide an immutable historical record.
* Support replay and auditing.

### It deliberately does **not**

* validate records
* calculate metrics
* expose business data


# Validation Engine

## Why this component exists

Analytics are only useful if the underlying data can be trusted.

The Validation Engine is responsible for deciding whether an event is suitable for downstream use.

### Responsibilities

* Apply data quality rules.
* Detect malformed or duplicate events.
* Separate trusted events from rejected events.
* Route rejected events to the Quarantine Layer.

### It deliberately does **not**

* calculate business metrics
* modify business datasets
* communicate with external providers

### Failure behaviour

Validation failures do not stop the pipeline.

Trusted events continue to Silver while rejected events remain available for investigation in the Quarantine Layer.

### Future evolution

Validation rules are expected to evolve over time, but the responsibility of this component should remain unchanged.



# Quarantine Layer

## Why this component exists

Rejecting an event should never mean losing it.

The Quarantine Layer preserves events that could not continue through the trusted pipeline so they can be inspected, understood, and, where appropriate, replayed after the underlying issue has been resolved.

### Responsibilities

* Preserve rejected events.
* Support investigation.
* Provide visibility into data quality issues.

### It deliberately does **not**

* participate in analytics
* feed downstream consumers
* replace Bronze history



# Silver Layer

## Why this component exists

Silver represents the platform's trusted operational data.

Every event stored here has successfully passed validation and is ready for downstream processing.

### Responsibilities

* Store validated events.
* Provide trusted input for processing.
* Act as the foundation for analytical workloads.

### It deliberately does **not**

* preserve rejected events
* calculate business metrics



# Processing Engine

## Why this component exists

Once data is trusted, it can be transformed into information that is useful for analysis.

Separating processing from validation keeps business logic independent from data quality rules.

### Responsibilities

* Aggregate trusted events.
* Calculate derived values.
* Produce analytical datasets.

### It deliberately does **not**

* validate incoming data
* receive external events
* expose dashboards

### Failure behaviour

Failures affect only the events being processed.

The rest of the platform continues operating independently.

### Future evolution

New analytical calculations can be introduced without changing the responsibilities of surrounding components.



# Gold Layer

## Why this component exists

Gold contains business-ready information rather than individual events.

It exists to provide a stable interface between the platform and the people or systems consuming its outputs.

### Responsibilities

* Store business-ready datasets.
* Support dashboards and reporting.
* Provide trusted analytical outputs.

### It deliberately does **not**

* preserve raw history
* perform event validation
* process streaming events



# Design Decisions

Several architectural decisions shape the way these components fit together.

### Why preserve Bronze before validation?

Once an event has been successfully received, the platform should preserve what it actually saw before deciding whether that event is trustworthy.

This makes replay, auditing, and debugging much easier.



### Why separate validation from processing?

Data quality and business logic solve different problems.

Keeping them independent allows each to evolve without affecting the other.



### Why keep a Quarantine Layer?

Rejected events often contain valuable information about upstream issues.

Keeping them visible makes failures explainable instead of mysterious.



### Why separate operational telemetry from business data?

Business data answers questions about the market.

Operational telemetry answers questions about the platform.

Treating them as separate products keeps monitoring independent of analytical workloads.
