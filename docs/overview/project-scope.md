# Project Scope

## Why this document exists

Every project starts with a clear objective.

Most projects become difficult because that objective slowly expands over time.

A useful idea becomes another feature.

Another feature becomes another dependency.

Before long, the original problem has been replaced by a much larger one.

This document defines the implementation boundaries of Version 1.

If a capability is described here, it belongs in the current release.

If it isn't, it belongs in a future version until this release is complete.

---

## Scope of Version 1

Version 1 focuses on building a complete, end-to-end streaming data platform capable of ingesting live events, processing them into analytical datasets, and exposing both business and operational information.

The implementation is intentionally limited to the capabilities described below.

---

## Included in Version 1

### Live event ingestion

* Connect to a single cryptocurrency exchange.
* Continuously consume live market events.
* Automatically recover from temporary connection failures.
* Normalise incoming events into a common internal format.

---

### Streaming pipeline

* Publish events into a streaming platform.
* Consume events for downstream processing.
* Decouple data producers from data consumers.

---

### Data processing

* Validate incoming events.
* Reject malformed records.
* Handle duplicate events.
* Generate derived datasets from validated data.

---

### Data storage

* Preserve raw event history.
* Maintain validated datasets.
* Produce analytical datasets for reporting and querying.

---

### Analytics

Generate a core set of business metrics from processed data.

The specific metrics are documented separately as part of the analytical model.

---

### Operational monitoring

Expose operational information required to understand the health of the platform, including processing status, pipeline activity, and validation behaviour.

---

### Local deployment

Provide a reproducible local deployment that allows another developer to run the complete platform using the documented setup process.

---

### Documentation

Document the architecture, implementation decisions, deployment process, and data model alongside the source code.

Documentation is considered part of the Version 1 deliverable.

---

## Explicitly out of scope

The following capabilities are intentionally excluded from Version 1.

### Data sources

* Multiple exchange connectors
* Historical backfilling from multiple providers

---

### Machine learning

* Price prediction
* Forecasting
* Recommendation systems
* Statistical anomaly detection

---

### Trading

* Order execution
* Portfolio management
* Wallet integration
* Exchange account management

---

### Infrastructure

* Kubernetes
* Cloud deployment
* Multi-region deployments
* High-availability clusters

---

### Application features

* User accounts
* Authentication
* Authorization
* Billing
* Multi-tenancy

---

## Acceptance criteria

Version 1 is considered complete when all scoped capabilities have been implemented and demonstrated.

Specifically:

* Live data flows continuously through the platform.
* Data is processed successfully from ingestion to analytics.
* Operational information is available for monitoring the platform.
* The platform can be deployed locally using the documented setup process.
* Documentation reflects the implemented system.

Detailed engineering and operational targets are defined in `success-metrics.md`.

---

## Scope changes

New ideas are expected throughout development.

Before adding a feature to Version 1, ask a single question:

> **Can the objectives of Version 1 still be achieved without this feature?**

If the answer is **yes**, the feature should remain outside the current release and be considered during future roadmap planning instead.
