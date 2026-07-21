# Data Flow

## Why this document exists

The System Architecture document explains how CryptoPulse is organised.

The Component Architecture document explains why each major component exists.

This document answers a different question:

> **What actually happens to a market event after it enters the platform?**

Rather than describing the platform component by component, it follows the lifecycle of a single event from the moment it is published by an exchange until it either becomes trusted business information or leaves the trusted pipeline.

The goal isn't to explain technologies.

It's to explain how trust is established.

 

# The lifecycle of an event

One way to understand CryptoPulse is to think less about databases and services, and more about how an event changes over time.

Every event passes through four logical states during its lifecycle.

```text
External Market Event
        │
        ▼
Internal Event
(Standardised)
        │
        ▼
Trusted Event
(Validated)
        │
        ▼
Business Information
(Aggregated Insights)
```

The datasets used throughout the platform simply preserve these different stages of that lifecycle.

* **Bronze** preserves successfully received internal events.
* **Silver** preserves trusted events.
* **Gold** preserves business information derived from trusted events.
* **Quarantine** preserves events that could not become trusted.

Thinking about the platform this way shifts the focus away from storage and toward how confidence in the data is established.

 

# Where the platform's responsibility begins

A market trade exists before CryptoPulse ever sees it.

Until the Exchange Connector successfully receives an event, it remains entirely outside the platform's control.

If an exchange becomes unavailable or an event is never delivered, CryptoPulse cannot recover or validate it.

Once an event crosses the Exchange Connector boundary, responsibility shifts to the platform.

From that point onward, every event should either continue through the pipeline or remain visible with a clear explanation of why it could not.

 

# Receiving the event

A completed market trade is published by an external event source.

The Exchange Connector receives that message and forwards it to Event Preparation.

At this stage, the event still reflects the exchange's own structure and naming conventions.

Event Preparation translates that message into CryptoPulse's internal event model so that every downstream component works with the same representation regardless of where the event originated.

The event has now become an internal event.

 

# Preserving what was received

Before the platform decides whether an event can be trusted, it first preserves what it successfully received.

The event is written to the Bronze Dataset.

This is a deliberate architectural decision.

Bronze is not a trusted analytical dataset.

It is the platform's historical record of successfully received events.

Keeping this record before validation makes replay, auditing, debugging, and future rule changes possible without depending on the original event source.

 

# Deciding whether the event can be trusted

Once the event has been preserved, it reaches the Validation Engine.

This is the point where the platform evaluates whether the event satisfies its quality rules.

The outcome of validation determines whether the event continues through the trusted pipeline or leaves it.

From here, the journey splits.

 

# Trusted path

If validation succeeds, the event is written to the Silver Dataset.

Silver represents trusted operational data.

Every event stored here has passed the platform's quality checks and is suitable for downstream processing.

The Processing Engine consumes trusted events from Silver and performs the business transformations that create analytical information.

These transformations may include aggregations, derived metrics, rolling calculations, or other business logic.

The resulting outputs are written to the Gold Dataset.

Gold no longer represents individual market events.

It represents business-ready information derived from many trusted events.

This is the data consumed by dashboards, reports, and analytical tools.

 

# Rejected path

Not every event reaches Silver.

If validation determines that an event cannot be trusted, it is written to the Quarantine Dataset instead.

Quarantine exists to preserve visibility into failures.

Rejected events should never disappear silently.

Keeping them separate from trusted data protects downstream consumers while allowing engineers to investigate why an event was rejected and whether the underlying issue should be corrected.

The Quarantine Dataset is part of the platform's operational history, not part of its analytical data.

 

# What happens if processing fails?

Validation and processing solve different problems.

Validation determines whether an event is trustworthy.

Processing transforms trusted events into business information.

If processing fails, the event does not become untrusted.

Instead, the processing failure remains isolated to that work while the rest of the pipeline continues operating.

This separation allows data quality and business logic to evolve independently.

 

# Operational telemetry follows a different path

Business data is only one output of the platform.

Every major component also produces operational telemetry describing its own behaviour.

Examples include:

* connection health,
* throughput,
* processing latency,
* validation failures,
* consumer lag,
* storage health.

Unlike business data, these signals do not pass through Bronze, Silver, or Gold.

Instead, they form a separate operational view of the platform that supports monitoring, troubleshooting, and capacity planning.

Keeping operational telemetry independent prevents monitoring concerns from becoming tightly coupled to business datasets.

 

# What changes during the journey?

Although the same event moves through the platform, its meaning changes over time.

| State                     | Description                                                                     |
|         - |                           - |
| **External Market Event** | A message published by an external event source.                                |
| **Internal Event**        | A standardised representation understood by every component inside CryptoPulse. |
| **Trusted Event**         | An internal event that has successfully passed validation.                      |
| **Business Information**  | Aggregated analytical outputs derived from one or more trusted events.          |

Thinking in terms of these states is often more useful than thinking about individual technologies or storage layers.

 

# What never changes?

Several principles remain true regardless of where an event is in its lifecycle.

* Every successfully received event is preserved before quality decisions are made.
* Trusted and rejected events never share the same path.
* Rejected events remain visible for investigation.
* Every transformation has a clear purpose.
* Business information is always derived from trusted data.
* Operational telemetry remains separate from business data.

These principles are more fundamental than the technologies used to implement them.

 

# Why the flow looks this way

Several architectural decisions shape the lifecycle of every event.

**Bronze comes before validation** because preserving what the platform actually received is often more valuable than immediately deciding whether that information can be trusted.

**Validation comes before processing** because business calculations should only operate on trusted data.

**Quarantine exists alongside Silver** because understanding failures is an essential part of operating a data platform.

**Operational telemetry follows its own path** because platform health and business insights answer different questions and should evolve independently.
