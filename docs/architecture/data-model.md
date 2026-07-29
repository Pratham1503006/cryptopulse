# Data Model

## Why this document exists

The previous architecture documents describe how CryptoPulse is organised, why its components exist, and how data moves through the platform.

This document focuses on something different.

It explains the information the platform works with.

Rather than describing databases or storage technologies, it describes how data evolves as it moves through the platform and how each stage represents a different level of confidence and meaning.

The goal isn't to describe where information is stored.

The goal is to describe what that information represents.

  

# The lifecycle of information

These states describe how the meaning of information changes over time. They do not necessarily correspond to individual runtime components or processing steps.

CryptoPulse does not repeatedly modify the same piece of data.

Instead, information evolves through a series of well-defined states.

Each state answers a different question about the data.

```text
External Market Event
        │
        ▼
Internal Event
        │
        ▼
Trusted Event
        │
        ▼
Business Information
```

Every stage exists because the information itself has changed, not simply because it has been copied somewhere else.

  

# External Market Event

An External Market Event represents information exactly as it is published by an exchange.

At this stage, CryptoPulse makes no assumptions about correctness or consistency.

The event belongs entirely to the external provider.

Different providers may describe the same trade differently, and field names, formats, or timestamps may vary.

Before the event enters the platform, it has no meaning beyond what the provider assigns to it.

  

# Internal Event

Once an event has been successfully received and standardised, it becomes an Internal Event.

This is the canonical representation used throughout CryptoPulse.

Every downstream component works with this model regardless of which exchange originally produced the data.

At this stage, the platform understands the structure of the event, but it has not yet decided whether the information should be trusted.

This is the representation preserved in the  Bronze Layer.

  

# Trusted Event

A Trusted Event is an Internal Event that has successfully passed the platform's validation rules.

Trust does not mean the event is objectively correct.

It means the event satisfies the quality requirements that CryptoPulse expects before allowing it to participate in downstream processing.

Trusted Events form the contents of the Silver Layer and become the input for every analytical workload.

  

# Business Information

Business Information is no longer concerned with individual events.

Instead, it represents analytical outputs derived from many Trusted Events.

Examples include aggregated trading statistics, rolling metrics, market indicators, and other business-facing information.

This information forms the Gold Layer consumed by dashboards and analytical tools.

  

# Rejected Events

Not every Internal Event becomes trusted.

Events that fail validation remain part of the platform's operational history, but they do not participate in downstream analytics.

These events are preserved in the Quarantine Layer.

Keeping rejected events is a deliberate design decision.

Understanding why an event failed validation is often just as valuable as processing the events that succeeded.

  

# What changes as data evolves?

As information moves through the platform, several properties change.

| Property                  | External Event      | Internal Event | Trusted Event | Business Information      |
|                 - |             - |         -- |         - |                 - |
| Standardised              | No                  | Yes            | Yes           | Yes                       |
| Validated                 | No                  | No             | Yes           | Derived from trusted data |
| Suitable for analytics    | No                  | No             | Yes           | Yes                       |
| Represents a single trade | Yes                 | Yes            | Yes           | Not necessarily           |
| Immutable                 | Managed by provider | Yes            | Yes           | Derived outputs           |

Each stage introduces new meaning rather than simply creating another copy of the data.

  

# Immutable and derived information

CryptoPulse distinguishes between information that is preserved and information that is created.

**Immutable information** represents events exactly as they existed when the platform received them.

Once preserved, these records should not be modified.

**Derived information** is produced through processing.

Aggregations, rolling calculations, and business metrics are all examples of derived information.

Separating immutable and derived data makes the platform easier to audit, replay, and reason about.

  

# Metadata

Every event carries more than business data.

The platform also maintains metadata that helps explain the event's history.

Examples include:

* where the event originated,
* when it was received,
* which stage produced it,
* whether validation succeeded,
* why validation failed (if applicable).

This metadata helps explain how information reached its current state without changing the underlying business data itself.

  

# Trust as a modelling concept

One of the central ideas in CryptoPulse is that trust is part of the data model rather than something inferred later.

An event is not considered trustworthy simply because it exists.

Trust is established through validation and preserved as the event moves through the platform.

Thinking about trust explicitly simplifies downstream systems because consumers no longer need to repeat the same validation work.

Instead of asking:

> "Can I trust this record?"

Consumers can ask:

> "Which stage of the data model produced this record?"

The answer already tells them what level of confidence they should have.

  

# Design principles

Several ideas shape the way information is represented throughout the platform.

* Information gains meaning as it moves through the platform.
* Trust is established, not assumed.
* Original events remain traceable.
* Derived information never replaces historical information.
* Rejected events remain part of the platform's history.
* Metadata explains how information reached its current state.

These principles are intended to remain stable even if the implementation changes in future versions.
