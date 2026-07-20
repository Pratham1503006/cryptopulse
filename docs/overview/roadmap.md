# Roadmap

## Why this document exists

No engineering project is ever truly finished.

As understanding improves, new ideas emerge, assumptions are challenged, and opportunities for improvement become easier to recognise.

This document captures the long-term direction of CryptoPulse.

Unlike `project-scope.md`, which defines what belongs in the current release, the roadmap looks beyond the current implementation and records where the project could evolve next.

Nothing in this document is a commitment.

It's simply the direction I believe the project should move once the current release is complete.

---

# Current Release

**Version:** 1.0

**Status:** 🟡 In Progress

The current focus is establishing a complete, production-inspired streaming data platform.

The implementation boundaries for this release are intentionally fixed and are documented in `project-scope.md`.

Until Version 1 is complete, expanding the platform takes lower priority than finishing it well.

---

# Version 2 — Strengthen the Platform

Once the foundation has proven itself, the next step is improving the platform rather than expanding its feature set.

Current areas of interest include:

* Supporting additional exchange connectors through the existing connector interface.
* Improving throughput and processing efficiency.
* Expanding operational monitoring and diagnostics.
* Improving local deployment and developer experience.
* Refining the analytical models built on top of the processed data.

The objective of Version 2 is to improve the quality of the platform without fundamentally changing its architecture.

---

# Version 3 — Challenge the Architecture

By this stage, the platform should have enough real-world experience to begin questioning some of its original design decisions.

Rather than adding features, the focus shifts towards evaluating alternative approaches.

Possible areas of exploration include:

* Comparing alternative stream processing engines.
* Revisiting storage architecture based on operational experience.
* Evaluating different deployment models.
* Introducing metadata management and data lineage.
* Exploring more advanced monitoring and operational tooling.

The purpose of this version is to validate architectural decisions through experience rather than assumptions.

---

# Future Exploration

These ideas are intentionally left without a target release.

Some may eventually become part of CryptoPulse.

Others may simply remain interesting experiments.

Current areas of interest include:

* Statistical anomaly detection.
* Historical replay and simulation tooling.
* Real-time notifications.
* Cloud-native deployment.
* Automated benchmarking.
* Support for additional streaming domains beyond cryptocurrency.
* Data governance and lineage.

These ideas will only be considered once they solve a real problem encountered during development rather than being added for their own sake.

---

# How this roadmap evolves

This roadmap is expected to change.

Ideas may move between releases, be replaced by better alternatives, or disappear entirely as implementation provides new information.

That isn't a sign of poor planning.

It's a normal part of engineering.

The roadmap should always reflect the best understanding of where the project ought to go next—not where it was expected to go months earlier.

Whenever a release is completed, this document should be reviewed before planning the next one.
