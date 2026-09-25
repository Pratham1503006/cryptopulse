"""Validation Engine base interfaces.

Defines the rule abstraction the trust boundary is built from and the
engine boundary that composes rules.

Design notes:

- A rule is the smallest independently testable unit of data quality.
  Rules are pure functions of the canonical InternalEvent: no I/O, no
  database, no transport, no configuration-dependent behavior.
- The engine evaluates ALL configured rules rather than stopping at the
  first failure, so Quarantine can explain every problem with an event.
- Validation decides trust only. It does not persist, does not route,
  and does not aggregate (those belong to the trust service, warehouse,
  and the future Processing Engine respectively).
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from common.contracts import InternalEvent, TrustedEvent, ValidationFailure, ValidationOutcome

__all__ = ["ValidationRule", "Validator"]


class ValidationRule(ABC):
    """One deterministic, independently testable data-quality check.

    Implementations must be pure functions of the InternalEvent: no I/O,
    no persistence, no transport awareness. A rule reports failures; it
    never raises for a data-quality outcome (invalid data is a business
    result, not an application crash). Infrastructure failures are not a
    rule concern.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Stable identifier of this rule (used in ValidationFailure.rule)."""
        ...

    @abstractmethod
    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        """Evaluate the rule against an InternalEvent.

        Returns:
            All failures produced by this rule for the event; empty when
            the event satisfies the rule.
        """
        ...


class Validator(ABC):
    """Boundary for the trust decision: InternalEvent -> trusted or not.

    Evolved from the Milestone-1 scaffold (which returned
    ``TrustedEvent | None``, unable to express WHY an event was rejected).
    The engine now returns the structured ValidationOutcome contract;
    constructing TrustedEvent from a valid event remains available as a
    pure mapping helper so existing callers keep working.

    Validation produces decisions and explanations; it never persists
    (routing to Silver/Quarantine belongs to the trust service) and never
    aggregates (that belongs to the future Processing Engine).
    """

    @abstractmethod
    def validate(self, event: InternalEvent) -> ValidationOutcome:
        """Evaluate all configured rules against an InternalEvent."""
        ...

    def trusted_event_from(self, event: InternalEvent) -> TrustedEvent:
        """Map a valid InternalEvent to its canonical TrustedEvent form.

        Pure conversion helper shared by all implementations: TrustedEvent
        is InternalEvent plus the validated_at timestamp, per the canonical
        contract in common/contracts/events.py. Only meaningful for events
        whose ValidationOutcome.valid is True.
        """
        from datetime import UTC, datetime

        return TrustedEvent(
            **event.model_dump(),
            validated_at=datetime.now(UTC),
        )
