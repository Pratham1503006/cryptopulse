"""Validation Engine.

Composes validation rules and produces the structured trust decision for a
canonical InternalEvent.

Behavior:

- Evaluates ALL configured rules rather than stopping at the first
  failure, so Quarantine can explain every problem with an event. A rule
  whose execution raises unexpectedly is recorded as a failure for that
  rule and does not prevent the remaining rules from running; the event is
  then never trusted (fail-closed), which keeps a faulty rule from
  silently promoting untrustworthy events to Silver.
- Produces the platform-wide ValidationOutcome contract. It does not
  persist, route, or aggregate.
"""

from __future__ import annotations

from common.contracts import InternalEvent, ValidationFailure, ValidationOutcome
from common.utils import get_logger
from processing.validation.base import ValidationRule, Validator
from processing.validation.rules import default_rules

logger = get_logger(__name__)

__all__ = ["ValidationEngine"]


class ValidationEngine(Validator):
    """Evaluates every configured rule and aggregates their failures.

    Args:
        rules: Ordered rule set. Defaults to :func:`default_rules`.
    """

    def __init__(self, rules: list[ValidationRule] | None = None) -> None:
        self._rules: list[ValidationRule] = rules if rules is not None else default_rules()

    def validate(self, event: InternalEvent) -> ValidationOutcome:
        """Run all rules against the event and aggregate the outcome."""
        failures: list[ValidationFailure] = []
        for rule in self._rules:
            try:
                failures.extend(rule.validate(event))
            except Exception as exc:  # noqa: BLE001 - fail-closed by design
                failures.append(
                    ValidationFailure(
                        rule=rule.name,
                        code="rule_execution_failed",
                        message=f"rule '{rule.name}' raised {type(exc).__name__}: {exc}",
                    )
                )
                logger.error(
                    "validation_rule_execution_failed",
                    rule=rule.name,
                    event_id=event.event_id,
                    error_type=type(exc).__name__,
                )
        outcome = ValidationOutcome(valid=not failures, failures=failures)
        if not outcome.valid:
            logger.info(
                "validation_failed",
                event_id=event.event_id,
                rules=[failure.rule for failure in outcome.failures],
            )
        else:
            logger.debug("validation_passed", event_id=event.event_id)
        return outcome
