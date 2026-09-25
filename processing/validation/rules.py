"""Initial deterministic validation rules.

The rules below are the platform's initial trust criteria, derived strictly
from the canonical InternalEvent contract (common/contracts/events.py).
Every rule is a pure function of the event, deterministic, and
independently testable.

Deliberate non-goals:

- No freshness windows: the architecture specifies none, and inventing one
  would make valid historical events untrustworthy.
- No business duplicate detection: the architecture defines NO business
  duplicate identity (Bronze explicitly preserves events sharing a
  trade_id, and the canonical contract distinguishes technical event_id
  from business identity). Inventing an identity here would redefine the
  data model through code. Duplicate detection remains a clearly defined
  future rule/interface.
- No exchange-specific symbol whitelists: symbols are checked for canonical
  form, not membership of one provider's product list.
"""

from __future__ import annotations

from decimal import Decimal

from common.contracts import InternalEvent, TradeSide, ValidationFailure
from processing.validation.base import ValidationRule

__all__ = [
    "EventIdentityRule",
    "PriceValidityRule",
    "QuantityValidityRule",
    "RequiredFieldsRule",
    "SymbolValidityRule",
    "TimestampValidityRule",
    "TradeIdentityRule",
    "TradeSideValidityRule",
    "default_rules",
]


class RequiredFieldsRule(ValidationRule):
    """Required canonical fields are present and usable.

    The InternalEvent pydantic contract already guarantees presence of its
    required fields, so this rule verifies usability: strings are non-empty
    and the payload is a JSON object. An event that somehow reached
    validation with missing/non-str identity fields is untrustworthy.
    """

    @property
    def name(self) -> str:
        return "required_fields"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        failures: list[ValidationFailure] = []
        for field in ("event_id", "source", "symbol", "trade_id"):
            value = getattr(event, field)
            if not isinstance(value, str) or not value.strip():
                failures.append(
                    ValidationFailure(
                        rule=self.name,
                        code="missing_or_empty",
                        message=f"required field '{field}' is missing or empty",
                    )
                )
        if not isinstance(event.payload, dict):
            failures.append(
                ValidationFailure(
                    rule=self.name,
                    code="payload_not_object",
                    message="payload must be a JSON object",
                )
            )
        return failures


class PriceValidityRule(ValidationRule):
    """Price must be a finite Decimal greater than zero."""

    @property
    def name(self) -> str:
        return "price_validity"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        price = event.price
        if not isinstance(price, Decimal) or not price.is_finite():
            return [
                ValidationFailure(
                    rule=self.name,
                    code="not_finite_decimal",
                    message="price must be a finite Decimal",
                )
            ]
        if price <= 0:
            return [
                ValidationFailure(
                    rule=self.name,
                    code="non_positive",
                    message=f"price must be positive, got {price}",
                )
            ]
        return []


class QuantityValidityRule(ValidationRule):
    """Quantity must be a finite Decimal greater than zero."""

    @property
    def name(self) -> str:
        return "quantity_validity"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        quantity = event.quantity
        if not isinstance(quantity, Decimal) or not quantity.is_finite():
            return [
                ValidationFailure(
                    rule=self.name,
                    code="not_finite_decimal",
                    message="quantity must be a finite Decimal",
                )
            ]
        if quantity <= 0:
            return [
                ValidationFailure(
                    rule=self.name,
                    code="non_positive",
                    message=f"quantity must be positive, got {quantity}",
                )
            ]
        return []


class TradeSideValidityRule(ValidationRule):
    """Side must be a supported canonical TradeSide value."""

    @property
    def name(self) -> str:
        return "trade_side_validity"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        if isinstance(event.side, TradeSide):
            return []
        return [
            ValidationFailure(
                rule=self.name,
                code="unsupported_side",
                message=f"side must be one of {', '.join(s.value for s in TradeSide)}",
            )
        ]


class TimestampValidityRule(ValidationRule):
    """trade_timestamp (and received_at) must be timezone-aware.

    Timestamps without timezone information cannot be ordered reliably
    against other events and are therefore untrustworthy. Freshness is
    deliberately NOT checked: no architecture document defines a freshness
    window, and historical events remain valid market history.
    """

    @property
    def name(self) -> str:
        return "timestamp_validity"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        failures: list[ValidationFailure] = []
        if (
            event.trade_timestamp.tzinfo is None
            or event.trade_timestamp.tzinfo.utcoffset(event.trade_timestamp) is None
        ):
            failures.append(
                ValidationFailure(
                    rule=self.name,
                    code="naive_trade_timestamp",
                    message="trade_timestamp must be timezone-aware",
                )
            )
        if (
            event.received_at.tzinfo is None
            or event.received_at.tzinfo.utcoffset(event.received_at) is None
        ):
            failures.append(
                ValidationFailure(
                    rule=self.name,
                    code="naive_received_at",
                    message="received_at must be timezone-aware",
                )
            )
        return failures


class SymbolValidityRule(ValidationRule):
    """Symbol must be non-empty and canonical in form.

    Canonical form is deliberately minimal: non-blank and without
    surrounding whitespace, so any exchange's pair notation survives. No
    provider-specific whitelist is applied.
    """

    @property
    def name(self) -> str:
        return "symbol_validity"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        symbol = event.symbol
        if not isinstance(symbol, str) or not symbol.strip() or symbol != symbol.strip():
            return [
                ValidationFailure(
                    rule=self.name,
                    code="invalid_symbol",
                    message="symbol must be a non-blank string without surrounding whitespace",
                )
            ]
        return []


class EventIdentityRule(ValidationRule):
    """event_id must be present and non-empty.

    This checks technical event identity (Bronze's primary key), which is
    distinct from business duplicate detection.
    """

    @property
    def name(self) -> str:
        return "event_identity"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        event_id = event.event_id
        if isinstance(event_id, str) and event_id.strip():
            return []
        return [
            ValidationFailure(
                rule=self.name,
                code="missing_event_id",
                message="event_id must be present and non-empty",
            )
        ]


class TradeIdentityRule(ValidationRule):
    """trade_id must be present and non-empty.

    Presence/quality of the exchange trade identifier only. Repeated
    trade_id values across events are NOT a duplicate signal: multiple
    received events may legitimately share one trade_id, and business
    duplicate detection is not defined by the architecture yet.
    """

    @property
    def name(self) -> str:
        return "trade_identity"

    def validate(self, event: InternalEvent) -> list[ValidationFailure]:
        trade_id = event.trade_id
        if isinstance(trade_id, str) and trade_id.strip():
            return []
        return [
            ValidationFailure(
                rule=self.name,
                code="missing_trade_id",
                message="trade_id must be present and non-empty",
            )
        ]


def default_rules() -> list[ValidationRule]:
    """The platform's initial rule set, in stable evaluation order."""
    return [
        RequiredFieldsRule(),
        EventIdentityRule(),
        TradeIdentityRule(),
        SymbolValidityRule(),
        TradeSideValidityRule(),
        PriceValidityRule(),
        QuantityValidityRule(),
        TimestampValidityRule(),
    ]
