"""Unit tests for the validation contracts, rules, and engine.

All tests construct their own deterministic events; no database or
messaging infrastructure is involved.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

from common.contracts import (
    EventType,
    InternalEvent,
    TradeSide,
    ValidationFailure,
    ValidationOutcome,
)
from processing.validation import ValidationEngine, ValidationRule, default_rules
from processing.validation.base import Validator

PRECISE_PRICE = Decimal("64001.12345678901234567890")
PRECISE_QUANTITY = Decimal("0.000123456789012345678901234567890")


def build_event(**overrides: Any) -> InternalEvent:
    defaults: dict[str, Any] = {
        "event_id": "evt-001",
        "source": "dev",
        "event_type": EventType.trade,
        "received_at": datetime(2026, 8, 26, 10, 10, 5, tzinfo=UTC),
        "symbol": "BTC-USD",
        "price": PRECISE_PRICE,
        "quantity": PRECISE_QUANTITY,
        "side": TradeSide.buy,
        "trade_id": "trade-001",
        "trade_timestamp": datetime(2026, 8, 26, 10, 10, 0, tzinfo=UTC),
        "payload": {"venue": "dev-exchange"},
    }
    defaults.update(overrides)
    return InternalEvent(**defaults)


# ---------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------


class TestValidationContracts:
    def test_outcome_valid_has_no_failures(self) -> None:
        outcome = ValidationOutcome(valid=True)
        assert outcome.valid is True
        assert outcome.failures == []

    def test_outcome_invalid_lists_all_failures(self) -> None:
        failures = [
            ValidationFailure(rule="r1", code="c1", message="m1"),
            ValidationFailure(rule="r2", code="c2", message="m2"),
        ]
        outcome = ValidationOutcome(valid=False, failures=failures)
        assert outcome.valid is False
        assert len(outcome.failures) == 2
        assert outcome.failures[0].rule == "r1"
        assert outcome.failures[1].code == "c2"

    def test_failure_is_frozen(self) -> None:
        failure = ValidationFailure(rule="r", code="c", message="m")
        with pytest.raises(PydanticValidationError):
            failure.rule = "other"

    def test_outcome_rejects_contradictory_shape_is_allowed(self) -> None:
        """The contract does not police valid/failures consistency: the
        engine is the only producer and always keeps them consistent."""
        outcome = ValidationOutcome(valid=True, failures=[])
        assert outcome.valid


# ---------------------------------------------------------------------------
# Individual rules
# ---------------------------------------------------------------------------


class TestRequiredFieldsRule:
    def test_valid_event_passes(self) -> None:
        from processing.validation.rules import RequiredFieldsRule

        assert RequiredFieldsRule().validate(build_event()) == []

    @pytest.mark.parametrize("field", ["event_id", "source", "symbol", "trade_id"])
    def test_blank_field_fails(self, field: str) -> None:
        from processing.validation.rules import RequiredFieldsRule

        event = build_event(**{field: "   "})
        failures = RequiredFieldsRule().validate(event)
        assert len(failures) == 1
        assert failures[0].code == "missing_or_empty"
        assert field in failures[0].message


class TestPriceValidityRule:
    def test_positive_price_passes(self) -> None:
        from processing.validation.rules import PriceValidityRule

        assert PriceValidityRule().validate(build_event()) == []

    def test_zero_price_fails(self) -> None:
        from processing.validation.rules import PriceValidityRule

        failures = PriceValidityRule().validate(build_event(price=Decimal("0")))
        assert failures[0].code == "non_positive"

    def test_negative_price_fails(self) -> None:
        from processing.validation.rules import PriceValidityRule

        failures = PriceValidityRule().validate(build_event(price=Decimal("-1.5")))
        assert failures[0].code == "non_positive"

    def test_infinite_price_fails(self) -> None:
        from processing.validation.rules import PriceValidityRule

        # The pydantic contract already blocks non-finite values at
        # construction; model_construct bypasses it so the rule's own
        # defense-in-depth check is exercised directly.
        event = InternalEvent.model_construct(
            **{**build_event().model_dump(), "price": Decimal("Infinity")}
        )
        failures = PriceValidityRule().validate(event)
        assert failures[0].code == "not_finite_decimal"


class TestQuantityValidityRule:
    def test_positive_quantity_passes(self) -> None:
        from processing.validation.rules import QuantityValidityRule

        assert QuantityValidityRule().validate(build_event()) == []

    def test_zero_quantity_fails(self) -> None:
        from processing.validation.rules import QuantityValidityRule

        failures = QuantityValidityRule().validate(build_event(quantity=Decimal("0")))
        assert failures[0].code == "non_positive"

    def test_nan_quantity_fails(self) -> None:
        from processing.validation.rules import QuantityValidityRule

        # Contract blocks NaN at construction; model_construct bypasses it
        # to exercise the rule's defense-in-depth finiteness check.
        event = InternalEvent.model_construct(
            **{**build_event().model_dump(), "quantity": Decimal("NaN")}
        )
        failures = QuantityValidityRule().validate(event)
        assert failures[0].code == "not_finite_decimal"


class TestTradeSideValidityRule:
    def test_canonical_sides_pass(self) -> None:
        from processing.validation.rules import TradeSideValidityRule

        for side in TradeSide:
            assert TradeSideValidityRule().validate(build_event(side=side)) == []

    def test_non_canonical_side_fails(self) -> None:
        """A value bypassing the StrEnum contract is not a canonical side."""
        from processing.validation.rules import TradeSideValidityRule

        # The contract blocks invalid sides at construction; model_construct
        # bypasses it to exercise the rule's own canonical-value check.
        event = InternalEvent.model_construct(**{**build_event().model_dump(), "side": "hold"})
        failures = TradeSideValidityRule().validate(event)
        assert failures[0].code == "unsupported_side"


class TestTimestampValidityRule:
    def test_timezone_aware_passes(self) -> None:
        from processing.validation.rules import TimestampValidityRule

        assert TimestampValidityRule().validate(build_event()) == []

    def test_non_utc_offset_passes(self) -> None:
        """Any explicit offset is timezone-aware, not just UTC."""
        from processing.validation.rules import TimestampValidityRule

        offset = timezone(timedelta(hours=5))
        event = build_event(
            trade_timestamp=datetime(2026, 8, 26, 15, 10, 0, tzinfo=offset),
            received_at=datetime(2026, 8, 26, 15, 10, 5, tzinfo=offset),
        )
        assert TimestampValidityRule().validate(event) == []

    def test_naive_trade_timestamp_fails(self) -> None:
        from processing.validation.rules import TimestampValidityRule

        event = build_event(trade_timestamp=datetime(2026, 8, 26, 10, 10, 0))  # noqa: DTZ001
        failures = TimestampValidityRule().validate(event)
        assert failures[0].code == "naive_trade_timestamp"

    def test_naive_received_at_fails(self) -> None:
        from processing.validation.rules import TimestampValidityRule

        event = build_event(received_at=datetime(2026, 8, 26, 10, 10, 5))  # noqa: DTZ001
        failures = TimestampValidityRule().validate(event)
        assert failures[0].code == "naive_received_at"


class TestSymbolValidityRule:
    def test_canonical_symbol_passes(self) -> None:
        from processing.validation.rules import SymbolValidityRule

        assert SymbolValidityRule().validate(build_event()) == []

    def test_other_exchange_notation_passes(self) -> None:
        """No exchange-specific whitelist: any provider notation survives."""
        from processing.validation.rules import SymbolValidityRule

        assert SymbolValidityRule().validate(build_event(symbol="XBT/USDT")) == []
        assert SymbolValidityRule().validate(build_event(symbol="ethusdt")) == []

    def test_blank_symbol_fails(self) -> None:
        from processing.validation.rules import SymbolValidityRule

        failures = SymbolValidityRule().validate(build_event(symbol="   "))
        assert failures[0].code == "invalid_symbol"

    def test_unpadded_symbol_fails(self) -> None:
        from processing.validation.rules import SymbolValidityRule

        failures = SymbolValidityRule().validate(build_event(symbol=" BTC-USD "))
        assert failures[0].code == "invalid_symbol"


class TestIdentityRules:
    def test_valid_ids_pass(self) -> None:
        from processing.validation.rules import EventIdentityRule, TradeIdentityRule

        assert EventIdentityRule().validate(build_event()) == []
        assert TradeIdentityRule().validate(build_event()) == []

    def test_empty_event_id_fails(self) -> None:
        from processing.validation.rules import EventIdentityRule

        failures = EventIdentityRule().validate(build_event(event_id=""))
        assert failures[0].code == "missing_event_id"

    def test_empty_trade_id_fails(self) -> None:
        from processing.validation.rules import TradeIdentityRule

        failures = TradeIdentityRule().validate(build_event(trade_id=""))
        assert failures[0].code == "missing_trade_id"

    def test_repeated_trade_id_is_not_rejected(self) -> None:
        """Multiple events sharing one trade_id are NOT duplicates."""
        from processing.validation.rules import TradeIdentityRule

        first = build_event(trade_id="shared-trade-id")
        second = build_event(trade_id="shared-trade-id", price=Decimal("1"))
        assert TradeIdentityRule().validate(first) == []
        assert TradeIdentityRule().validate(second) == []


# ---------------------------------------------------------------------------
# Engine composition
# ---------------------------------------------------------------------------


class TestValidationEngine:
    def test_valid_event_passes_all_rules(self) -> None:
        engine = ValidationEngine()
        outcome = engine.validate(build_event())
        assert outcome.valid is True
        assert outcome.failures == []

    def test_engine_evaluates_all_rules_and_collects_multiple_failures(self) -> None:
        event = build_event(
            price=Decimal("0"),
            quantity=Decimal("-1"),
            trade_timestamp=datetime(2026, 8, 26, 10, 10, 0),  # noqa: DTZ001
        )
        engine = ValidationEngine()
        outcome = engine.validate(event)

        assert outcome.valid is False
        failed_rules = {failure.rule for failure in outcome.failures}
        # Not short-circuited: price, quantity AND timestamp all reported.
        assert failed_rules == {"price_validity", "quantity_validity", "timestamp_validity"}
        assert len(outcome.failures) == 3

    def test_engine_reports_every_failing_required_field(self) -> None:
        event = build_event(event_id="", trade_id="", symbol="  ")
        outcome = ValidationEngine().validate(event)

        required_failures = [
            failure for failure in outcome.failures if failure.rule == "required_fields"
        ]
        assert len(required_failures) == 3

    def test_engine_uses_configured_rules_only(self) -> None:
        class AlwaysFails(ValidationRule):
            @property
            def name(self) -> str:
                return "always_fails"

            def validate(self, event: InternalEvent) -> list[ValidationFailure]:
                return [ValidationFailure(rule=self.name, code="no", message="rejected")]

        engine = ValidationEngine(rules=[AlwaysFails()])
        outcome = engine.validate(build_event())
        assert outcome.valid is False
        assert [failure.rule for failure in outcome.failures] == ["always_fails"]

    def test_faulty_rule_fails_closed(self) -> None:
        class Explodes(ValidationRule):
            @property
            def name(self) -> str:
                return "explodes"

            def validate(self, event: InternalEvent) -> list[ValidationFailure]:
                raise RuntimeError("boom")

        engine = ValidationEngine(rules=[Explodes()])
        outcome = engine.validate(build_event())
        assert outcome.valid is False
        assert outcome.failures[0].rule == "explodes"
        assert outcome.failures[0].code == "rule_execution_failed"

    def test_engine_is_a_validator(self) -> None:
        assert isinstance(ValidationEngine(), Validator)

    def test_default_rules_cover_all_eight_concerns(self) -> None:
        names = [rule.name for rule in default_rules()]
        assert names == [
            "required_fields",
            "event_identity",
            "trade_identity",
            "symbol_validity",
            "trade_side_validity",
            "price_validity",
            "quantity_validity",
            "timestamp_validity",
        ]


# ---------------------------------------------------------------------------
# TrustedEvent conversion
# ---------------------------------------------------------------------------


class TestTrustedEventConversion:
    def test_valid_event_maps_to_trusted_event(self) -> None:
        engine = ValidationEngine()
        event = build_event()
        outcome = engine.validate(event)
        assert outcome.valid

        trusted = engine.trusted_event_from(event)
        assert trusted.event_id == event.event_id
        assert trusted.source == event.source
        assert trusted.event_type is event.event_type
        assert trusted.symbol == event.symbol
        assert trusted.price == event.price
        assert trusted.quantity == event.quantity
        assert trusted.side is event.side
        assert trusted.trade_id == event.trade_id
        assert trusted.trade_timestamp == event.trade_timestamp
        assert trusted.received_at == event.received_at
        assert trusted.payload == event.payload
        assert trusted.validated_at.tzinfo is not None

    def test_conversion_is_deterministic_in_fields(self) -> None:
        engine = ValidationEngine()
        event = build_event()
        first = engine.trusted_event_from(event)
        second = engine.trusted_event_from(event)
        assert first.model_dump() == second.model_dump() | {"validated_at": first.validated_at}
