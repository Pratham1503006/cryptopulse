"""Unit tests for Silver and Quarantine schema mappings.

Verifies row-to-contract mapping in isolation (no PostgreSQL): exact
Decimal preservation, timezone-aware timestamps, enum reconstruction,
structured failure round-trip, and rejection of malformed rows.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from common.contracts import EventType, TradeSide, TrustedEvent, ValidationFailure
from common.exceptions import StorageError
from warehouse.schemas.quarantine import (
    QuarantineRecord,
    quarantine_record_to_record,
    record_to_quarantine_record,
)
from warehouse.schemas.silver import (
    SILVER_COLUMNS,
    record_to_trusted_event,
    trusted_event_to_record,
)

PRECISE_PRICE = Decimal("64001.12345678901234567890")
PRECISE_QUANTITY = Decimal("0.000123456789012345678901234567890")
RECEIVED_AT = datetime(2026, 9, 26, 12, 30, 45, 123456, tzinfo=UTC)
VALIDATED_AT = datetime(2026, 9, 26, 12, 30, 46, tzinfo=UTC)
QUARANTINED_AT = datetime(2026, 9, 26, 12, 30, 47, tzinfo=UTC)
TRADE_TS = datetime(2026, 9, 26, 12, 30, 0, tzinfo=UTC)


def build_trusted() -> TrustedEvent:
    return TrustedEvent(
        event_id="ut-silver-1",
        source="dev",
        event_type=EventType.trade,
        received_at=RECEIVED_AT,
        validated_at=VALIDATED_AT,
        symbol="BTC-USD",
        price=PRECISE_PRICE,
        quantity=PRECISE_QUANTITY,
        side=TradeSide.buy,
        trade_id="trade-xyz",
        trade_timestamp=TRADE_TS,
        payload={"venue": "dev-exchange", "sequence": 99},
    )


def build_record() -> QuarantineRecord:
    from common.contracts import InternalEvent

    event = InternalEvent(
        event_id="ut-quar-1",
        source="dev",
        event_type=EventType.trade,
        received_at=RECEIVED_AT,
        symbol="BTC-USD",
        price=PRECISE_PRICE,
        quantity=PRECISE_QUANTITY,
        side=TradeSide.sell,
        trade_id="trade-xyz",
        trade_timestamp=TRADE_TS,
        payload={"venue": "dev-exchange"},
    )
    return QuarantineRecord(
        event=event,
        quarantined_at=QUARANTINED_AT,
        failures=[
            ValidationFailure(rule="price_validity", code="non_positive", message="price <= 0"),
            ValidationFailure(
                rule="timestamp_validity",
                code="naive_trade_timestamp",
                message="tz missing",
            ),
        ],
    )


class TestSilverMapping:
    def test_record_contains_all_silver_columns(self) -> None:
        record = trusted_event_to_record(build_trusted())
        assert set(record) == set(SILVER_COLUMNS)

    def test_round_trip_preserves_exact_values(self) -> None:
        event = build_trusted()
        reconstructed = record_to_trusted_event(trusted_event_to_record(event))
        assert reconstructed == event
        assert reconstructed.price.as_tuple() == PRECISE_PRICE.as_tuple()
        assert reconstructed.quantity.as_tuple() == PRECISE_QUANTITY.as_tuple()

    def test_round_trip_preserves_timezone_awareness(self) -> None:
        reconstructed = record_to_trusted_event(trusted_event_to_record(build_trusted()))
        for value in (
            reconstructed.received_at,
            reconstructed.trade_timestamp,
            reconstructed.validated_at,
        ):
            assert value.tzinfo is not None
        assert reconstructed.validated_at == VALIDATED_AT

    def test_enums_are_canonical_text(self) -> None:
        record = trusted_event_to_record(build_trusted())
        assert record["event_type"] == "trade"
        assert record["side"] == "buy"

    def test_non_numeric_price_raises_storage_error(self) -> None:
        record = trusted_event_to_record(build_trusted())
        record["price"] = 123.45  # float, not Decimal
        with pytest.raises(StorageError):
            record_to_trusted_event(record)

    def test_unknown_side_raises_storage_error(self) -> None:
        record = trusted_event_to_record(build_trusted())
        record["side"] = "hold"
        with pytest.raises(StorageError):
            record_to_trusted_event(record)


class TestQuarantineMapping:
    def test_round_trip_preserves_record_exactly(self) -> None:
        record = build_record()
        reconstructed = record_to_quarantine_record(quarantine_record_to_record(record))
        assert reconstructed == record
        assert reconstructed.event == record.event
        assert reconstructed.quarantined_at == QUARANTINED_AT
        assert reconstructed.failures == record.failures

    def test_failures_survive_as_structured_json(self) -> None:
        row = quarantine_record_to_record(build_record())
        assert isinstance(row["failures"], list)
        assert row["failures"][0]["rule"] == "price_validity"
        assert row["failures"][0]["code"] == "non_positive"

    def test_decimal_precision_survives(self) -> None:
        reconstructed = record_to_quarantine_record(quarantine_record_to_record(build_record()))
        assert reconstructed.event.price.as_tuple() == PRECISE_PRICE.as_tuple()

    def test_timezone_awareness_survives(self) -> None:
        reconstructed = record_to_quarantine_record(quarantine_record_to_record(build_record()))
        assert reconstructed.quarantined_at.tzinfo is not None
        assert reconstructed.event.trade_timestamp.tzinfo is not None

    def test_non_list_failures_raises_storage_error(self) -> None:
        row = quarantine_record_to_record(build_record())
        row["failures"] = "not-a-list"
        with pytest.raises(StorageError):
            record_to_quarantine_record(row)

    def test_malformed_failure_entry_raises_storage_error(self) -> None:
        row = quarantine_record_to_record(build_record())
        row["failures"] = [{"rule": "r"}]  # missing code/message
        with pytest.raises(StorageError):
            record_to_quarantine_record(row)
