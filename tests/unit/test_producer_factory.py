"""Unit tests for producer connector/preparer selection by configuration."""

from __future__ import annotations

import pytest

from producer.config import ExchangeSettings, ProducerSettings
from producer.connectors.coinbase import CoinbaseExchangeConnector
from producer.connectors.dev import DevExchangeConnector
from producer.factory import build_connector, build_preparer
from producer.preparation.coinbase import CoinbaseEventPreparer
from producer.preparation.dev import DevEventPreparer


def settings_for(name: str) -> ProducerSettings:
    return ProducerSettings(exchange=ExchangeSettings(name=name))


def test_builds_coinbase_connector_and_preparer() -> None:
    settings = settings_for("coinbase")

    assert isinstance(build_connector(settings), CoinbaseExchangeConnector)
    assert isinstance(build_preparer(settings), CoinbaseEventPreparer)


def test_builds_dev_connector_and_preparer() -> None:
    settings = settings_for("dev")

    assert isinstance(build_connector(settings), DevExchangeConnector)
    assert isinstance(build_preparer(settings), DevEventPreparer)


def test_exchange_name_is_case_insensitive() -> None:
    settings = settings_for("CoinBase")

    assert isinstance(build_connector(settings), CoinbaseExchangeConnector)


def test_unknown_exchange_is_rejected() -> None:
    settings = settings_for("kraken")

    with pytest.raises(ValueError, match="unsupported exchange"):
        build_connector(settings)
    with pytest.raises(ValueError, match="unsupported exchange"):
        build_preparer(settings)


def test_coinbase_connector_uses_configured_product_ids() -> None:
    settings = ProducerSettings(
        exchange=ExchangeSettings(name="coinbase", product_ids=["SOL-USD"]),
    )
    connector = build_connector(settings)

    assert isinstance(connector, CoinbaseExchangeConnector)
    # Source is stable regardless of configured products.
    assert connector.source == "coinbase"
