"""Producer composition helpers.

Selects the Exchange Connector and Event Preparer implementations from
configuration so the ingestion pipeline can run against either the live
Coinbase feed or the deterministic development source without the pipeline
knowing which is in use.

    settings.exchange.name == "coinbase" -> CoinbaseExchangeConnector
                                            CoinbaseEventPreparer
    settings.exchange.name == "dev"      -> DevExchangeConnector
                                            DevEventPreparer

This keeps the choice of exchange in configuration only, so the pipeline,
publisher, and downstream processing are unchanged between environments.
"""

from __future__ import annotations

from producer.config import ProducerSettings
from producer.connectors.base import ExchangeConnector
from producer.connectors.coinbase import CoinbaseExchangeConnector
from producer.connectors.dev import DevExchangeConnector
from producer.preparation.base import EventPreparer
from producer.preparation.coinbase import CoinbaseEventPreparer
from producer.preparation.dev import DevEventPreparer

__all__ = [
    "SUPPORTED_EXCHANGES",
    "build_connector",
    "build_preparer",
]

SUPPORTED_EXCHANGES = ("coinbase", "dev")


def _normalise(name: str) -> str:
    return name.strip().lower()


def build_connector(settings: ProducerSettings) -> ExchangeConnector:
    """Build the Exchange Connector selected by ``settings.exchange.name``.

    Raises:
        ValueError: If the configured exchange is not supported.
    """
    name = _normalise(settings.exchange.name)
    if name == "coinbase":
        return CoinbaseExchangeConnector(settings.exchange)
    if name == "dev":
        return DevExchangeConnector()
    raise ValueError(
        f"unsupported exchange '{settings.exchange.name}'; "
        f"expected one of {', '.join(SUPPORTED_EXCHANGES)}"
    )


def build_preparer(settings: ProducerSettings) -> EventPreparer:
    """Build the Event Preparer selected by ``settings.exchange.name``.

    Raises:
        ValueError: If the configured exchange is not supported.
    """
    name = _normalise(settings.exchange.name)
    if name == "coinbase":
        return CoinbaseEventPreparer()
    if name == "dev":
        return DevEventPreparer()
    raise ValueError(
        f"unsupported exchange '{settings.exchange.name}'; "
        f"expected one of {', '.join(SUPPORTED_EXCHANGES)}"
    )
