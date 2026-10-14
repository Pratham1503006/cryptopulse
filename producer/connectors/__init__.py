"""Exchange Connector subpackage.

Provides the abstract interface for communicating with external event
sources and the deterministic development/test implementation used by the
ingestion vertical slice.
"""

from producer.connectors.base import ConnectionState as ConnectionState
from producer.connectors.base import ExchangeConnector as ExchangeConnector
from producer.connectors.coinbase import CoinbaseExchangeConnector as CoinbaseExchangeConnector
from producer.connectors.coinbase import CoinbaseExchangeError as CoinbaseExchangeError
from producer.connectors.dev import DEV_RAW_EVENTS as DEV_RAW_EVENTS
from producer.connectors.dev import DevExchangeConnector as DevExchangeConnector

__all__ = [
    "ConnectionState",
    "ExchangeConnector",
    "CoinbaseExchangeConnector",
    "CoinbaseExchangeError",
    "DevExchangeConnector",
    "DEV_RAW_EVENTS",
]
