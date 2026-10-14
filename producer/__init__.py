"""Producer module - Exchange Connector and Event Preparation.

Implements two architectural responsibilities:

1. Exchange Connector - communicates with external event sources
2. Event Preparation - translates exchange payloads into InternalEvent

This module depends ONLY on common/ and messaging/. It has NO
dependencies on processing/, warehouse/, analytics/, or monitoring/.
"""

from producer.config import ExchangeSettings as ExchangeSettings
from producer.config import ProducerSettings as ProducerSettings
from producer.connectors import CoinbaseExchangeConnector as CoinbaseExchangeConnector
from producer.connectors import CoinbaseExchangeError as CoinbaseExchangeError
from producer.connectors import ConnectionState as ConnectionState
from producer.connectors import DevExchangeConnector as DevExchangeConnector
from producer.connectors import ExchangeConnector as ExchangeConnector
from producer.factory import build_connector as build_connector
from producer.factory import build_preparer as build_preparer
from producer.pipeline import IngestionPipeline as IngestionPipeline
from producer.preparation import CoinbaseEventPreparer as CoinbaseEventPreparer
from producer.preparation import DevEventPreparer as DevEventPreparer
from producer.preparation import EventPreparer as EventPreparer
from producer.preparation import PreparationError as PreparationError

__all__ = [
    # Configuration
    "ExchangeSettings",
    "ProducerSettings",
    # Exchange Connector
    "ConnectionState",
    "ExchangeConnector",
    "CoinbaseExchangeConnector",
    "CoinbaseExchangeError",
    "DevExchangeConnector",
    # Event Preparation
    "EventPreparer",
    "PreparationError",
    "CoinbaseEventPreparer",
    "DevEventPreparer",
    # Composition
    "build_connector",
    "build_preparer",
    # Orchestration
    "IngestionPipeline",
]
