"""Producer module - Exchange Connector and Event Preparation.

Implements two architectural responsibilities:

1. Exchange Connector - communicates with external event sources
2. Event Preparation - translates exchange payloads into InternalEvent

This module depends ONLY on common/. It has NO dependencies on
processing/, warehouse/, analytics/, or monitoring/.
"""

from producer.config import ExchangeSettings as ExchangeSettings
from producer.config import ProducerSettings as ProducerSettings
from producer.connectors import ConnectionState as ConnectionState
from producer.connectors import ExchangeConnector as ExchangeConnector
from producer.preparation import EventPreparer as EventPreparer
from producer.preparation import PreparationError as PreparationError

__all__ = [
    # Configuration
    "ExchangeSettings",
    "ProducerSettings",
    # Exchange Connector
    "ConnectionState",
    "ExchangeConnector",
    # Event Preparation
    "EventPreparer",
    "PreparationError",
]
