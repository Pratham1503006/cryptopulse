"""Exchange Connector subpackage.

Provides the abstract interface for communicating with external event sources.
"""

from producer.connectors.base import ConnectionState as ConnectionState
from producer.connectors.base import ExchangeConnector as ExchangeConnector

__all__ = [
    "ConnectionState",
    "ExchangeConnector",
]
