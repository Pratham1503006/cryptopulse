"""Producer module configuration.

Contains configuration settings specific to the Producer module,
including exchange connection settings and event preparation options.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExchangeSettings(BaseSettings):
    """Configuration for exchange connection."""

    model_config = SettingsConfigDict(env_prefix="exchange_")

    name: str = Field(default="coinbase", description="Exchange identifier")
    websocket_url: str = Field(
        default="wss://ws-feed.exchange.coinbase.com",
        description="WebSocket endpoint URL",
    )
    product_ids: list[str] = Field(
        default_factory=lambda: ["BTC-USD"],
        description="Product IDs to subscribe to",
    )
    max_reconnect_attempts: int = Field(
        default=10,
        description="Maximum number of reconnection attempts before giving up",
    )
    reconnect_delay_seconds: float = Field(
        default=1.0,
        description="Initial delay between reconnection attempts (doubles each retry)",
    )
    max_reconnect_delay_seconds: float = Field(
        default=60.0,
        description="Maximum delay between reconnection attempts",
    )
    heartbeat_interval_seconds: float = Field(
        default=30.0,
        description="Expected heartbeat interval from the exchange",
    )


class ProducerSettings(BaseSettings):
    """Top-level configuration for the Producer module."""

    model_config = SettingsConfigDict(
        env_prefix="producer_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    exchange: ExchangeSettings = Field(default_factory=ExchangeSettings)
    buffer_size: int = Field(
        default=1000,
        description="Maximum number of raw events to buffer before backpressure",
    )
    log_raw_events: bool = Field(
        default=False,
        description="Whether to log raw exchange payloads (debug only)",
    )
