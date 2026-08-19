"""Messaging module configuration.

Contains Kafka-specific settings. Kafka configuration is owned by the
messaging/ module rather than common/ because it is transport-specific,
not a technology-neutral platform contract.

All values are sourced from environment variables (prefix ``KAFKA_``) so no
environment-specific values or credentials are hardcoded.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    """Configuration for the event-streaming backbone.

    Provides the connection and behaviour settings shared by both the
    publisher and consumer surfaces of the messaging module.
    """

    model_config = SettingsConfigDict(
        env_prefix="kafka_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers (host:port)",
    )
    market_events_topic: str = Field(
        default="cryptopulse.market.events",
        description="Topic that carries standardised InternalEvent payloads",
    )
    consumer_group: str = Field(
        default="cryptopulse-processing",
        description="Default consumer group for downstream processing",
    )
    auto_offset_reset: str = Field(
        default="earliest",
        description="Where to start reading when no committed offset exists",
    )
    enable_auto_commit: bool = Field(
        default=False,
        description="Whether the consumer commits offsets automatically",
    )
    session_timeout_ms: int = Field(
        default=45000,
        description="Consumer session timeout in milliseconds",
    )
    producer_retry_backoff_ms: int = Field(
        default=100,
        description="Producer delay in milliseconds between retries of transient failures",
    )
