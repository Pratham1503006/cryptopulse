"""Verify that the messaging module components import correctly."""

from messaging.config import KafkaSettings


def test_messaging_imports() -> None:
    from messaging import (
        ConsumeError,
        Consumer,
        InternalEventCodec,
        KafkaConsumer,
        KafkaPublisher,
        MessagingError,
        Publisher,
        PublishError,
        SerializationError,
    )

    assert InternalEventCodec is not None
    assert Publisher is not None
    assert KafkaPublisher is not None
    assert Consumer is not None
    assert KafkaConsumer is not None
    assert SerializationError is not None
    assert PublishError is not None
    assert ConsumeError is not None
    assert MessagingError is not None


def test_kafka_settings_defaults() -> None:
    settings = KafkaSettings()
    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.market_events_topic == "cryptopulse.market.events"
    assert settings.consumer_group == "cryptopulse-processing"
    assert settings.auto_offset_reset == "earliest"
    assert settings.enable_auto_commit is False
    assert settings.session_timeout_ms == 45000
    assert settings.producer_retry_backoff_ms == 100
