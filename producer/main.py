"""Producer module entry point.

Runs the ingestion vertical slice against the exchange selected by
configuration (``EXCHANGE_NAME``):

    ExchangeConnector -> EventPreparer -> KafkaPublisher (Kafka)

``coinbase`` uses the live Coinbase feed; ``dev`` uses the deterministic
local source. The pipeline itself is identical for both.

Usage:
    python -m producer.main
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys
from types import FrameType
from typing import TYPE_CHECKING

from common.utils import configure_logging, get_logger
from messaging import KafkaPublisher, KafkaSettings
from producer.config import ProducerSettings
from producer.factory import build_connector, build_preparer
from producer.pipeline import IngestionPipeline

if TYPE_CHECKING:
    from asyncio import Event

logger = get_logger(__name__)


def _install_stop_signal(stop: Event) -> None:
    """Request graceful shutdown on SIGINT/SIGTERM where supported."""

    def handle_signal(signum: int, frame: FrameType | None) -> None:
        logger.info("producer_shutdown_requested", signal=signal.Signals(signum).name)
        stop.set()

    with contextlib.suppress(NotImplementedError, ValueError):
        # Windows supports SIGINT; SIGTERM raises NotImplementedError there.
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)


async def run_producer(stop: Event | None = None) -> int:
    """Run one ingestion session against the configured exchange.

    The connector and preparer are selected from configuration
    (``EXCHANGE_NAME``). The deterministic ``dev`` source is finite, so a dev
    run ends after its scripted events; the live ``coinbase`` source runs
    until ``stop`` is requested (or the run fails).

    ``stop`` is the lifecycle contract for ending a live session: the
    composition root sets it on SIGINT/SIGTERM.

    Returns:
        The number of InternalEvents published to Kafka.
    """
    configure_logging()
    settings = ProducerSettings()
    stop = stop if stop is not None else asyncio.Event()

    connector = build_connector(settings)
    preparer = build_preparer(settings)
    publisher = KafkaPublisher(KafkaSettings())

    await publisher.start()
    try:
        pipeline = IngestionPipeline(
            connector=connector,
            preparer=preparer,
            publisher=publisher,
        )
        published = await pipeline.run()
    finally:
        await publisher.close()

    logger.info("producer_run_complete", published=published, exchange=settings.exchange.name)
    return published


def main() -> None:
    """Start the Producer module."""
    stop = asyncio.Event()
    _install_stop_signal(stop)
    try:
        published = asyncio.run(run_producer(stop))
    except Exception as exc:  # pragma: no cover - defensive entry-point guard
        logger.error("producer_run_failed", error=str(exc))
        sys.exit(1)
    logger.info("producer_exited", published=published)


if __name__ == "__main__":
    main()
