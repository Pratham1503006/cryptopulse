"""Producer module entry point.

Runs the ingestion vertical slice against the deterministic development
source:

    DevExchangeConnector -> DevEventPreparer -> KafkaPublisher (Kafka)

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
from producer.connectors.dev import DevExchangeConnector
from producer.pipeline import IngestionPipeline
from producer.preparation.dev import DevEventPreparer

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
    """Run one bounded ingestion session using the development source.

    The development connector is finite by design, so a producer run has a
    natural end: every scripted raw event is prepared and published, then
    the pipeline stops cleanly. ``stop`` allows an external caller to end
    the session early (not yet consumed by the finite dev source, but part
    of the lifecycle contract for real connectors).

    Returns:
        The number of InternalEvents published to Kafka.
    """
    configure_logging()
    settings = ProducerSettings()
    stop = stop if stop is not None else asyncio.Event()

    connector = DevExchangeConnector()
    preparer = DevEventPreparer()
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
