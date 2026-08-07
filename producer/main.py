from __future__ import annotations

from common.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


def main() -> None:
    configure_logging()
    logger.info("producer_starting", module="producer")
