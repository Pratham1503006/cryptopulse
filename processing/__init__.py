"""Processing module.

Currently hosts the Bronze ingestion service, the consumer-side
application of the ingestion vertical slice:

    Kafka -> InternalEvent -> Bronze

The Validation Engine and Processing Engine will live here in later
milestones. This module depends on common/, messaging/, and warehouse/;
the Bronze ingestion service is the application boundary that connects
messaging to warehouse.
"""

from processing.bronze_ingestion import BronzeIngestionService as BronzeIngestionService

__all__ = [
    "BronzeIngestionService",
]
