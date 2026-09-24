"""warehouse.schemas - physical representations of the data layers.

Each submodule owns the mapping between a canonical contract and its
PostgreSQL representation. No SQL execution happens here; repositories
own execution.
"""

from warehouse.schemas.bronze import (
    BRONZE_COLUMNS as BRONZE_COLUMNS,
)
from warehouse.schemas.bronze import BRONZE_SCHEMA as BRONZE_SCHEMA
from warehouse.schemas.bronze import BRONZE_TABLE as BRONZE_TABLE
from warehouse.schemas.bronze import BronzeRowError as BronzeRowError
from warehouse.schemas.bronze import (
    internal_event_to_record as internal_event_to_record,
)
from warehouse.schemas.bronze import (
    record_to_internal_event as record_to_internal_event,
)

__all__ = [
    "BRONZE_COLUMNS",
    "BRONZE_SCHEMA",
    "BRONZE_TABLE",
    "BronzeRowError",
    "internal_event_to_record",
    "record_to_internal_event",
]
