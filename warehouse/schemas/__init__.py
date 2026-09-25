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
from warehouse.schemas.quarantine import (
    QUARANTINE_COLUMNS as QUARANTINE_COLUMNS,
)
from warehouse.schemas.quarantine import QUARANTINE_SCHEMA as QUARANTINE_SCHEMA
from warehouse.schemas.quarantine import QUARANTINE_TABLE as QUARANTINE_TABLE
from warehouse.schemas.quarantine import QuarantineRecord as QuarantineRecord
from warehouse.schemas.quarantine import QuarantineRowError as QuarantineRowError
from warehouse.schemas.quarantine import (
    quarantine_record_to_record as quarantine_record_to_record,
)
from warehouse.schemas.quarantine import (
    record_to_quarantine_record as record_to_quarantine_record,
)
from warehouse.schemas.silver import (
    SILVER_COLUMNS as SILVER_COLUMNS,
)
from warehouse.schemas.silver import SILVER_SCHEMA as SILVER_SCHEMA
from warehouse.schemas.silver import SILVER_TABLE as SILVER_TABLE
from warehouse.schemas.silver import SilverRowError as SilverRowError
from warehouse.schemas.silver import (
    record_to_trusted_event as record_to_trusted_event,
)
from warehouse.schemas.silver import (
    trusted_event_to_record as trusted_event_to_record,
)

__all__ = [
    "BRONZE_COLUMNS",
    "BRONZE_SCHEMA",
    "BRONZE_TABLE",
    "BronzeRowError",
    "QUARANTINE_COLUMNS",
    "QUARANTINE_SCHEMA",
    "QUARANTINE_TABLE",
    "QuarantineRecord",
    "QuarantineRowError",
    "internal_event_to_record",
    "quarantine_record_to_record",
    "record_to_internal_event",
    "record_to_quarantine_record",
    "record_to_trusted_event",
    "SILVER_COLUMNS",
    "SILVER_SCHEMA",
    "SILVER_TABLE",
    "SilverRowError",
    "trusted_event_to_record",
]
