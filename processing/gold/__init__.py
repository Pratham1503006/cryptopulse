"""Gold business-processing engine package.

Derives Gold market-window aggregates from trusted (Silver) events:

    silver -> windows (pure aggregation) -> gold (repository upsert)

- ``windows``: deterministic, side-effect-free aggregation logic
- ``service``: the Silver-to-Gold processing engine

No validation, no transport, no SQL lives here.
"""

from processing.gold.service import GoldProcessingService as GoldProcessingService
from processing.gold.windows import aggregate_window as aggregate_window
from processing.gold.windows import is_final_for as is_final_for
from processing.gold.windows import window_bounds_for as window_bounds_for
from processing.gold.windows import window_start_for as window_start_for

__all__ = [
    "GoldProcessingService",
    "aggregate_window",
    "is_final_for",
    "window_bounds_for",
    "window_start_for",
]
