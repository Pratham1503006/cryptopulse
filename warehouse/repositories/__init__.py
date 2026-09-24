"""warehouse.repositories - persistence implementations.

Implements the repository abstractions defined in ``base.py``. Concrete
classes here own the SQL and the driver usage; the abstractions stay
technology-neutral.
"""

from warehouse.repositories.base import BronzeRepository as BronzeRepository
from warehouse.repositories.base import GoldRepository as GoldRepository
from warehouse.repositories.base import QuarantineRepository as QuarantineRepository
from warehouse.repositories.base import SilverRepository as SilverRepository
from warehouse.repositories.bronze import PostgresBronzeRepository as PostgresBronzeRepository

__all__ = [
    "BronzeRepository",
    "GoldRepository",
    "PostgresBronzeRepository",
    "QuarantineRepository",
    "SilverRepository",
]
