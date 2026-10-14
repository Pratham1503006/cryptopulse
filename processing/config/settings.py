"""Processing module configuration.

Contains configuration for the business-processing (Gold) engine. All values
are sourced from environment variables (prefix ``GOLD_``) so no
environment-specific values are hardcoded.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GoldSettings(BaseSettings):
    """Configuration for the Silver-to-Gold window aggregation engine."""

    model_config = SettingsConfigDict(
        env_prefix="gold_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    window_seconds: int = Field(
        default=60,
        gt=0,
        description="Fixed UTC time-window size for Gold market aggregates",
    )
    allowed_lateness_seconds: int = Field(
        default=120,
        ge=0,
        description=(
            "How long after a window's end late events may still arrive. A window "
            "becomes final once now >= window_end + allowed_lateness_seconds."
        ),
    )
    vwap_scale: int = Field(
        default=8,
        ge=0,
        description="Decimal places to which VWAP is quantized (ROUND_HALF_UP)",
    )
    batch_size: int = Field(
        default=1000,
        gt=0,
        description="Maximum number of Silver events considered per processing call",
    )
