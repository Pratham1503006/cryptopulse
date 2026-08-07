from __future__ import annotations

from datetime import UTC, datetime

from fastapi import FastAPI
from pydantic import BaseModel, Field
from uvicorn import Config, Server

from common.utils.logging import get_logger

logger = get_logger(__name__)

health_app = FastAPI(title="cryptopulse-health")


class LivenessResponse(BaseModel):
    status: str = Field(default="alive")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    service: str = Field(default="cryptopulse")


class ReadinessResponse(BaseModel):
    status: str = Field(default="ready")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    service: str = Field(default="cryptopulse")
    dependencies: dict[str, bool] = Field(default_factory=dict)


@health_app.get("/health/live", response_model=LivenessResponse)
async def liveness() -> LivenessResponse:
    return LivenessResponse()


@health_app.get("/health/ready", response_model=ReadinessResponse)
async def readiness() -> ReadinessResponse:
    return ReadinessResponse(
        dependencies={
            "kafka": False,
            "postgres": False,
        }
    )


async def start_health_server(host: str = "0.0.0.0", port: int = 8080) -> None:
    config = Config(
        app=health_app,
        host=host,
        port=port,
        log_level="info",
    )
    server = Server(config=config)
    logger.info("health_server_starting", host=host, port=port)
    await server.serve()
