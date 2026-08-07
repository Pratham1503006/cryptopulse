"""Verify health endpoints."""

import pytest
from httpx import ASGITransport, AsyncClient

from monitoring.health.server import health_app


@pytest.mark.asyncio
async def test_liveness_endpoint() -> None:
    transport = ASGITransport(app=health_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/live")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "alive"
        assert data["service"] == "cryptopulse"


@pytest.mark.asyncio
async def test_readiness_endpoint() -> None:
    transport = ASGITransport(app=health_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ready"
        assert "kafka" in data["dependencies"]
        assert "postgres" in data["dependencies"]
