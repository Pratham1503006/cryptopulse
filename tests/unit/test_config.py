"""Verify that configuration loading works."""

from common.config import AppSettings, load_settings


def test_load_settings_returns_app_settings() -> None:
    settings = load_settings()
    assert isinstance(settings, AppSettings)
    assert settings.app_name == "cryptopulse"


def test_settings_defaults() -> None:
    settings = load_settings()
    assert settings.postgres.host == "localhost"
    assert settings.postgres.port == 5432
    assert settings.prometheus.port == 9090
    assert settings.grafana.port == 3000
    assert settings.health_port == 8080
