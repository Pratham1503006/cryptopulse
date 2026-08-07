from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="kafka_")

    bootstrap_servers: str = Field(default="localhost:9092")
    topic_prefix: str = Field(default="cryptopulse")
    consumer_group: str = Field(default="cryptopulse-processing")
    session_timeout_ms: int = Field(default=45000)
    enable_auto_commit: bool = Field(default=False)


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="postgres_")

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="cryptopulse")
    username: str = Field(default="cryptopulse")
    password: str = Field(default="cryptopulse")
    min_connections: int = Field(default=2)
    max_connections: int = Field(default=10)


class PrometheusSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="prometheus_")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=9090)


class GrafanaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="grafana_")

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=3000)
    admin_user: str = Field(default="admin")
    admin_password: str = Field(default="admin")


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = Field(default="cryptopulse")
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")

    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    prometheus: PrometheusSettings = Field(default_factory=PrometheusSettings)
    grafana: GrafanaSettings = Field(default_factory=GrafanaSettings)

    health_host: str = Field(default="0.0.0.0")
    health_port: int = Field(default=8080)


def load_settings() -> AppSettings:
    return AppSettings()
