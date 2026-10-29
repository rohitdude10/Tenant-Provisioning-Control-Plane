from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = (
        "postgresql+psycopg://tenant_app:replace-me@localhost:5432/tenant_control_plane"
    )
    rabbitmq_url: str = "amqp://tenant_app:replace-me@localhost:5672/tenant_control_plane"
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = "INFO"
    outbox_poll_seconds: float = Field(default=1.0, gt=0)
    outbox_retry_base_seconds: float = Field(default=1.0, gt=0)
    outbox_retry_max_seconds: float = Field(default=30.0, gt=0)
    inbound_retry_seconds: float = Field(default=2.0, gt=0)
    inbound_retry_max_seconds: float = Field(default=30.0, gt=0)
    inbound_retry_max_attempts: int = Field(default=5, ge=1, le=10)
    worker_fail_rate: float = Field(default=0.0, ge=0, le=1)
    worker_min_delay_ms: int = Field(default=250, ge=0)
    worker_max_delay_ms: int = Field(default=1000, ge=0)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @field_validator("database_url")
    @classmethod
    def require_postgresql(cls, value: str) -> str:
        if not value.startswith(("postgresql+psycopg://", "postgresql://")):
            raise ValueError("DATABASE_URL must use PostgreSQL; SQLite is not supported")
        return value

    @field_validator("rabbitmq_url")
    @classmethod
    def require_amqp(cls, value: str) -> str:
        if not value.startswith(("amqp://", "amqps://")):
            raise ValueError("RABBITMQ_URL must use RabbitMQ AMQP")
        return value

    @field_validator("worker_max_delay_ms")
    @classmethod
    def validate_delay_range(cls, value: int, info) -> int:
        minimum = info.data.get("worker_min_delay_ms", 0)
        if value < minimum:
            raise ValueError(
                "WORKER_MAX_DELAY_MS must be greater than or equal to WORKER_MIN_DELAY_MS"
            )
        return value

    @field_validator("outbox_retry_max_seconds")
    @classmethod
    def validate_outbox_retry_range(cls, value: float, info) -> float:
        base = info.data.get("outbox_retry_base_seconds", 1.0)
        if value < base:
            raise ValueError(
                "OUTBOX_RETRY_MAX_SECONDS must be greater than or equal to "
                "OUTBOX_RETRY_BASE_SECONDS"
            )
        return value

    @field_validator("inbound_retry_max_seconds")
    @classmethod
    def validate_inbound_retry_range(cls, value: float, info) -> float:
        base = info.data.get("inbound_retry_seconds", 2.0)
        if value < base:
            raise ValueError(
                "INBOUND_RETRY_MAX_SECONDS must be greater than or equal to INBOUND_RETRY_SECONDS"
            )
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
