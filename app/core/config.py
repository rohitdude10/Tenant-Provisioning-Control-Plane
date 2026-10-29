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
    inbound_retry_seconds: float = Field(default=2.0, gt=0)
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


@lru_cache
def get_settings() -> Settings:
    return Settings()
