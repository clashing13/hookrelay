"""Environment-backed application configuration."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Validated process configuration loaded from ``HOOKRELAY_*`` variables."""

    model_config = SettingsConfigDict(
        env_prefix="HOOKRELAY_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        hide_input_in_errors=True,
    )

    service_name: Literal["hookrelay"] = "hookrelay"
    version: Literal["0.1.0"] = "0.1.0"
    environment: Literal["local", "test", "staging", "production"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    database_url: SecretStr = SecretStr(
        "postgresql+asyncpg://hookrelay:hookrelay@localhost:5432/hookrelay"
    )
    database_pool_size: int = Field(default=5, ge=1, le=50)
    database_max_overflow: int = Field(default=10, ge=0, le=100)
    readiness_timeout_seconds: float = Field(default=2.0, gt=0, le=30)

    @field_validator("database_url", mode="before")
    @classmethod
    def require_async_postgresql(cls, value: object) -> object:
        """Reject database drivers that cannot support the async PostgreSQL design."""

        raw_value = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
        if not raw_value.startswith("postgresql+asyncpg://"):
            msg = "database_url must use the postgresql+asyncpg driver"
            raise ValueError(msg)
        return value


@lru_cache
def get_settings() -> Settings:
    """Create settings once for the lifetime of the process."""

    return Settings()
