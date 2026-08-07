"""Environment-backed application configuration."""

import base64
import binascii
from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEVELOPMENT_SECRET_ENCRYPTION_KEY = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="


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
    version: Literal["0.2.0"] = "0.2.0"
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
    secret_encryption_key: SecretStr = SecretStr(DEVELOPMENT_SECRET_ENCRYPTION_KEY)
    secret_encryption_key_version: int = Field(default=1, ge=1, le=32767)
    bootstrap_enabled: bool = False
    bootstrap_token: SecretStr | None = None

    @field_validator("database_url", mode="before")
    @classmethod
    def require_async_postgresql(cls, value: object) -> object:
        """Reject database drivers that cannot support the async PostgreSQL design."""

        raw_value = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
        if not raw_value.startswith("postgresql+asyncpg://"):
            msg = "database_url must use the postgresql+asyncpg driver"
            raise ValueError(msg)
        return value

    @field_validator("secret_encryption_key", mode="before")
    @classmethod
    def require_256_bit_encryption_key(cls, value: object) -> object:
        """Require one URL-safe base64-encoded 256-bit AES key."""

        raw_value = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
        try:
            decoded = base64.b64decode(raw_value, altchars=b"-_", validate=True)
        except (binascii.Error, ValueError) as exc:
            msg = "secret_encryption_key must be URL-safe base64"
            raise ValueError(msg) from exc
        if len(decoded) != 32:
            msg = "secret_encryption_key must decode to exactly 32 bytes"
            raise ValueError(msg)
        if base64.urlsafe_b64encode(decoded).decode("ascii") != raw_value:
            msg = "secret_encryption_key must use canonical URL-safe base64"
            raise ValueError(msg)
        return value

    @field_validator("bootstrap_token", mode="before")
    @classmethod
    def require_bounded_ascii_bootstrap_token(cls, value: object) -> object:
        """Keep constant-time byte comparison defined and header parsing bounded."""

        if value is None:
            return value
        raw_value = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
        try:
            encoded = raw_value.encode("ascii")
        except UnicodeEncodeError as exc:
            msg = "bootstrap_token must contain only ASCII characters"
            raise ValueError(msg) from exc
        if not 32 <= len(encoded) <= 256 or any(byte < 33 or byte > 126 for byte in encoded):
            msg = "bootstrap_token must be 32-256 printable ASCII characters without spaces"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def reject_development_key_in_production(self) -> Self:
        """Prevent the documented local-only key from reaching production."""

        development_key = base64.urlsafe_b64decode(DEVELOPMENT_SECRET_ENCRYPTION_KEY)
        if (
            self.environment in {"staging", "production"}
            and self.secret_encryption_key_bytes() == development_key
        ):
            msg = "staging and production require a unique secret_encryption_key"
            raise ValueError(msg)
        if self.bootstrap_enabled:
            if self.bootstrap_token is None or len(self.bootstrap_token.get_secret_value()) < 32:
                msg = "enabled bootstrap requires a token of at least 32 characters"
                raise ValueError(msg)
        return self

    def secret_encryption_key_bytes(self) -> bytes:
        """Decode the already-validated key only at the cryptographic boundary."""

        return base64.urlsafe_b64decode(self.secret_encryption_key.get_secret_value())


@lru_cache
def get_settings() -> Settings:
    """Create settings once for the lifetime of the process."""

    return Settings()
