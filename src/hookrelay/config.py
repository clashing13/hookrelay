"""Environment-backed application configuration."""

import base64
import binascii
import re
from functools import lru_cache
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEVELOPMENT_SECRET_ENCRYPTION_KEY = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
NATS_ASSET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
NATS_SUBJECT_PATTERN = re.compile(r"^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$")


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
    version: Literal["0.4.0"] = "0.4.0"
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
    nats_url: SecretStr = SecretStr("nats://127.0.0.1:4222")
    nats_stream_name: str = "HOOKRELAY_DELIVERIES_V1"
    nats_subject: str = "hookrelay.delivery.requested.v1"
    nats_consumer_name: str = "HOOKRELAY_DELIVERY_WORKERS_V1"
    nats_connect_timeout_seconds: int = Field(default=2, ge=1, le=30)
    nats_drain_timeout_seconds: int = Field(default=5, ge=1, le=30)
    nats_publish_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    nats_duplicate_window_seconds: float = Field(default=600.0, ge=120, le=3600)
    nats_stream_max_bytes: int = Field(default=1_073_741_824, ge=1_048_576)
    nats_ack_wait_seconds: float = Field(default=30.0, ge=5, le=600)
    nats_ack_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    nats_max_ack_pending: int = Field(default=32, ge=1, le=10_000)
    outbox_batch_size: int = Field(default=25, ge=1, le=100)
    outbox_poll_interval_seconds: float = Field(default=0.25, ge=0.05, le=30)
    outbox_claim_ttl_seconds: float = Field(default=60.0, ge=5, le=600)
    delivery_worker_concurrency: int = Field(default=8, ge=1, le=100)
    delivery_fetch_timeout_seconds: float = Field(default=1.0, gt=0, le=30)
    delivery_http_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    delivery_claim_ttl_seconds: float = Field(default=20.0, ge=2, le=600)
    delivery_finalization_margin_seconds: float = Field(default=5.0, ge=0.5, le=60)
    delivery_max_attempts: int = Field(default=5, ge=1, le=100)
    delivery_retry_base_seconds: float = Field(default=1.0, ge=0.1, le=3600)
    delivery_retry_max_seconds: float = Field(default=60.0, ge=0.1, le=86_400)
    delivery_retry_jitter_ratio: float = Field(default=0.25, ge=0, le=1)
    delivery_policy_block_delay_seconds: float = Field(default=30.0, ge=1, le=3600)
    delivery_allowed_hosts: frozenset[str] = frozenset({"127.0.0.1", "localhost", "receiver"})

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

    @field_validator("nats_url", mode="before")
    @classmethod
    def require_nats_url(cls, value: object) -> object:
        """Accept only one bounded NATS client URL and keep credentials redacted."""

        raw_value = value.get_secret_value() if isinstance(value, SecretStr) else str(value)
        parsed = urlsplit(raw_value)
        if (
            parsed.scheme not in {"nats", "tls"}
            or parsed.hostname is None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or len(raw_value) > 2048
        ):
            msg = "nats_url must be a single nats:// or tls:// server URL"
            raise ValueError(msg)
        return value

    @field_validator("nats_stream_name", "nats_consumer_name")
    @classmethod
    def require_nats_asset_name(cls, value: str) -> str:
        """Keep durable asset names explicit and compatible across restarts."""

        if NATS_ASSET_NAME_PATTERN.fullmatch(value) is None:
            msg = "NATS stream and consumer names must use 1-64 letters, numbers, _ or -"
            raise ValueError(msg)
        return value

    @field_validator("nats_subject")
    @classmethod
    def require_literal_nats_subject(cls, value: str) -> str:
        """Reject wildcards because the publisher owns one exact internal subject."""

        if NATS_SUBJECT_PATTERN.fullmatch(value) is None:
            msg = "nats_subject must be a literal dot-delimited subject without wildcards"
            raise ValueError(msg)
        return value

    @field_validator("delivery_allowed_hosts")
    @classmethod
    def normalize_delivery_allowed_hosts(cls, value: frozenset[str]) -> frozenset[str]:
        """Require an explicit local/test allowlist until Stage 5 adds SSRF defenses."""

        normalized = frozenset(host.strip().lower().rstrip(".") for host in value)
        if not normalized or any(
            not host or any(character.isspace() for character in host) or host == "*"
            for host in normalized
        ):
            msg = "delivery_allowed_hosts must contain explicit non-wildcard hostnames"
            raise ValueError(msg)
        return normalized

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
        if self.nats_max_ack_pending < self.delivery_worker_concurrency:
            msg = "nats_max_ack_pending must be at least delivery_worker_concurrency"
            raise ValueError(msg)
        if self.nats_ack_wait_seconds < self.delivery_http_timeout_seconds + 5:
            msg = "nats_ack_wait_seconds must exceed delivery_http_timeout_seconds by 5 seconds"
            raise ValueError(msg)
        if self.delivery_claim_ttl_seconds <= (
            self.delivery_http_timeout_seconds + self.delivery_finalization_margin_seconds
        ):
            msg = (
                "delivery_claim_ttl_seconds must exceed delivery_http_timeout_seconds "
                "plus delivery_finalization_margin_seconds"
            )
            raise ValueError(msg)
        if self.delivery_retry_max_seconds < self.delivery_retry_base_seconds:
            msg = "delivery_retry_max_seconds must be at least delivery_retry_base_seconds"
            raise ValueError(msg)
        worst_case_batch_publish_seconds = (
            self.outbox_batch_size * self.nats_publish_timeout_seconds
        )
        if self.outbox_claim_ttl_seconds <= worst_case_batch_publish_seconds:
            msg = (
                "outbox_claim_ttl_seconds must exceed the aggregate broker "
                "publish-timeout budget for one outbox batch"
            )
            raise ValueError(msg)
        return self

    def secret_encryption_key_bytes(self) -> bytes:
        """Decode the already-validated key only at the cryptographic boundary."""

        return base64.urlsafe_b64decode(self.secret_encryption_key.get_secret_value())

    def nats_server_url(self) -> str:
        """Reveal the validated URL only at the NATS connection boundary."""

        return self.nats_url.get_secret_value()

    def require_delivery_runtime(self) -> None:
        """Fail closed outside local/test until Stage 5 implements complete SSRF controls."""

        if self.environment not in {"local", "test"}:
            msg = "Delivery workers are restricted to local and test environments until Stage 5"
            raise RuntimeError(msg)

    def require_stage3_delivery_runtime(self) -> None:
        """Retain the Stage 3 public helper while callers migrate to the current name."""

        self.require_delivery_runtime()


@lru_cache
def get_settings() -> Settings:
    """Create settings once for the lifetime of the process."""

    return Settings()
