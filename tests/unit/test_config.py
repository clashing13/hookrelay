"""Validation tests for environment-backed settings."""

import base64

import pytest
from pydantic import ValidationError

from hookrelay import __version__
from hookrelay.config import DEVELOPMENT_SECRET_ENCRYPTION_KEY, Settings


def test_settings_read_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOOKRELAY_ENVIRONMENT", "test")
    monkeypatch.setenv("HOOKRELAY_PORT", "9000")

    settings = Settings(_env_file=None)

    assert settings.environment == "test"
    assert settings.port == 9000


def test_settings_reject_non_async_postgresql_url() -> None:
    rejected_url = "postgresql://user:FAKE_LEAK_ME@database/hookrelay"

    with pytest.raises(ValidationError, match=r"postgresql\+asyncpg") as error:
        Settings(database_url=rejected_url, _env_file=None)

    assert rejected_url not in str(error.value)
    assert "FAKE_LEAK_ME" not in str(error.value)


def test_settings_mask_database_credentials() -> None:
    password = "not-for-logs"
    settings = Settings(
        database_url=f"postgresql+asyncpg://hookrelay:{password}@localhost/hookrelay",
        _env_file=None,
    )

    assert password not in repr(settings)


def test_settings_version_matches_package_version() -> None:
    settings = Settings(_env_file=None)

    assert settings.version == __version__


def test_settings_reject_health_contract_identity_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HOOKRELAY_SERVICE_NAME", "renamed-service")
    monkeypatch.setenv("HOOKRELAY_VERSION", "9.9.9")

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_non_local_environments_reject_the_published_development_key(environment: str) -> None:
    with pytest.raises(ValidationError, match="require a unique secret_encryption_key"):
        Settings(environment=environment, _env_file=None)


@pytest.mark.parametrize("environment", ["local", "test"])
def test_local_environments_may_use_the_documented_development_key(environment: str) -> None:
    settings = Settings(environment=environment, _env_file=None)

    assert settings.secret_encryption_key.get_secret_value() == DEVELOPMENT_SECRET_ENCRYPTION_KEY


def test_staging_accepts_a_unique_canonical_256_bit_encryption_key() -> None:
    encoded_key = base64.urlsafe_b64encode(b"S" * 32).decode("ascii")

    settings = Settings(
        environment="staging",
        secret_encryption_key=encoded_key,
        secret_encryption_key_version=17,
        _env_file=None,
    )

    assert settings.secret_encryption_key_bytes() == b"S" * 32
    assert settings.secret_encryption_key_version == 17


@pytest.mark.parametrize(
    "token",
    [
        None,
        "too-short",
        "A" * 31,
        "A" * 257,
        "A" * 31 + " ",
        "A" * 31 + "é",
    ],
)
def test_enabled_bootstrap_requires_a_bounded_printable_ascii_token(token: str | None) -> None:
    with pytest.raises(ValidationError, match="bootstrap"):
        Settings(bootstrap_enabled=True, bootstrap_token=token, _env_file=None)


def test_enabled_bootstrap_accepts_a_valid_token_and_keeps_it_masked() -> None:
    token = "bootstrap_" + "A" * 40

    settings = Settings(bootstrap_enabled=True, bootstrap_token=token, _env_file=None)

    assert settings.bootstrap_token is not None
    assert settings.bootstrap_token.get_secret_value() == token
    assert token not in repr(settings)


def test_encryption_key_requires_canonical_url_safe_base64() -> None:
    standard_base64 = base64.b64encode(b"\xfb" * 32).decode("ascii")

    with pytest.raises(ValidationError, match="canonical URL-safe base64"):
        Settings(secret_encryption_key=standard_base64, _env_file=None)


@pytest.mark.parametrize("version", [0, 32768])
def test_encryption_key_version_is_a_positive_small_integer(version: int) -> None:
    with pytest.raises(ValidationError):
        Settings(secret_encryption_key_version=version, _env_file=None)


def test_nats_credentials_are_masked_and_asset_names_are_versioned() -> None:
    password = "nats-password-not-for-logs"
    settings = Settings(
        nats_url=f"nats://worker:{password}@broker:4222",
        _env_file=None,
    )

    assert password not in repr(settings)
    assert settings.nats_server_url().endswith("@broker:4222")
    assert settings.nats_stream_name.endswith("_V1")
    assert settings.nats_subject.endswith(".v1")
    assert settings.nats_consumer_name.endswith("_V1")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("nats_url", "https://broker.example"),
        ("nats_url", "nats://broker.example/path"),
        ("nats_stream_name", "bad.stream"),
        ("nats_consumer_name", "bad consumer"),
        ("nats_subject", "hookrelay.delivery.*"),
    ],
)
def test_nats_configuration_rejects_ambiguous_assets(field: str, value: str) -> None:
    with pytest.raises(ValidationError):
        Settings.model_validate({field: value})


def test_worker_backpressure_and_timeout_budgets_must_align() -> None:
    with pytest.raises(ValidationError, match="nats_max_ack_pending"):
        Settings(
            delivery_worker_concurrency=9,
            nats_max_ack_pending=8,
            _env_file=None,
        )
    with pytest.raises(ValidationError, match="nats_ack_wait_seconds"):
        Settings(
            delivery_http_timeout_seconds=10,
            nats_ack_wait_seconds=14,
            _env_file=None,
        )
    with pytest.raises(ValidationError, match="publish-timeout budget"):
        Settings(
            outbox_batch_size=25,
            nats_publish_timeout_seconds=3,
            outbox_claim_ttl_seconds=60,
            _env_file=None,
        )


def test_stage3_worker_is_gated_to_explicit_local_test_hosts() -> None:
    settings = Settings(
        environment="test",
        delivery_allowed_hosts=frozenset({" Receiver ", "LOCALHOST."}),
        _env_file=None,
    )

    settings.require_stage3_delivery_runtime()
    assert settings.delivery_allowed_hosts == frozenset({"receiver", "localhost"})

    production = Settings(
        environment="production",
        secret_encryption_key=base64.urlsafe_b64encode(b"P" * 32).decode("ascii"),
        _env_file=None,
    )
    with pytest.raises(RuntimeError, match="restricted to local and test"):
        production.require_stage3_delivery_runtime()


def test_delivery_host_allowlist_rejects_empty_and_wildcard_values() -> None:
    with pytest.raises(ValidationError):
        Settings(delivery_allowed_hosts=frozenset(), _env_file=None)
    with pytest.raises(ValidationError):
        Settings(delivery_allowed_hosts=frozenset({"*"}), _env_file=None)
