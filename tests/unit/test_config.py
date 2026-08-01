"""Validation tests for environment-backed settings."""

import pytest
from pydantic import ValidationError

from hookrelay import __version__
from hookrelay.config import Settings


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
