"""Validation tests for environment-backed settings."""

import pytest
from pydantic import ValidationError

from hookrelay.config import Settings


def test_settings_read_prefixed_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOOKRELAY_ENVIRONMENT", "test")
    monkeypatch.setenv("HOOKRELAY_PORT", "9000")

    settings = Settings(_env_file=None)

    assert settings.environment == "test"
    assert settings.port == 9000


def test_settings_reject_non_async_postgresql_url() -> None:
    with pytest.raises(ValidationError, match=r"postgresql\+asyncpg"):
        Settings(database_url="sqlite:///hookrelay.db", _env_file=None)


def test_settings_mask_database_credentials() -> None:
    password = "not-for-logs"
    settings = Settings(
        database_url=f"postgresql+asyncpg://hookrelay:{password}@localhost/hookrelay",
        _env_file=None,
    )

    assert password not in repr(settings)
