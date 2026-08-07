"""Integration validation for the asynchronous Alembic environment."""

import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.integration


def _database_url() -> str:
    database_url = os.getenv("HOOKRELAY_TEST_DATABASE_URL")
    if database_url is None:
        if os.getenv("CI") == "true":
            pytest.fail("CI must configure HOOKRELAY_TEST_DATABASE_URL")
        pytest.skip("HOOKRELAY_TEST_DATABASE_URL is not configured")
    return database_url


def _run_alembic(database_url: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    """Run Alembic against only the explicitly configured disposable database."""

    environment = os.environ.copy()
    environment["HOOKRELAY_DATABASE_URL"] = database_url
    return subprocess.run(
        [sys.executable, "-m", "alembic", *arguments],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=30,
    )


def test_alembic_upgrade_head_against_postgresql() -> None:
    """Load Alembic's config and apply all reviewed revisions to the test database."""

    result = _run_alembic(_database_url(), "upgrade", "head")

    assert result.returncode == 0, result.stderr


def test_alembic_metadata_has_no_unreviewed_schema_drift() -> None:
    """Require the reviewed revision and SQLAlchemy metadata to describe one schema."""

    database_url = _database_url()
    upgrade = _run_alembic(database_url, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr

    check = _run_alembic(database_url, "check")
    assert check.returncode == 0, check.stdout + check.stderr


def test_alembic_database_is_at_every_head() -> None:
    """Detect a database left behind an available migration head."""

    database_url = _database_url()
    upgrade = _run_alembic(database_url, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr

    current = _run_alembic(database_url, "current", "--check-heads")
    assert current.returncode == 0, current.stdout + current.stderr
