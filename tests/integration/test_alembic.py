"""Integration validation for the asynchronous Alembic environment."""

import os
import subprocess
import sys

import pytest

pytestmark = pytest.mark.integration


def test_alembic_upgrade_head_against_postgresql() -> None:
    """Load Alembic's config and apply all reviewed revisions to the test database."""

    database_url = os.getenv("HOOKRELAY_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip("HOOKRELAY_TEST_DATABASE_URL is not configured")

    environment = os.environ.copy()
    environment["HOOKRELAY_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
