"""Integration validation for the asynchronous Alembic environment."""

import asyncio
import json
import os
import re
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration

_MIGRATION_DATABASE_NAME = re.compile(r"hookrelay_migration_[0-9a-f]{32}\Z")


@dataclass(frozen=True, slots=True)
class _DatabaseSeed:
    tenant_id: UUID
    api_key_id: UUID
    endpoint_id: UUID
    signing_secret_id: UUID
    event_id: UUID
    delivery_id: UUID
    attempt_id: UUID
    outbox_id: UUID


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


async def _admin_statement(database_url: str, statement: str) -> None:
    engine = create_async_engine(database_url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            await connection.execute(text(statement))
    finally:
        await engine.dispose()


def _quoted_migration_database_name(database_name: str) -> str:
    if _MIGRATION_DATABASE_NAME.fullmatch(database_name) is None:
        raise ValueError("refusing to manage an unexpected migration-test database")
    return f'"{database_name}"'


@contextmanager
def _isolated_migration_database(source_database_url: str) -> Iterator[str]:
    """Create and remove one exact UUID-named database without touching shared state."""

    database_name = f"hookrelay_migration_{uuid4().hex}"
    quoted_name = _quoted_migration_database_name(database_name)
    isolated_url = (
        make_url(source_database_url)
        .set(database=database_name)
        .render_as_string(hide_password=False)
    )
    created = False
    try:
        asyncio.run(_admin_statement(source_database_url, f"CREATE DATABASE {quoted_name}"))
        created = True
        yield isolated_url
    finally:
        if created:
            asyncio.run(
                _admin_statement(
                    source_database_url,
                    f"DROP DATABASE {quoted_name} WITH (FORCE)",
                )
            )


@pytest.fixture
def migration_database_url() -> Iterator[str]:
    with _isolated_migration_database(_database_url()) as database_url:
        yield database_url


async def _execute_batch_async(
    database_url: str,
    statements: list[tuple[str, dict[str, Any]]],
) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for statement, parameters in statements:
                await connection.execute(text(statement), parameters)
    finally:
        await engine.dispose()


def _execute_batch(
    database_url: str,
    statements: list[tuple[str, dict[str, Any]]],
) -> None:
    asyncio.run(_execute_batch_async(database_url, statements))


async def _fetch_all_async(
    database_url: str,
    statement: str,
    parameters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text(statement), parameters or {})
            return [dict(row) for row in result.mappings()]
    finally:
        await engine.dispose()


def _fetch_all(
    database_url: str,
    statement: str,
    parameters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return asyncio.run(_fetch_all_async(database_url, statement, parameters))


def _new_database_seed() -> _DatabaseSeed:
    return _DatabaseSeed(
        tenant_id=uuid4(),
        api_key_id=uuid4(),
        endpoint_id=uuid4(),
        signing_secret_id=uuid4(),
        event_id=uuid4(),
        delivery_id=uuid4(),
        attempt_id=uuid4(),
        outbox_id=uuid4(),
    )


def _domain_seed_statements(
    seed: _DatabaseSeed, *, delivery_status: str
) -> list[tuple[str, dict[str, Any]]]:
    return [
        (
            "INSERT INTO tenants (id, name) VALUES (:tenant_id, 'migration-test')",
            {"tenant_id": seed.tenant_id},
        ),
        (
            """
            INSERT INTO api_keys (
                id, tenant_id, name, public_id, secret_hash, secret_last_four
            ) VALUES (
                :api_key_id, :tenant_id, 'migration-key', :public_id, :secret_hash, '1234'
            )
            """,
            {
                "api_key_id": seed.api_key_id,
                "tenant_id": seed.tenant_id,
                "public_id": f"hr_{uuid4().hex[:16]}",
                "secret_hash": b"h" * 32,
            },
        ),
        (
            """
            INSERT INTO webhook_endpoints (id, tenant_id, name, url)
            VALUES (:endpoint_id, :tenant_id, 'migration-endpoint', 'http://127.0.0.1/webhooks')
            """,
            {"endpoint_id": seed.endpoint_id, "tenant_id": seed.tenant_id},
        ),
        (
            """
            INSERT INTO endpoint_signing_secrets (
                id, tenant_id, endpoint_id, version, encryption_key_version,
                ciphertext, secret_hint
            ) VALUES (
                :secret_id, :tenant_id, :endpoint_id, 1, 1, :ciphertext, '5678'
            )
            """,
            {
                "secret_id": seed.signing_secret_id,
                "tenant_id": seed.tenant_id,
                "endpoint_id": seed.endpoint_id,
                "ciphertext": b"s" * 29,
            },
        ),
        (
            """
            INSERT INTO events (
                id, tenant_id, api_key_id, event_type, payload, idempotency_key,
                request_fingerprint
            ) VALUES (
                :event_id, :tenant_id, :api_key_id, 'migration.created',
                CAST(:payload AS jsonb), :idempotency_key, :fingerprint
            )
            """,
            {
                "event_id": seed.event_id,
                "tenant_id": seed.tenant_id,
                "api_key_id": seed.api_key_id,
                "payload": json.dumps({"migration": True}),
                "idempotency_key": f"migration-{uuid4().hex}",
                "fingerprint": b"f" * 32,
            },
        ),
        (
            """
            INSERT INTO deliveries (
                id, tenant_id, event_id, endpoint_id, signing_secret_id,
                target_url, status
            ) VALUES (
                :delivery_id, :tenant_id, :event_id, :endpoint_id, :secret_id,
                'http://127.0.0.1/webhooks', :status
            )
            """,
            {
                "delivery_id": seed.delivery_id,
                "tenant_id": seed.tenant_id,
                "event_id": seed.event_id,
                "endpoint_id": seed.endpoint_id,
                "secret_id": seed.signing_secret_id,
                "status": delivery_status,
            },
        ),
    ]


def _seed_stage3_active_attempt(database_url: str) -> _DatabaseSeed:
    seed = _new_database_seed()
    statements = _domain_seed_statements(seed, delivery_status="delivering")
    statements.extend(
        [
            (
                """
                INSERT INTO delivery_attempts (
                    id, tenant_id, delivery_id, attempt_number, started_at
                ) VALUES (
                    :attempt_id, :tenant_id, :delivery_id, 1,
                    clock_timestamp() - interval '30 days'
                )
                """,
                {
                    "attempt_id": seed.attempt_id,
                    "tenant_id": seed.tenant_id,
                    "delivery_id": seed.delivery_id,
                },
            ),
            (
                """
                INSERT INTO outbox_messages (
                    id, tenant_id, delivery_id, topic, payload
                ) VALUES (
                    :outbox_id, :tenant_id, :delivery_id, 'delivery.requested',
                    CAST(:payload AS jsonb)
                )
                """,
                {
                    "outbox_id": seed.outbox_id,
                    "tenant_id": seed.tenant_id,
                    "delivery_id": seed.delivery_id,
                    "payload": json.dumps({"schema_version": 1}),
                },
            ),
        ]
    )
    _execute_batch(database_url, statements)
    return seed


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


def test_stage4_migration_backfills_active_attempt_and_refuses_replay_downgrade(
    migration_database_url: str,
) -> None:
    """Prove the Stage 3-to-4 data transition and the lossy-downgrade guard."""

    stage3 = _run_alembic(migration_database_url, "upgrade", "20260803_0002")
    assert stage3.returncode == 0, stage3.stderr
    seed = _seed_stage3_active_attempt(migration_database_url)

    stage4 = _run_alembic(migration_database_url, "upgrade", "20260804_0003")
    assert stage4.returncode == 0, stage4.stderr

    migrated = _fetch_all(
        migration_database_url,
        """
        SELECT
            d.status,
            d.next_attempt_at IS NOT NULL AS has_retry_schedule,
            d.dispatch_generation,
            a.finished_at IS NOT NULL AS attempt_finished,
            a.outcome,
            a.error_code,
            a.duration_ms,
            pg_typeof(a.duration_ms)::text AS duration_type,
            a.dispatch_generation AS attempt_generation
        FROM deliveries AS d
        JOIN delivery_attempts AS a ON a.delivery_id = d.id
        WHERE d.id = :delivery_id
        """,
        {"delivery_id": seed.delivery_id},
    )
    assert len(migrated) == 1
    migrated_row = migrated[0]
    assert migrated_row["status"] == "retry_scheduled"
    assert migrated_row["has_retry_schedule"] is True
    assert migrated_row["dispatch_generation"] == 1
    assert migrated_row["attempt_finished"] is True
    assert migrated_row["outcome"] == "abandoned"
    assert migrated_row["error_code"] == "stage4_migration_recovery"
    assert migrated_row["duration_type"] == "bigint"
    assert int(migrated_row["duration_ms"]) > 2_147_483_647
    assert migrated_row["attempt_generation"] == 1

    # Make the old attempt Stage-3-compatible, then create only replay state so
    # refusal is specifically evidence that replay history cannot be collapsed.
    replay_outbox_id = uuid4()
    _execute_batch(
        migration_database_url,
        [
            (
                """
                UPDATE deliveries
                SET status = 'pending', next_attempt_at = NULL, dispatch_generation = 2
                WHERE id = :delivery_id
                """,
                {"delivery_id": seed.delivery_id},
            ),
            (
                "UPDATE delivery_attempts SET duration_ms = 100 WHERE id = :attempt_id",
                {"attempt_id": seed.attempt_id},
            ),
            (
                """
                INSERT INTO outbox_messages (
                    id, tenant_id, delivery_id, topic, payload, dispatch_generation
                ) VALUES (
                    :outbox_id, :tenant_id, :delivery_id, 'delivery.requested',
                    CAST(:payload AS jsonb), 2
                )
                """,
                {
                    "outbox_id": replay_outbox_id,
                    "tenant_id": seed.tenant_id,
                    "delivery_id": seed.delivery_id,
                    "payload": json.dumps({"schema_version": 1}),
                },
            ),
        ],
    )
    generations = _fetch_all(
        migration_database_url,
        """
        SELECT dispatch_generation
        FROM outbox_messages
        WHERE delivery_id = :delivery_id
        ORDER BY dispatch_generation
        """,
        {"delivery_id": seed.delivery_id},
    )
    assert generations == [{"dispatch_generation": 1}, {"dispatch_generation": 2}]

    downgrade = _run_alembic(migration_database_url, "downgrade", "20260803_0002")
    assert downgrade.returncode != 0
    assert "cannot downgrade Stage 4 while Stage 4 delivery state or envelopes exist" in (
        downgrade.stdout + downgrade.stderr
    )
    assert _fetch_all(migration_database_url, "SELECT version_num FROM alembic_version") == [
        {"version_num": "20260804_0003"}
    ]


@pytest.mark.parametrize(
    ("statement", "constraint_name"),
    [
        (
            """
            UPDATE deliveries
            SET status = 'dead_lettered',
                dead_lettered_at = clock_timestamp(),
                dead_letter_reason = NULL
            WHERE id = :delivery_id
            """,
            "ck_deliveries_dead_letter_state_consistent",
        ),
        (
            """
            UPDATE deliveries
            SET status = 'pending',
                dead_lettered_at = clock_timestamp(),
                dead_letter_reason = 'permanent_failure'
            WHERE id = :delivery_id
            """,
            "ck_deliveries_dead_letter_state_consistent",
        ),
        (
            """
            UPDATE deliveries
            SET status = 'dead_lettered',
                dead_lettered_at = clock_timestamp(),
                dead_letter_reason = 'not_a_real_reason'
            WHERE id = :delivery_id
            """,
            "ck_deliveries_dead_letter_reason_valid",
        ),
    ],
    ids=["missing-reason", "metadata-on-live-state", "invalid-reason"],
)
def test_stage4_dead_letter_checks_reject_invalid_terminal_rows(
    migration_database_url: str,
    statement: str,
    constraint_name: str,
) -> None:
    """Exercise CHECK SQL directly because Alembic drift checks omit its semantics."""

    upgrade = _run_alembic(migration_database_url, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr
    seed = _new_database_seed()
    _execute_batch(
        migration_database_url,
        _domain_seed_statements(seed, delivery_status="pending"),
    )

    with pytest.raises(IntegrityError, match=constraint_name):
        _execute_batch(
            migration_database_url,
            [(statement, {"delivery_id": seed.delivery_id})],
        )

    assert _fetch_all(
        migration_database_url,
        """
        SELECT status, dead_lettered_at, dead_letter_reason
        FROM deliveries
        WHERE id = :delivery_id
        """,
        {"delivery_id": seed.delivery_id},
    ) == [
        {
            "status": "pending",
            "dead_lettered_at": None,
            "dead_letter_reason": None,
        }
    ]


def test_stage6_operations_migration_backfills_context_and_builds_keyset_indexes(
    migration_database_url: str,
) -> None:
    """Prove Stage 6 preserves outbox rows and installs its exact query support."""

    stage5 = _run_alembic(migration_database_url, "upgrade", "20260805_0004")
    assert stage5.returncode == 0, stage5.stderr
    seed = _new_database_seed()
    _execute_batch(
        migration_database_url,
        [
            *_domain_seed_statements(seed, delivery_status="pending"),
            (
                """
                INSERT INTO outbox_messages (
                    id, tenant_id, delivery_id, topic, payload
                ) VALUES (
                    :outbox_id, :tenant_id, :delivery_id, 'delivery.requested',
                    CAST(:payload AS jsonb)
                )
                """,
                {
                    "outbox_id": seed.outbox_id,
                    "tenant_id": seed.tenant_id,
                    "delivery_id": seed.delivery_id,
                    "payload": json.dumps({"schema_version": 1}),
                },
            ),
        ],
    )

    stage6 = _run_alembic(migration_database_url, "upgrade", "20260806_0005")
    assert stage6.returncode == 0, stage6.stderr
    assert _fetch_all(
        migration_database_url,
        """
        SELECT id, correlation_id, traceparent
        FROM outbox_messages
        WHERE id = :outbox_id
        """,
        {"outbox_id": seed.outbox_id},
    ) == [
        {
            "id": seed.outbox_id,
            "correlation_id": seed.outbox_id,
            "traceparent": None,
        }
    ]

    indexes = {
        row["indexname"]: row["indexdef"]
        for row in _fetch_all(
            migration_database_url,
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = current_schema()
              AND tablename = 'deliveries'
              AND indexname IN (
                'ix_deliveries_tenant_id_status_created_at',
                'ix_deliveries_tenant_id_created_at_id',
                'ix_deliveries_tenant_id_endpoint_id_created_at_id'
              )
            """,
        )
    }
    assert set(indexes) == {
        "ix_deliveries_tenant_id_status_created_at",
        "ix_deliveries_tenant_id_created_at_id",
        "ix_deliveries_tenant_id_endpoint_id_created_at_id",
    }
    assert (
        "(tenant_id, status, created_at, id)"
        in indexes["ix_deliveries_tenant_id_status_created_at"]
    )
    assert "(tenant_id, created_at, id)" in indexes["ix_deliveries_tenant_id_created_at_id"]
    assert (
        "(tenant_id, endpoint_id, created_at, id)"
        in indexes["ix_deliveries_tenant_id_endpoint_id_created_at_id"]
    )

    valid_traceparent = f"00-{'a' * 32}-{'b' * 16}-01"
    _execute_batch(
        migration_database_url,
        [
            (
                "UPDATE outbox_messages SET traceparent = :traceparent WHERE id = :outbox_id",
                {"traceparent": valid_traceparent, "outbox_id": seed.outbox_id},
            )
        ],
    )
    with pytest.raises(IntegrityError, match="ck_outbox_messages_traceparent_canonical"):
        _execute_batch(
            migration_database_url,
            [
                (
                    "UPDATE outbox_messages SET traceparent = :traceparent WHERE id = :outbox_id",
                    {
                        "traceparent": f"00-{'A' * 32}-{'b' * 16}-01",
                        "outbox_id": seed.outbox_id,
                    },
                )
            ],
        )

    defaulted_outbox_id = uuid4()
    _execute_batch(
        migration_database_url,
        [
            (
                """
                INSERT INTO outbox_messages (
                    id, tenant_id, delivery_id, topic, payload, dispatch_generation
                ) VALUES (
                    :outbox_id, :tenant_id, :delivery_id, 'delivery.requested',
                    CAST(:payload AS jsonb), 2
                )
                """,
                {
                    "outbox_id": defaulted_outbox_id,
                    "tenant_id": seed.tenant_id,
                    "delivery_id": seed.delivery_id,
                    "payload": json.dumps({"schema_version": 1}),
                },
            )
        ],
    )
    defaulted = _fetch_all(
        migration_database_url,
        """
        SELECT correlation_id IS NOT NULL AS has_correlation
        FROM outbox_messages
        WHERE id = :outbox_id
        """,
        {"outbox_id": defaulted_outbox_id},
    )
    assert defaulted == [{"has_correlation": True}]

    downgrade = _run_alembic(migration_database_url, "downgrade", "20260805_0004")
    assert downgrade.returncode == 0, downgrade.stderr
    columns = _fetch_all(
        migration_database_url,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'outbox_messages'
          AND column_name IN ('correlation_id', 'traceparent')
        """,
    )
    assert columns == []
    old_index = _fetch_all(
        migration_database_url,
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'deliveries'
          AND indexname = 'ix_deliveries_tenant_id_status_created_at'
        """,
    )
    assert len(old_index) == 1
    assert "(tenant_id, status, created_at)" in old_index[0]["indexdef"]
    assert "(tenant_id, status, created_at, id)" not in old_index[0]["indexdef"]

    reupgrade = _run_alembic(migration_database_url, "upgrade", "head")
    assert reupgrade.returncode == 0, reupgrade.stderr
    rebackfilled = _fetch_all(
        migration_database_url,
        """
        SELECT id, correlation_id
        FROM outbox_messages
        ORDER BY id
        """,
    )
    assert all(row["id"] == row["correlation_id"] for row in rebackfilled)
