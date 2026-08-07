"""Asynchronous Alembic environment for HookRelay's PostgreSQL schema."""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from hookrelay.config import Settings
from hookrelay.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata


def _database_url() -> str:
    """Load the async PostgreSQL URL without putting credentials in alembic.ini."""

    return Settings().database_url.get_secret_value()


def run_migrations_offline() -> None:
    """Generate SQL without creating an Engine or opening a database connection."""

    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def _run_migrations(connection: Connection) -> None:
    """Configure Alembic on the synchronous facade of an async connection."""

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def _run_async_migrations() -> None:
    """Open one migration connection through SQLAlchemy's async engine."""

    # ConfigParser treats percent signs as interpolation, so escaped URL
    # characters must be doubled when the value is placed in Alembic's config.
    config.set_main_option("sqlalchemy.url", _database_url().replace("%", "%%"))
    section = config.get_section(config.config_ini_section) or {}
    connectable = async_engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    try:
        async with connectable.connect() as connection:
            await connection.run_sync(_run_migrations)
    finally:
        await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations against PostgreSQL using the asyncpg SQLAlchemy driver."""

    asyncio.run(_run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
