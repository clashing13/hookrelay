"""PostgreSQL engine ownership and lightweight connectivity checks."""

from typing import Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from hookrelay.config import Settings


class DatabaseHealth(Protocol):
    """Minimal database behavior needed by application lifecycle and readiness."""

    async def check_readiness(self) -> None:
        """Raise when the database cannot answer a lightweight query."""

    async def dispose(self) -> None:
        """Release pooled connections and engine resources."""


class PostgresDatabase:
    """Own one long-lived SQLAlchemy async engine for an API process."""

    def __init__(self, settings: Settings) -> None:
        self._engine = create_async_engine(
            settings.database_url.get_secret_value(),
            pool_pre_ping=True,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
        )
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    @property
    def engine(self) -> AsyncEngine:
        """Expose the engine for future short-lived unit-of-work session factories."""

        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        """Create independent short-lived sessions for API units of work."""

        return self._session_factory

    async def check_readiness(self) -> None:
        """Acquire a pooled connection and ask PostgreSQL to evaluate ``SELECT 1``."""

        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        """Close every pooled connection during graceful application shutdown."""

        await self._engine.dispose()
