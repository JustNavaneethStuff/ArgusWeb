from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from argus_core.settings import Settings


class DatabaseManager:
    """Owns a single async engine and session factory per service process."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine: AsyncEngine = create_async_engine(
            settings.async_database_url,
            echo=False,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
        )
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._session_factory

    async def check(self) -> bool:
        from sqlalchemy import text

        async with self._session_factory() as session:
            await session.execute(text("SELECT 1"))
        return True

    async def dispose(self) -> None:
        await self._engine.dispose()


def create_session_factory(settings: Settings) -> async_sessionmaker[AsyncSession]:
    """Backward-compatible helper; prefer DatabaseManager in new code."""
    return DatabaseManager(settings).session_factory


async def check_postgres(settings: Settings, db: DatabaseManager | None = None) -> bool:
    if db is not None:
        return await db.check()
    manager = DatabaseManager(settings)
    try:
        return await manager.check()
    finally:
        await manager.dispose()
