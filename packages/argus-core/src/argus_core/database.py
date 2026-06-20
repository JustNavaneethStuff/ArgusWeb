from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from argus_core.settings import Settings


def create_engine(settings: Settings):
    return create_async_engine(settings.async_database_url, echo=False, pool_pre_ping=True)


def create_session_factory(settings: Settings) -> async_sessionmaker[AsyncSession]:
    engine = create_engine(settings)
    return async_sessionmaker(engine, expire_on_commit=False)


async def check_postgres(settings: Settings) -> bool:
    from sqlalchemy import text

    factory = create_session_factory(settings)
    async with factory() as session:
        await session.execute(text("SELECT 1"))
    return True
