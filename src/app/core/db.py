"""Async SQLAlchemy database engine and session factory."""

from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    pool_pre_ping=True,
)

async_session = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession]:
    """Yield an async database session."""
    async with async_session() as session:
        yield session

SessionDep = Annotated[AsyncSession, Depends(get_session)]
