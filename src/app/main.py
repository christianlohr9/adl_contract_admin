"""FastAPI application with lifespan management."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from app.core.db import SessionDep, engine


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown."""
    yield
    await engine.dispose()


app = FastAPI(title="ADL Contract Admin", lifespan=lifespan)


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Return application health status."""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/health/db")
async def health_db(session: SessionDep) -> dict[str, str]:
    """Verify database connectivity."""
    await session.execute(text("SELECT 1"))
    return {"status": "ok", "database": "connected"}
