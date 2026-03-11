"""FastAPI application with lifespan management."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fastapi import FastAPI
from sqlalchemy import text

from app.api.cap import router as cap_router
from app.api.players import router as players_router
from app.api.sync import router as sync_router
from app.api.teams import router as teams_router
from app.api.tools import router as tools_router
from app.core.config import settings
from app.core.db import SessionDep, engine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and shutdown."""
    if settings.sync_enabled:
        from apscheduler import AsyncScheduler
        from apscheduler.triggers.interval import IntervalTrigger

        from app.services.sync_orchestrator import run_full_sync

        async with AsyncScheduler() as scheduler:
            await scheduler.add_schedule(
                run_full_sync,
                IntervalTrigger(hours=settings.sync_interval_hours),
                id="mfl-full-sync",
                kwargs={"settings": settings},
            )
            await scheduler.start_in_background()
            logger.info(
                "APScheduler started: full sync every %d hours",
                settings.sync_interval_hours,
            )
            yield
    else:
        yield

    await engine.dispose()


app = FastAPI(title="ADL Contract Admin", lifespan=lifespan)
app.include_router(sync_router)
app.include_router(teams_router)
app.include_router(players_router)
app.include_router(tools_router)
app.include_router(cap_router)


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Return application health status."""
    return {"status": "ok", "version": "0.1.0"}


@app.get("/health/db")
async def health_db(session: SessionDep) -> dict[str, str]:
    """Verify database connectivity."""
    await session.execute(text("SELECT 1"))
    return {"status": "ok", "database": "connected"}
