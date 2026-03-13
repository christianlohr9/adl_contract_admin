"""Sync API endpoints — status, manual trigger, and historical sync."""

from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Response

from app.core.config import settings
from app.schemas.sync import BackfillStatusSchema, SyncStatusSchema, SyncTriggerResponse
from app.services.historical_sync import get_backfill_status, run_historical_backfill
from app.services.sync_orchestrator import (
    get_sync_status,
    run_full_sync,
    run_historical_sync,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sync", tags=["sync"])


@router.get("/status", response_model=SyncStatusSchema)
async def sync_status() -> SyncStatusSchema:
    """Return the current sync status."""
    status = get_sync_status()
    return SyncStatusSchema(
        last_sync=status.last_sync,
        in_progress=status.in_progress,
        last_result=status.last_result,
        last_error=status.last_error,
    )


@router.post("/trigger", response_model=SyncTriggerResponse, status_code=202)
async def trigger_sync(
    background_tasks: BackgroundTasks, response: Response
) -> SyncTriggerResponse:
    """Trigger a manual full sync.

    Returns 202 Accepted if started, 409 Conflict if already in progress.
    """
    status = get_sync_status()
    if status.in_progress:
        response.status_code = 409
        return SyncTriggerResponse(
            message="Sync is already in progress",
            status="already_in_progress",
        )

    background_tasks.add_task(run_full_sync, settings)
    return SyncTriggerResponse(
        message="Full sync started",
        status="started",
    )


@router.post("/historical", response_model=SyncTriggerResponse, status_code=202)
async def trigger_historical_sync(
    background_tasks: BackgroundTasks, response: Response
) -> SyncTriggerResponse:
    """Trigger a historical score sync for configured years.

    Returns 202 Accepted if started, 409 Conflict if already in progress.
    """
    status = get_sync_status()
    if status.in_progress:
        response.status_code = 409
        return SyncTriggerResponse(
            message="Sync is already in progress",
            status="already_in_progress",
        )

    background_tasks.add_task(run_historical_sync, settings)
    return SyncTriggerResponse(
        message="Historical sync started",
        status="started",
    )


@router.get("/backfill-status", response_model=BackfillStatusSchema)
async def backfill_status() -> BackfillStatusSchema:
    """Return the current historical backfill status."""
    status = get_backfill_status()
    return BackfillStatusSchema(
        in_progress=status.in_progress,
        scores_complete=status.scores_complete,
        contracts_complete=status.contracts_complete,
        missing_score_years=status.missing_score_years,
        missing_contract_years=status.missing_contract_years,
        started_at=status.started_at,
        completed_at=status.completed_at,
        error=status.error,
    )


@router.post("/backfill", response_model=SyncTriggerResponse, status_code=202)
async def trigger_backfill(
    background_tasks: BackgroundTasks, response: Response
) -> SyncTriggerResponse:
    """Trigger a manual historical backfill.

    Returns 202 Accepted if started, 409 Conflict if already in progress.
    """
    status = get_backfill_status()
    if status.in_progress:
        response.status_code = 409
        return SyncTriggerResponse(
            message="Backfill is already in progress",
            status="already_in_progress",
        )

    background_tasks.add_task(run_historical_backfill, settings)
    return SyncTriggerResponse(
        message="Historical backfill started",
        status="started",
    )
