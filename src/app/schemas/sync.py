"""Pydantic response models for the sync API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class SyncResultSchema(BaseModel):
    """Result counts for a single sync service."""

    created: int
    updated: int
    errors: list[str]


class SyncStatusSchema(BaseModel):
    """Current sync status response."""

    last_sync: datetime | None
    in_progress: bool
    last_result: dict[str, SyncResultSchema] | None
    last_error: str | None


class SyncTriggerResponse(BaseModel):
    """Response for sync trigger endpoints."""

    message: str
    status: str


class BackfillStatusSchema(BaseModel):
    """Current historical backfill status response."""

    in_progress: bool
    scores_complete: bool
    contracts_complete: bool
    missing_score_years: list[int]
    missing_contract_years: list[int]
    started_at: datetime | None
    completed_at: datetime | None
    error: str | None
