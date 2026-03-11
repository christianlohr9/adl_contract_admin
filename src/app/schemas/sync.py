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
