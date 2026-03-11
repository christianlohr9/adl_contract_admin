"""Pydantic response schemas for teams."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class TeamSchema(BaseModel):
    """Single team response."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    franchise_id: str
    name: str
    conference: str
    division: str
