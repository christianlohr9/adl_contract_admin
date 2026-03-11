"""Pydantic response schemas for players."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    import datetime
    from decimal import Decimal


class PlayerSchema(BaseModel):
    """Single player response."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    mfl_id: int
    name: str
    position: str
    nfl_team: str | None
    birthdate: datetime.date | None
    draft_year: int | None
    draft_round: int | None


class PlayerWithContractSchema(BaseModel):
    """Player with current contract info."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    mfl_id: int
    name: str
    position: str
    nfl_team: str | None
    birthdate: datetime.date | None
    draft_year: int | None
    draft_round: int | None

    # Contract fields (None if no active contract)
    salary: Decimal | None = None
    years_remaining: int | None = None
    contract_type: str | None = None
    designation: str | None = None
    status: str | None = None
