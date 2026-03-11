"""Pydantic response schemas for contracts and roster entries."""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class ContractSchema(BaseModel):
    """Single contract response."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    team_id: int
    player_id: int
    season: int
    salary: Decimal
    years_remaining: int
    contract_type: str
    designation: str
    status: str
    signed_season: int


class RosterEntrySchema(BaseModel):
    """Roster entry with player and contract info."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    player_id: int
    player_name: str
    position: str
    salary: Decimal | None = None
    years_remaining: int | None = None
    contract_type: str | None = None
    designation: str | None = None
    roster_status: str
