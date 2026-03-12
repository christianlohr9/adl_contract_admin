"""Pydantic response schemas for roster-wide eligibility results."""

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from app.schemas.tools import WindowStatusSchema


class PlayerActionSummarySchema(BaseModel):
    """Summary of a single player's eligibility for a contract action."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal | None = None
    headline_value: Decimal | None = None
    headline_label: str


class ActionGroupSchema(BaseModel):
    """Group of eligible players for a single contract action type."""

    model_config = ConfigDict(from_attributes=True)

    action: str
    window_closes: date | None = None
    players: list[PlayerActionSummarySchema] = []


class RosterEligibilitySchema(BaseModel):
    """Full roster eligibility summary across all open action windows."""

    model_config = ConfigDict(from_attributes=True)

    team_id: int
    season: int
    action_groups: list[ActionGroupSchema] = []
    window_statuses: dict[str, WindowStatusSchema] = {}
