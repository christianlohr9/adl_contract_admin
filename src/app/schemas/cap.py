"""Pydantic response schemas for salary cap summaries and allotments.

Mirrors service-layer dataclasses (PenaltyResult, PlayerCapDetail,
TeamCapSummary) for JSON serialization of cap endpoints.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# Penalty result
# ---------------------------------------------------------------------------


class PenaltyResultSchema(BaseModel):
    """Mirror of cap_penalties.PenaltyResult dataclass."""

    model_config = ConfigDict(from_attributes=True)

    contract_type: str
    salary: Decimal
    years_remaining: int
    year_1_penalty: Decimal
    additional_years_penalty: Decimal
    total_penalty: Decimal
    notes: str


# ---------------------------------------------------------------------------
# Player cap detail
# ---------------------------------------------------------------------------


class PlayerCapDetailSchema(BaseModel):
    """Per-player cap breakdown including penalty if dropped."""

    model_config = ConfigDict(from_attributes=True)

    player_id: int
    player_name: str
    position: str
    salary: Decimal
    contract_type: str
    years_remaining: int
    designation: str
    penalty_if_dropped: PenaltyResultSchema


# ---------------------------------------------------------------------------
# Team cap summary
# ---------------------------------------------------------------------------


class TeamCapSummarySchema(BaseModel):
    """Team-level cap summary with per-player details."""

    model_config = ConfigDict(from_attributes=True)

    team_id: int
    team_name: str
    season: int
    total_salary: Decimal
    salary_by_type: dict[str, Decimal]
    total_penalty_exposure: Decimal
    penalty_by_type: dict[str, Decimal]
    player_details: list[PlayerCapDetailSchema]
    roster_count: int


# ---------------------------------------------------------------------------
# Allotments
# ---------------------------------------------------------------------------


class AllotmentsSchema(BaseModel):
    """Remaining per-team allotments for contract actions."""

    model_config = ConfigDict(from_attributes=True)

    franchise_tag: int
    buyout_restructure: int
    tender: int
    july_1_tender: int
