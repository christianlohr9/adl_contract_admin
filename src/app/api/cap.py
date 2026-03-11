"""Salary cap API endpoints — team cap summaries, player cap detail, allotments.

All endpoints are read-only. Salary values are in millions (0.01 = $10k).
"""

from fastapi import APIRouter, HTTPException

from app.core.db import SessionDep
from app.models.team import Team
from app.schemas.cap import (
    AllotmentsSchema,
    PlayerCapDetailSchema,
    TeamCapSummarySchema,
)
from app.services.allotments import get_remaining_allotments
from app.services.cap_summary import get_player_cap_detail, get_team_cap_summary

router = APIRouter(prefix="/api/cap", tags=["salary-cap"])

# Default season for all endpoints
_DEFAULT_SEASON = 2026


@router.get("/teams/{team_id}", response_model=TeamCapSummarySchema)
async def get_team_cap(
    team_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
) -> TeamCapSummarySchema:
    """Return full cap summary for a team including per-player breakdown."""
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")

    summary = await get_team_cap_summary(session, team_id, season)
    return TeamCapSummarySchema.model_validate(summary, from_attributes=True)


@router.get("/players/{player_id}", response_model=PlayerCapDetailSchema)
async def get_player_cap(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
) -> PlayerCapDetailSchema:
    """Return cap detail for a single player's active contract."""
    detail = await get_player_cap_detail(session, player_id, season)
    if detail is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active contract found for player {player_id} in season {season}",
        )
    return PlayerCapDetailSchema.model_validate(detail, from_attributes=True)


@router.get("/teams/{team_id}/allotments", response_model=AllotmentsSchema)
async def get_team_allotments(
    team_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
) -> AllotmentsSchema:
    """Return remaining allotments for a team (franchise tag, B/R, tenders)."""
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")

    remaining = await get_remaining_allotments(session, team_id, season)
    return AllotmentsSchema(**remaining)
