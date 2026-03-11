"""Teams API endpoints — browse teams, rosters, and contracts."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from app.core.db import SessionDep
from app.models.contract import Contract
from app.models.roster import RosterEntry
from app.models.team import Team
from app.schemas.contract import ContractSchema, RosterEntrySchema
from app.schemas.team import TeamSchema

router = APIRouter(prefix="/api/teams", tags=["teams"])


@router.get("/", response_model=list[TeamSchema])
async def list_teams(session: SessionDep) -> list[TeamSchema]:
    """Return all 32 teams, ordered by name."""
    result = await session.execute(select(Team).order_by(Team.name))
    teams = result.scalars().all()
    return [TeamSchema.model_validate(t) for t in teams]


@router.get("/{team_id}", response_model=TeamSchema)
async def get_team(team_id: int, session: SessionDep) -> TeamSchema:
    """Return a single team by ID. 404 if not found."""
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")
    return TeamSchema.model_validate(team)


@router.get("/{team_id}/roster", response_model=list[RosterEntrySchema])
async def get_team_roster(
    team_id: int, session: SessionDep, season: int = 2026
) -> list[RosterEntrySchema]:
    """Return team roster for a season with player and contract info.

    Joins RosterEntry -> Player -> Contract to build each entry.
    Ordered by salary descending (nulls last).
    """
    # Verify team exists
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")

    result = await session.execute(
        select(RosterEntry)
        .where(RosterEntry.team_id == team_id, RosterEntry.season == season)
        .options(joinedload(RosterEntry.player), joinedload(RosterEntry.contract))
    )
    entries = result.unique().scalars().all()

    roster = []
    for entry in entries:
        contract = entry.contract
        roster.append(
            RosterEntrySchema(
                id=entry.id,
                player_id=entry.player_id,
                player_name=entry.player.name,
                position=entry.player.position,
                salary=contract.salary if contract else None,
                years_remaining=contract.years_remaining if contract else None,
                contract_type=contract.contract_type if contract else None,
                designation=contract.designation if contract else None,
                roster_status=entry.roster_status,
            )
        )

    # Sort by salary descending (None values last)
    roster.sort(key=lambda r: (r.salary is None, -(r.salary or 0)))
    return roster


@router.get("/{team_id}/contracts", response_model=list[ContractSchema])
async def get_team_contracts(
    team_id: int, session: SessionDep, season: int = 2026
) -> list[ContractSchema]:
    """Return all contracts for a team in a season, ordered by salary descending."""
    # Verify team exists
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")

    result = await session.execute(
        select(Contract)
        .where(Contract.team_id == team_id, Contract.season == season)
        .order_by(Contract.salary.desc())
    )
    contracts = result.scalars().all()
    return [ContractSchema.model_validate(c) for c in contracts]
