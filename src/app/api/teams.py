"""Teams API endpoints — browse teams, rosters, contracts, and snapshots."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from app.core.db import SessionDep
from app.models.contract import Contract
from app.models.roster import RosterEntry
from app.models.team import Team
from app.schemas.cap import AllotmentsSchema, TeamCapSummarySchema
from app.schemas.contract import ContractSchema, RosterEntrySchema
from app.schemas.roster_eligibility import (
    ActionGroupSchema,
    RosterEligibilitySchema,
)
from app.schemas.snapshot import TeamSnapshotSchema
from app.schemas.team import TeamSchema
from app.schemas.tools import WindowStatusSchema
from app.services.allotments import get_remaining_allotments
from app.services.cap_summary import get_team_cap_summary
from app.services.roster_eligibility import get_roster_eligibility

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


@router.get("/{team_id}/snapshot", response_model=TeamSnapshotSchema)
async def get_team_snapshot(
    team_id: int, session: SessionDep, season: int = 2026
) -> TeamSnapshotSchema:
    """Return the full team picture in a single call.

    Bundles team info, roster, cap summary, and remaining allotments so the
    GM gets a complete franchise snapshot without making multiple requests.
    """
    # 1. Load team (404 if not found)
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")

    # 2. Cap summary (includes player details)
    cap_summary = await get_team_cap_summary(session, team_id, season)

    # 3. Roster entries (reuse same query as roster endpoint)
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
    roster.sort(key=lambda r: (r.salary is None, -(r.salary or 0)))

    # 4. Remaining allotments
    remaining = await get_remaining_allotments(session, team_id, season)

    # 5. Bundle into snapshot
    return TeamSnapshotSchema(
        team=TeamSchema.model_validate(team),
        roster=roster,
        cap=TeamCapSummarySchema.model_validate(cap_summary, from_attributes=True),
        allotments=AllotmentsSchema(**remaining),
        season=season,
    )


@router.get("/{team_id}/eligibility", response_model=RosterEligibilitySchema)
async def get_roster_eligibility_endpoint(
    team_id: int,
    session: SessionDep,
    season: int = 2026,
) -> RosterEligibilitySchema:
    """Return contract action eligibility for all players on a team.

    Groups eligible players by action type with headline calculated values.
    Only includes actions whose calendar windows are currently open.
    """
    # Verify team exists
    team = await session.get(Team, team_id)
    if not team:
        raise HTTPException(status_code=404, detail=f"Team {team_id} not found")

    summary = await get_roster_eligibility(session, team_id, season)

    # Convert dataclasses to Pydantic schemas
    action_groups = [
        ActionGroupSchema.model_validate(group, from_attributes=True)
        for group in summary.action_groups
    ]
    window_statuses = {
        k: WindowStatusSchema.model_validate(v, from_attributes=True)
        for k, v in summary.window_statuses.items()
    }

    return RosterEligibilitySchema(
        team_id=summary.team_id,
        season=summary.season,
        action_groups=action_groups,
        window_statuses=window_statuses,
    )
