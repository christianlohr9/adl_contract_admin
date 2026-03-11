"""Players API endpoints — browse and search players."""

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from app.core.db import SessionDep
from app.models.contract import Contract, ContractStatus
from app.models.player import Player
from app.schemas.player import PlayerSchema, PlayerWithContractSchema

router = APIRouter(prefix="/api/players", tags=["players"])


@router.get("/search", response_model=list[PlayerSchema])
async def search_players(
    session: SessionDep, q: str = Query(min_length=2)
) -> list[PlayerSchema]:
    """Search players by name substring (case-insensitive). Max 50 results."""
    result = await session.execute(
        select(Player)
        .where(Player.name.ilike(f"%{q}%"))
        .order_by(Player.name)
        .limit(50)
    )
    players = result.scalars().all()
    return [PlayerSchema.model_validate(p) for p in players]


@router.get("/{player_id}", response_model=PlayerWithContractSchema)
async def get_player(
    player_id: int, session: SessionDep, season: int = 2026
) -> PlayerWithContractSchema:
    """Return a single player with current contract info. 404 if not found."""
    player = await session.get(Player, player_id)
    if not player:
        raise HTTPException(status_code=404, detail=f"Player {player_id} not found")

    # Load active contract for the given season
    result = await session.execute(
        select(Contract).where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == ContractStatus.ACTIVE,
        )
    )
    contract = result.scalars().first()

    return PlayerWithContractSchema(
        id=player.id,
        mfl_id=player.mfl_id,
        name=player.name,
        position=player.position,
        nfl_team=player.nfl_team,
        birthdate=player.birthdate,
        draft_year=player.draft_year,
        draft_round=player.draft_round,
        salary=contract.salary if contract else None,
        years_remaining=contract.years_remaining if contract else None,
        contract_type=contract.contract_type if contract else None,
        designation=contract.designation if contract else None,
        status=contract.status if contract else None,
    )
