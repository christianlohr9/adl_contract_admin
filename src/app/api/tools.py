"""Contract tools API endpoints — player-centric contract action calculator.

Exposes the full contract engine (extensions, franchise tags, tenders, buyouts,
5th Year Options, PPE) through REST endpoints. All endpoints are read-only
calculations; no mutations occur.
"""

import logging

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from app.core.db import SessionDep
from app.models.player import Player
from app.schemas.tools import (
    BuyoutResultSchema,
    EligibilitySchema,
    ExtensionResultSchema,
    FifthYearOptionSchema,
    PlayerToolsSchema,
    PPESchema,
    TagResultSchema,
    TenderResultSchema,
    WindowStatusSchema,
)
from app.services.buyouts import calculate_5yo, calculate_buyout, calculate_ppe
from app.services.eligibility import (
    _VALID_ACTIONS,
    check_eligibility,
)
from app.services.extensions import calculate_extensions
from app.services.franchise_tags import calculate_franchise_tags
from app.services.tenders import calculate_tenders
from app.services.window_status import get_all_window_statuses

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tools", tags=["contract-tools"])

# Default season for all endpoints
_DEFAULT_SEASON = 2026

# Helpers


async def _verify_player(session: SessionDep, player_id: int) -> Player:
    """Load a player by ID or raise 404."""
    result = await session.execute(select(Player).where(Player.id == player_id))
    player = result.scalar_one_or_none()
    if player is None:
        raise HTTPException(status_code=404, detail=f"Player {player_id} not found")
    return player

# Bundled endpoint — "What can I do with this player?"


@router.get("/{player_id}/all", response_model=PlayerToolsSchema)


async def get_all_tools(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
    team_id: int | None = None,
) -> PlayerToolsSchema:
    """Return all contract tool results for a player in a single call.

    Calls every calculate service and bundles results. If an individual
    tool raises an unexpected error, that tool is set to None and the
    error is logged — one failing tool does not block the others.
    """
    player = await _verify_player(session, player_id)

    extensions: ExtensionResultSchema | None = None
    tags: TagResultSchema | None = None
    tenders: TenderResultSchema | None = None
    buyout: BuyoutResultSchema | None = None
    fifth_year_option: FifthYearOptionSchema | None = None
    ppe: PPESchema | None = None

    # Extensions
    try:
        result = await calculate_extensions(session, player_id, season)
        extensions = ExtensionResultSchema.model_validate(result, from_attributes=True)
    except Exception:
        logger.exception("Error calculating extensions for player %d", player_id)

    # Franchise tags
    try:
        result = await calculate_franchise_tags(session, player_id, season, team_id)
        tags = TagResultSchema.model_validate(result, from_attributes=True)
    except Exception:
        logger.exception("Error calculating franchise tags for player %d", player_id)

    # Tenders
    try:
        result = await calculate_tenders(session, player_id, season, team_id)
        tenders = TenderResultSchema.model_validate(result, from_attributes=True)
    except Exception:
        logger.exception("Error calculating tenders for player %d", player_id)

    # Buyout
    try:
        result = await calculate_buyout(session, player_id, season)
        buyout = BuyoutResultSchema.model_validate(result, from_attributes=True)
    except Exception:
        logger.exception("Error calculating buyout for player %d", player_id)

    # 5th Year Option
    try:
        result = await calculate_5yo(session, player_id, season)
        fifth_year_option = FifthYearOptionSchema.model_validate(
            result, from_attributes=True
        )
    except Exception:
        logger.exception("Error calculating 5YO for player %d", player_id)

    # PPE
    try:
        result = await calculate_ppe(session, player_id, season)
        ppe = PPESchema.model_validate(result, from_attributes=True)
    except Exception:
        logger.exception("Error calculating PPE for player %d", player_id)

    # Window statuses
    window_statuses: dict[str, WindowStatusSchema] | None = None
    try:
        raw_statuses = await get_all_window_statuses(session, season)
        window_statuses = {
            k: WindowStatusSchema.model_validate(v, from_attributes=True)
            for k, v in raw_statuses.items()
        }
    except Exception:
        logger.exception("Error fetching window statuses")

    return PlayerToolsSchema(
        player_id=player.id,
        player_name=player.name,
        position=player.position,
        season=season,
        extensions=extensions,
        tags=tags,
        tenders=tenders,
        buyout=buyout,
        fifth_year_option=fifth_year_option,
        ppe=ppe,
        window_statuses=window_statuses,
    )

# Eligibility endpoint — check all action types at once


@router.get("/{player_id}/eligibility", response_model=list[EligibilitySchema])


async def get_eligibility(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
    team_id: int | None = None,
) -> list[EligibilitySchema]:
    """Check eligibility for all contract action types.

    Returns one EligibilitySchema per action type (extension, franchise_tag,
    erfa_tender, rfa_tender, buyout_restructure, fifth_year_option,
    proven_performance_escalator).
    """
    await _verify_player(session, player_id)

    results: list[EligibilitySchema] = []
    for action in sorted(_VALID_ACTIONS):
        eligibility = await check_eligibility(session, player_id, season, action, team_id)
        results.append(
            EligibilitySchema.model_validate(eligibility, from_attributes=True)
        )

    return results

# Individual tool endpoints


@router.get("/{player_id}/extensions", response_model=ExtensionResultSchema)


async def get_extensions(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
) -> ExtensionResultSchema:
    """Calculate extension options for a player."""
    await _verify_player(session, player_id)
    result = await calculate_extensions(session, player_id, season)
    return ExtensionResultSchema.model_validate(result, from_attributes=True)


@router.get("/{player_id}/tags", response_model=TagResultSchema)


async def get_tags(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
    team_id: int | None = None,
) -> TagResultSchema:
    """Calculate franchise/transition tag options for a player."""
    await _verify_player(session, player_id)
    result = await calculate_franchise_tags(session, player_id, season, team_id)
    return TagResultSchema.model_validate(result, from_attributes=True)


@router.get("/{player_id}/tenders", response_model=TenderResultSchema)


async def get_tenders(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
    team_id: int | None = None,
) -> TenderResultSchema:
    """Calculate tender options (ERFA/RFA) for a player."""
    await _verify_player(session, player_id)
    result = await calculate_tenders(session, player_id, season, team_id)
    return TenderResultSchema.model_validate(result, from_attributes=True)


@router.get("/{player_id}/buyout", response_model=BuyoutResultSchema)


async def get_buyout(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
) -> BuyoutResultSchema:
    """Calculate buyout/restructure options for a player."""
    await _verify_player(session, player_id)
    result = await calculate_buyout(session, player_id, season)
    return BuyoutResultSchema.model_validate(result, from_attributes=True)


@router.get("/{player_id}/5yo", response_model=FifthYearOptionSchema)


async def get_5yo(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
) -> FifthYearOptionSchema:
    """Calculate 5th Year Option salary for a first-round rookie."""
    await _verify_player(session, player_id)
    result = await calculate_5yo(session, player_id, season)
    return FifthYearOptionSchema.model_validate(result, from_attributes=True)


@router.get("/{player_id}/ppe", response_model=PPESchema)


async def get_ppe(
    player_id: int,
    session: SessionDep,
    season: int = _DEFAULT_SEASON,
) -> PPESchema:
    """Calculate Proven Performance Escalator for a drafted rookie."""
    await _verify_player(session, player_id)
    result = await calculate_ppe(session, player_id, season)
    return PPESchema.model_validate(result, from_attributes=True)
