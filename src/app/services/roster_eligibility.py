"""Roster-wide eligibility service — aggregates contract action eligibility across a team.

Loads a team's roster once and checks every open-window contract action for each player,
extracting headline calculated values for eligible players.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from app.models.contract import Contract
from app.models.player import Player
from app.services.buyouts import calculate_5yo, calculate_buyout, calculate_ppe
from app.services.eligibility import check_eligibility
from app.services.extensions import calculate_extensions
from app.services.franchise_tags import calculate_franchise_tags
from app.services.tenders import calculate_tenders
from app.services.window_status import WindowStatus, get_all_window_statuses

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PlayerActionSummary:
    """Summary of a single player's eligibility for a contract action."""

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal | None
    headline_value: Decimal | None
    headline_label: str


@dataclass
class ActionGroup:
    """Group of eligible players for a single contract action type."""

    action: str
    window_closes: date | None
    players: list[PlayerActionSummary] = field(default_factory=list)


@dataclass
class RosterEligibilitySummary:
    """Full roster eligibility summary across all open action windows."""

    team_id: int
    season: int
    action_groups: list[ActionGroup] = field(default_factory=list)
    window_statuses: dict[str, WindowStatus] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Headline value extractors — one per action type
# ---------------------------------------------------------------------------


async def _extract_extension_headline(
    session: AsyncSession, player_id: int, season: int,
) -> tuple[Decimal | None, str]:
    """Extract headline value from extension calculation."""
    result = await calculate_extensions(session, player_id, season)
    if result.options:
        return result.options[0].total_value, "Extension Value"
    return None, "Extension Value"


async def _extract_tag_headline(
    session: AsyncSession, player_id: int, season: int,
    team_id: int | None = None,
) -> tuple[Decimal | None, str]:
    """Extract headline value from franchise tag calculation."""
    result = await calculate_franchise_tags(session, player_id, season, team_id)
    if result.options:
        return result.options[0].salary, "Tag Salary"
    return None, "Tag Salary"


async def _extract_erfa_headline(
    session: AsyncSession, player_id: int, season: int,
    team_id: int | None = None,
) -> tuple[Decimal | None, str]:
    """Extract headline value from ERFA tender calculation."""
    result = await calculate_tenders(session, player_id, season, team_id)
    if result.erfa_option:
        return result.erfa_option.salary, "ERFA Tender"
    return None, "ERFA Tender"


async def _extract_rfa_headline(
    session: AsyncSession, player_id: int, season: int,
    team_id: int | None = None,
) -> tuple[Decimal | None, str]:
    """Extract headline value from RFA tender calculation."""
    result = await calculate_tenders(session, player_id, season, team_id)
    if result.rfa_options:
        return result.rfa_options[0].salary, "RFA Tender"
    return None, "RFA Tender"


async def _extract_buyout_headline(
    session: AsyncSession, player_id: int, season: int,
) -> tuple[Decimal | None, str]:
    """Extract headline value from buyout calculation."""
    result = await calculate_buyout(session, player_id, season)
    return result.opening_bid, "Opening Bid"


async def _extract_5yo_headline(
    session: AsyncSession, player_id: int, season: int,
) -> tuple[Decimal | None, str]:
    """Extract headline value from 5th Year Option calculation."""
    result = await calculate_5yo(session, player_id, season)
    return result.salary, "5YO Salary"


async def _extract_ppe_headline(
    session: AsyncSession, player_id: int, season: int,
) -> tuple[Decimal | None, str]:
    """Extract headline value from PPE calculation."""
    result = await calculate_ppe(session, player_id, season)
    return result.escalator_salary, "PPE Salary"


_HEADLINE_EXTRACTORS = {
    "extension": _extract_extension_headline,
    "franchise_tag": _extract_tag_headline,
    "erfa_tender": _extract_erfa_headline,
    "rfa_tender": _extract_rfa_headline,
    "buyout_restructure": _extract_buyout_headline,
    "fifth_year_option": _extract_5yo_headline,
    "proven_performance_escalator": _extract_ppe_headline,
}


# ---------------------------------------------------------------------------
# Main service
# ---------------------------------------------------------------------------


async def get_roster_eligibility(
    session: AsyncSession,
    team_id: int,
    season: int,
) -> RosterEligibilitySummary:
    """Aggregate contract action eligibility for all players on a team.

    1. Gets all window statuses and filters to open ones
    2. Loads team roster (RosterEntry + Player + Contract joins)
    3. For each open action, checks eligibility per player
    4. For eligible players, calls the calculate service and extracts headline value
    5. Groups results into ActionGroup dataclasses
    6. Returns RosterEligibilitySummary

    Parameters
    ----------
    session : AsyncSession
        Database session.
    team_id : int
        The team to check.
    season : int
        ADL season year.
    """
    # 1. Get all window statuses
    all_statuses = await get_all_window_statuses(session, season)

    # 2. Filter to open windows
    open_actions = {
        action: status
        for action, status in all_statuses.items()
        if status.status == "open"
    }

    # 3. Load team contracts once (contracts are complete; roster_entries has
    #    gaps due to the (player_id, season) unique constraint in dual-conference)
    result = await session.execute(
        select(Contract)
        .where(Contract.team_id == team_id, Contract.season == season)
        .options(joinedload(Contract.player))
    )
    contracts = result.unique().scalars().all()

    # 4. For each open action, check eligibility and extract headlines
    action_groups: list[ActionGroup] = []

    for action, window_status in open_actions.items():
        extractor = _HEADLINE_EXTRACTORS.get(action)
        if extractor is None:
            continue

        eligible_players: list[PlayerActionSummary] = []

        for contract in contracts:
            player = contract.player

            # Check eligibility for this player + action
            try:
                eligibility = await check_eligibility(
                    session, player.id, season, action, team_id,
                )
            except Exception:
                logger.exception(
                    "Error checking eligibility for player %d action %s",
                    player.id, action,
                )
                continue

            if not eligibility.eligible:
                continue

            # Player is eligible — extract headline value
            headline_value: Decimal | None = None
            headline_label: str = ""
            try:
                # Pass team_id to extractors that need conference scoping
                if action in ("franchise_tag", "erfa_tender", "rfa_tender"):
                    headline_value, headline_label = await extractor(
                        session, player.id, season, team_id,
                    )
                else:
                    headline_value, headline_label = await extractor(
                        session, player.id, season,
                    )
            except Exception:
                logger.exception(
                    "Error calculating %s for player %d",
                    action, player.id,
                )
                # Still include the player, just with None headline
                headline_label = _HEADLINE_EXTRACTORS[action].__doc__ or action

            current_salary = (
                Decimal(str(contract.salary)) if contract.salary else None
            )

            eligible_players.append(
                PlayerActionSummary(
                    player_id=player.id,
                    player_name=player.name,
                    position=player.position,
                    current_salary=current_salary,
                    headline_value=headline_value,
                    headline_label=headline_label,
                )
            )

        # Only include groups with eligible players
        if eligible_players:
            # Sort by headline_value descending, None values last
            eligible_players.sort(
                key=lambda p: (
                    p.headline_value is None,
                    -(p.headline_value or Decimal("0")),
                )
            )

            action_groups.append(
                ActionGroup(
                    action=action,
                    window_closes=window_status.closes,
                    players=eligible_players,
                )
            )

    return RosterEligibilitySummary(
        team_id=team_id,
        season=season,
        action_groups=action_groups,
        window_statuses=all_statuses,
    )
