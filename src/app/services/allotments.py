"""Allotment tracking service — per-team annual limits on contract tools.

Tracks and enforces how many franchise tags, buyout/restructures, tenders,
and July 1 tenders each team can use per season.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from app.models.allotment import Allotment

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# Allotment limits (from contract_tools.yaml / contracts.json)

# Per-team per-season limits for each action type.
# Extensions are unlimited and not tracked.
_ALLOTMENT_LIMITS: dict[str, int] = {
    "franchise_tag": 1,
    "buyout_restructure": 1,
    "tender": 2,  # Shared between RFA and ERFA
    "july_1_tender": 2,
}

# Public API


async def get_remaining_allotments(
    session: AsyncSession,
    team_id: int,
    season: int,
) -> dict[str, int]:
    """Return remaining allotments for each action type.

    Returns a dict of ``action_type -> remaining_count``. Extensions are
    unlimited and not included.

    Parameters
    ----------
    session : AsyncSession
        Database session.
    team_id : int
        The team to check.
    season : int
        The ADL season year.
    """
    # Count used allotments grouped by action_type
    result = await session.execute(
        select(
            Allotment.action_type,
            func.count(Allotment.id),
        )
        .where(
            Allotment.team_id == team_id,
            Allotment.season == season,
        )
        .group_by(Allotment.action_type)
    )
    used_counts: dict[str, int] = {
        str(row[0]): int(row[1]) for row in result.all()
    }

    remaining: dict[str, int] = {}
    for action_type, limit in _ALLOTMENT_LIMITS.items():
        used = used_counts.get(action_type, 0)
        remaining[action_type] = max(0, limit - used)

    return remaining


async def has_allotment(
    session: AsyncSession,
    team_id: int,
    season: int,
    action_type: str,
) -> bool:
    """Check whether a team has a remaining allotment for an action type.

    Parameters
    ----------
    session : AsyncSession
        Database session.
    team_id : int
        The team to check.
    season : int
        The ADL season year.
    action_type : str
        One of: franchise_tag, buyout_restructure, tender, july_1_tender.

    Returns
    -------
    bool
        True if the team has at least one remaining allotment.
    """
    limit = _ALLOTMENT_LIMITS.get(action_type)
    if limit is None:
        # Unknown or unlimited action type — allow it
        return True

    result = await session.execute(
        select(func.count(Allotment.id))
        .where(
            Allotment.team_id == team_id,
            Allotment.season == season,
            Allotment.action_type == action_type,
        )
    )
    used = result.scalar_one()
    return used < limit


async def record_allotment_use(
    session: AsyncSession,
    team_id: int,
    season: int,
    action_type: str,
    player_id: int | None = None,
) -> bool:
    """Record that a team has used an allotment for an action type.

    Checks availability first. If available, inserts a record and returns
    ``True``. If the allotment is exhausted, returns ``False``.

    Does NOT commit — the caller controls the transaction.

    Parameters
    ----------
    session : AsyncSession
        Database session.
    team_id : int
        The team using the allotment.
    season : int
        The ADL season year.
    action_type : str
        One of: franchise_tag, buyout_restructure, tender, july_1_tender.
    player_id : int | None
        Optional player the allotment is being used on.

    Returns
    -------
    bool
        True if the allotment was successfully recorded, False if exhausted.
    """
    if not await has_allotment(session, team_id, season, action_type):
        return False

    allotment = Allotment(
        team_id=team_id,
        season=season,
        action_type=action_type,
        player_id=player_id,
        used_at=datetime.now(UTC),
    )
    session.add(allotment)
    return True
