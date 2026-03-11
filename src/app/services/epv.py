"""EPV (Estimated Player Value) calculation core.

Internal helper used by extensions, tags, and other contract tools.
All salary math uses ``Decimal`` — floats from DB are converted at the boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import String, cast, func, select

from app.models.contract import Contract
from app.models.player import Player
from app.models.player_score import PlayerScore
from app.services.rules import get_contract_constants, round_to_10k

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class EPVResult:
    """Container for an EPV calculation result."""

    epv_curr: Decimal | None  # Current non-robust season EPV (None if offseason)
    epv_new: Decimal | None  # Most recent robust season EPV
    epv_old: Decimal | None  # Previous robust season EPV
    pr_curr: int | None  # Current non-robust PR
    pr_new: int | None  # Most recent robust PR
    pr_old: int | None  # Previous robust PR
    floor: Decimal  # 75% or 82.5% of previous salary
    position: str
    seasons_used: list[int] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Position rank helpers
# ---------------------------------------------------------------------------


async def get_position_rank(
    session: AsyncSession,
    player_id: int,
    position: str,
    season: int,
) -> int | None:
    """Calculate a player's position rank (dense rank by total points) for a season.

    Returns the player's 1-based rank, or ``None`` if no scores exist.
    """
    # Subquery: total points per player at this position for the season (YTD row)
    ytd_scores = (
        select(
            PlayerScore.player_id,
            PlayerScore.points.label("total_points"),
        )
        .join(Player, Player.id == PlayerScore.player_id)
        .where(
            Player.position == position,
            PlayerScore.season == season,
            PlayerScore.week == "YTD",
        )
        .subquery()
    )

    # Dense rank descending by total_points
    rank_col = func.dense_rank().over(order_by=ytd_scores.c.total_points.desc()).label("pr")
    ranked = select(ytd_scores.c.player_id, rank_col).subquery()

    result = await session.execute(
        select(ranked.c.pr).where(ranked.c.player_id == player_id)
    )
    row = result.scalar_one_or_none()
    return int(row) if row is not None else None


async def is_robust_season(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> bool:
    """Check whether a player has a robust season (>= ``ext_robust_games_minimum`` active weeks).

    An "active" week is a ``PlayerScore`` record where ``week`` is numeric (``'1'``-``'17'``).
    """
    constants = get_contract_constants()
    min_games = constants["extensions"]["ext_robust_games_minimum"]

    # Count numeric-week score records for this player+season
    result = await session.execute(
        select(func.count())
        .select_from(PlayerScore)
        .where(
            PlayerScore.player_id == player_id,
            PlayerScore.season == season,
            # Numeric weeks only (exclude 'YTD' and other non-numeric strings)
            cast(PlayerScore.week, String).regexp_match(r"^\d+$"),
        )
    )
    count = result.scalar_one()
    return count >= min_games


# ---------------------------------------------------------------------------
# Performance salary
# ---------------------------------------------------------------------------


def _sal_at_rank(salaries: list[Decimal], n: int) -> Decimal:
    """Return the nth highest salary (1-indexed) from a sorted-descending list.

    If *n* exceeds the list length, return the last (lowest) salary.
    If *n* < 1, return the first (highest) salary.
    """
    if not salaries:
        return Decimal("0")
    idx = max(0, min(n - 1, len(salaries) - 1))
    return salaries[idx]


async def calculate_performance_salary(
    session: AsyncSession,
    position: str,
    pr: int,
    season: int,
) -> Decimal:
    """Calculate the performance salary for a position rank in a season.

    Formula (from extensions.yaml):
        ``ROUND_TO_10K(AVERAGE(SAL(2*PR - 3), SAL(2*PR - 2)))``

    For PR=1 the formula becomes a linear extrapolation:
        ``2 * AVG(SAL(1), SAL(2)) - AVG(SAL(3), SAL(4))``

    *SAL(n)* is the nth highest salary at the position from the Contract table.
    """
    # Fetch all active salaries at this position for the season, sorted desc
    result = await session.execute(
        select(Contract.salary)
        .join(Player, Player.id == Contract.player_id)
        .where(
            Player.position == position,
            Contract.season == season,
            Contract.status == "active",
        )
        .order_by(Contract.salary.desc())
    )
    salaries = [Decimal(str(row[0])) for row in result.all()]

    if not salaries:
        return Decimal("0")

    if pr == 1:
        # Linear extrapolation above rank 1
        avg_top2 = (_sal_at_rank(salaries, 1) + _sal_at_rank(salaries, 2)) / 2
        avg_next2 = (_sal_at_rank(salaries, 3) + _sal_at_rank(salaries, 4)) / 2
        raw = 2 * avg_top2 - avg_next2
    else:
        n1 = 2 * pr - 3
        n2 = 2 * pr - 2
        raw = (_sal_at_rank(salaries, n1) + _sal_at_rank(salaries, n2)) / 2

    return round_to_10k(raw)


# ---------------------------------------------------------------------------
# EPV orchestrator
# ---------------------------------------------------------------------------


async def calculate_epv(
    session: AsyncSession,
    player_id: int,
    season: int,
    previous_salary: Decimal,
    years_remaining: int,
) -> EPVResult:
    """Calculate the full EPV result for a player.

    Parameters
    ----------
    session:
        Async database session.
    player_id:
        Player's database ID.
    season:
        The season to evaluate (typically the current season).
    previous_salary:
        The player's previous contract salary (Decimal, in millions).
    years_remaining:
        Contract years remaining. 0 means expired contract.

    Returns
    -------
    EPVResult
        The full EPV calculation result with floor, position ranks, and EPVs.
    """
    # Resolve player position
    result = await session.execute(select(Player.position).where(Player.id == player_id))
    position = result.scalar_one()

    # Floor percentages from constants
    constants = get_contract_constants()
    if years_remaining > 0:
        floor_pct = Decimal(str(constants["extensions"]["ext_prev_sal_floor_active_pct"]))
    else:
        floor_pct = Decimal(str(constants["extensions"]["ext_prev_sal_floor_expired_pct"]))

    floor_value = round_to_10k(floor_pct * previous_salary)

    # Look back up to 3 seasons for robust PRs
    seasons_to_check = [season, season - 1, season - 2]
    robust_seasons: list[tuple[int, int]] = []  # (season, pr)
    curr_pr: int | None = None
    curr_season_checked = False

    for s in seasons_to_check:
        pr = await get_position_rank(session, player_id, position, s)
        if pr is None:
            continue

        robust = await is_robust_season(session, player_id, s)

        if s == season and not robust:
            # Current season, not yet robust — this is EPV_curr candidate
            curr_pr = pr
            curr_season_checked = True
        elif robust:
            robust_seasons.append((s, pr))

        if len(robust_seasons) >= 2:
            break

    # Extract EPV_new and EPV_old from robust seasons
    pr_new: int | None = None
    pr_old: int | None = None
    epv_new: Decimal | None = None
    epv_old: Decimal | None = None
    epv_curr: Decimal | None = None
    seasons_used: list[int] = []

    if len(robust_seasons) >= 1:
        s_new, pr_new = robust_seasons[0]
        epv_new = await calculate_performance_salary(session, position, pr_new, s_new)
        seasons_used.append(s_new)

    if len(robust_seasons) >= 2:
        s_old, pr_old_raw = robust_seasons[1]
        # Floor PR_old at PR Starter Floor (if available — for now use raw)
        pr_old = pr_old_raw
        epv_old = await calculate_performance_salary(session, position, pr_old, s_old)
        seasons_used.append(s_old)

    if curr_pr is not None and curr_season_checked:
        epv_curr = await calculate_performance_salary(session, position, curr_pr, season)
        if season not in seasons_used:
            seasons_used.append(season)

    return EPVResult(
        epv_curr=epv_curr,
        epv_new=epv_new,
        epv_old=epv_old,
        pr_curr=curr_pr,
        pr_new=pr_new,
        pr_old=pr_old,
        floor=floor_value,
        position=position,
        seasons_used=sorted(seasons_used),
    )
