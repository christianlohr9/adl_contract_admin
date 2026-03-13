"""Franchise tag price calculations — EFT, NEFT, and Transition Tag services.

Calculates tag salaries based on positional salary averages (top 5 for EFT/NEFT,
top 10 for TT), opening bids for NEFT/TT, and consecutive tag premiums.

All salary math uses ``Decimal`` — floats from DB are converted at the boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from app.models.contract import Contract
from app.models.player import Player
from app.services.rules import (
    ceil_100k,
    floor_100k,
    get_contract_constants,
    get_sd_minimum,
    round_to_10k,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TagOption:
    """A single franchise/transition tag option."""

    tag_type: str  # "EFT", "NEFT", "TT"
    salary: Decimal  # Calculated tag salary
    opening_bid: Decimal | None  # Auction opening bid (NEFT/TT only, None for EFT)
    contract_years: int  # Always 1 for tags
    guarantee_level: str  # Always "FG"
    compensation: str  # "None" for EFT/TT, "Two 1st round picks" for NEFT


@dataclass
class FranchiseTagResult:
    """Full result from calculate_franchise_tags."""

    player_id: int
    player_name: str
    position: str
    previous_salary: Decimal
    options: list[TagOption]  # Up to 3 (EFT, NEFT, TT)
    eligible: bool
    ineligibility_reason: str | None
    consecutive_tag_count: int  # How many consecutive EFT/NEFT tags


# ---------------------------------------------------------------------------
# Tag salary calculation
# ---------------------------------------------------------------------------


def _resolve_position_filter(position: str):
    """Return a SQLAlchemy filter for position, grouping PK/PN into 'Kicker/Punter'.

    Bylaws: "Kickers and Punters are grouped into a single 'Kicker/Punter'
    position category" for franchise tag and 5YO salary calculations.
    """
    if position in ("PK", "PN"):
        return Player.position.in_(["PK", "PN"])
    return Player.position == position


async def _get_top_n_positional_salaries(
    session: AsyncSession,
    position: str,
    season: int,
    n: int,
) -> list[Decimal]:
    """Fetch the top N salaries at a position for a given season (end-of-year).

    Uses the previous season's salary data (season - 1).
    PK and PN are grouped into a single "Kicker/Punter" category per bylaws.
    """
    pos_filter = _resolve_position_filter(position)
    result = await session.execute(
        select(Contract.salary)
        .join(Player, Player.id == Contract.player_id)
        .where(
            pos_filter,
            Contract.season == season - 1,
        )
        .order_by(Contract.salary.desc())
        .limit(n)
    )
    return [Decimal(str(row[0])) for row in result.all()]


async def calculate_tag_salary(
    session: AsyncSession,
    position: str,
    tag_type: str,
    prev_salary: Decimal,
    season: int,
) -> Decimal:
    """Calculate franchise/transition tag salary.

    EFT/NEFT: MAX(AVG(Top5_Salaries_at_Position_EOY), 1.20 × prev_salary)
    TT: MAX(AVG(Top10_Salaries_at_Position_EOY), 1.20 × prev_salary)

    Rounded to nearest $10k.
    """
    n = 10 if tag_type == "TT" else 5
    top_salaries = await _get_top_n_positional_salaries(session, position, season, n)

    positional_avg = sum(top_salaries) / len(top_salaries) if top_salaries else Decimal("0")

    salary_floor = Decimal("1.20") * prev_salary
    raw = max(positional_avg, salary_floor)
    return round_to_10k(raw)


# ---------------------------------------------------------------------------
# Opening bid calculation (NEFT and TT only)
# ---------------------------------------------------------------------------


def calculate_opening_bid(tag_salary: Decimal, sd_minimum: Decimal) -> Decimal:
    """Calculate the auction opening bid for NEFT or TT.

    Formula: MAX(CEIL_100K(SD_Min - 0.1), FLOOR_100K(tag_salary))
    """
    sd_component = ceil_100k(sd_minimum - Decimal("0.1"))
    tag_component = floor_100k(tag_salary)
    return max(sd_component, tag_component)


# ---------------------------------------------------------------------------
# Consecutive tag check
# ---------------------------------------------------------------------------


async def get_consecutive_tag_count(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> int:
    """Count consecutive EFT/NEFT tags leading into the given season.

    Checks previous seasons' contracts for EFT/NEFT designations, counting
    backwards until a non-tag season is found.
    """
    constants = get_contract_constants()
    max_tags = constants["franchise_tags"]["max_consecutive_neft_eft_tags"]
    count = 0

    for offset in range(1, max_tags + 1):
        check_season = season - offset
        result = await session.execute(
            select(Contract.designation)
            .where(
                Contract.player_id == player_id,
                Contract.season == check_season,
            )
            .limit(1)
        )
        designation = result.scalar_one_or_none()
        if designation and ("EFT" in designation or "NEFT" in designation):
            count += 1
        else:
            break

    return count


# ---------------------------------------------------------------------------
# Eligibility check
# ---------------------------------------------------------------------------


async def check_tag_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> tuple[bool, str | None]:
    """Check whether a player is eligible for a franchise/transition tag.

    Returns ``(True, None)`` if eligible, ``(False, reason)`` if not.
    """
    constants = get_contract_constants()
    max_consecutive = constants["franchise_tags"]["max_consecutive_neft_eft_tags"]

    # Player must have an expired contract (years_remaining == 0).
    # Query explicitly for expired contracts to avoid picking up an active
    # contract when multiple contracts exist for the same player/season (Q2-D fix).
    result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.years_remaining == 0,
        )
        .order_by(Contract.salary.desc())
        .limit(1)
    )
    contract = result.scalar_one_or_none()

    if contract is None:
        return False, "No expired contract found (player may still have years remaining)"

    # Check consecutive tag limit for EFT/NEFT
    consecutive = await get_consecutive_tag_count(session, player_id, season)
    if consecutive >= max_consecutive:
        return (
            False,
            f"Player has reached the maximum {max_consecutive} consecutive EFT/NEFT tags",
        )

    return True, None


# ---------------------------------------------------------------------------
# Third consecutive tag premium
# ---------------------------------------------------------------------------


async def _calculate_third_tag_salary(
    session: AsyncSession,
    position: str,
    prev_salary: Decimal,
    season: int,
) -> Decimal:
    """Calculate the salary for a third consecutive EFT/NEFT tag.

    Formula: MAX(1.44 × prev_tag_salary, 1.20 × positional_FT_price,
                 highest_positional_FT_amount)

    The positional_FT_price is the EFT/NEFT tag price for this position.
    The highest_positional_FT_amount is the highest franchise tag salary at
    this position in the current season.
    """
    constants = get_contract_constants()
    multipliers = constants["franchise_tags"]["third_consecutive_tag_salary_multipliers"]
    prev_tag_pct = Decimal(str(multipliers["previous_tag_salary_pct"]))
    positional_pct = Decimal(str(multipliers["positional_ft_tag_price_pct"]))

    # 1.44 × prev_tag_salary
    option_a = prev_tag_pct * prev_salary

    # Positional FT price (top 5 avg for EFT/NEFT)
    positional_ft = await calculate_tag_salary(
        session, position, "EFT", prev_salary, season
    )
    # 1.20 × positional_FT_price
    option_b = positional_pct * positional_ft

    # Highest positional FT amount — look for existing franchise tag salaries
    # at this position in the current season (PK/PN grouped per bylaws)
    pos_filter = _resolve_position_filter(position)
    result = await session.execute(
        select(func.max(Contract.salary))
        .join(Player, Player.id == Contract.player_id)
        .where(
            pos_filter,
            Contract.season == season,
            Contract.designation.contains("EFT")
            | Contract.designation.contains("NEFT"),
        )
    )
    highest_ft = result.scalar_one_or_none()
    option_c = Decimal(str(highest_ft)) if highest_ft else Decimal("0")

    return round_to_10k(max(option_a, option_b, option_c))


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


async def calculate_franchise_tags(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> FranchiseTagResult:
    """Calculate all franchise/transition tag options for a player.

    Returns a ``FranchiseTagResult`` with up to 3 tag options (EFT, NEFT, TT),
    eligibility status, and consecutive tag count.
    """
    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()
    if player is None:
        return FranchiseTagResult(
            player_id=player_id,
            player_name="Unknown",
            position="",
            previous_salary=Decimal("0"),
            options=[],
            eligible=False,
            ineligibility_reason="Player not found",
            consecutive_tag_count=0,
        )

    # Load previous contract (stored in current season with years_remaining=0)
    # Explicitly filter for expired contracts to avoid picking wrong contract (Q2-D fix)
    contract_result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.years_remaining == 0,
        )
        .order_by(Contract.salary.desc())
        .limit(1)
    )
    contract = contract_result.scalar_one_or_none()
    prev_salary = Decimal(str(contract.salary)) if contract else Decimal("0")

    # Check eligibility
    eligible, reason = await check_tag_eligibility(session, player_id, season)
    consecutive = await get_consecutive_tag_count(session, player_id, season)

    if not eligible:
        return FranchiseTagResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            previous_salary=prev_salary,
            options=[],
            eligible=False,
            ineligibility_reason=reason,
            consecutive_tag_count=consecutive,
        )

    # Calculate tag salaries
    sd_minimum = get_sd_minimum(season)
    options: list[TagOption] = []
    is_third_tag = consecutive == 2  # 0-indexed: 2 previous = 3rd tag

    for tag_type in ("EFT", "NEFT", "TT"):
        # EFT/NEFT limited by consecutive tag count (max 3 combined)
        if tag_type in ("EFT", "NEFT") and consecutive >= 3:
            continue

        # Calculate base salary
        salary = await calculate_tag_salary(
            session, player.position, tag_type, prev_salary, season
        )

        # Apply third consecutive tag premium for EFT/NEFT
        if is_third_tag and tag_type in ("EFT", "NEFT"):
            third_salary = await _calculate_third_tag_salary(
                session, player.position, prev_salary, season
            )
            salary = max(salary, third_salary)

        # Opening bid (NEFT/TT only)
        opening_bid: Decimal | None = None
        if tag_type in ("NEFT", "TT"):
            opening_bid = calculate_opening_bid(salary, sd_minimum)

        # Compensation
        compensation = "Two 1st round picks" if tag_type == "NEFT" else "None"

        options.append(
            TagOption(
                tag_type=tag_type,
                salary=salary,
                opening_bid=opening_bid,
                contract_years=1,
                guarantee_level="FG",
                compensation=compensation,
            )
        )

    return FranchiseTagResult(
        player_id=player_id,
        player_name=player.name,
        position=player.position,
        previous_salary=prev_salary,
        options=options,
        eligible=True,
        ineligibility_reason=None,
        consecutive_tag_count=consecutive,
    )
