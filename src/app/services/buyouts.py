"""Buyout/Restructure, 5th Year Option, and Proven Performance Escalator services.

Calculates B/R salary options and GM choices, 5YO salary tiers based on
positional percentile performance, and PPE escalator salaries.

All salary math uses ``Decimal`` — floats from DB are converted at the boundary.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.models.contract import Contract
from app.models.player import Player
from app.models.player_score import PlayerScore
from app.services.franchise_tags import (
    _get_top_n_positional_salaries,
    calculate_tag_salary,
)
from app.services.rules import (
    ceil_100k,
    get_contract_constants,
    get_sd_minimum,
    round_to_10k,
)
from app.services.tenders import (
    calculate_orfa_bid,
    calculate_srfa_bid,
    _get_nfl_rfa_prices,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Buyout/Restructure result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class BuyoutOption:
    """A single GM option after B/R auction."""

    action: str  # "retain_restructure", "retain_revert", "revert_transfer", "buyout_transfer"
    description: str  # Human-readable description
    salary: Decimal | None  # New salary (None for revert options)
    contract_years: int | None  # New years (None for revert)


@dataclass
class BuyoutSalaryTier:
    """Salary at a given contract year count for B/R."""

    contract_years: int  # Years declared in bid
    salary: Decimal  # Calculated salary at that year count


@dataclass
class BuyoutResult:
    """Full result from calculate_buyout."""

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal
    current_years: int
    opening_bid: Decimal  # SD minimum rounded up to $100k
    salary_tiers: list[BuyoutSalaryTier]  # Salary at each contract year option
    gm_options: list[BuyoutOption]  # All 4 GM options
    eligible: bool
    ineligibility_reason: str | None


# ---------------------------------------------------------------------------
# B/R salary calculation
# ---------------------------------------------------------------------------


def calculate_br_salary(
    high_bid: Decimal,
    contract_years: int,
    sd_minimum: Decimal,
) -> Decimal:
    """Calculate B/R salary from high bid and contract years.

    Formula: ``high_bid * (1 - 0.05 * (contract_years - 1))``
    Floored at SD minimum, rounded to nearest $10k.
    """
    constants = get_contract_constants()
    discount_rate = Decimal(
        str(constants["buyout_restructure"]["salary_discount_per_year_beyond_1"])
    )
    discount = Decimal("1") - discount_rate * (contract_years - 1)
    calculated = high_bid * discount
    return round_to_10k(max(calculated, sd_minimum))


# ---------------------------------------------------------------------------
# Opening bid
# ---------------------------------------------------------------------------


def calculate_br_opening_bid(season: int) -> Decimal:
    """Calculate the B/R auction opening bid: SD minimum rounded up to $100k."""
    sd_minimum = get_sd_minimum(season)
    return ceil_100k(sd_minimum)


# ---------------------------------------------------------------------------
# Salary tier table
# ---------------------------------------------------------------------------


def calculate_br_salary_tiers(
    opening_bid: Decimal,
    sd_minimum: Decimal,
    max_years: int = 6,
) -> list[BuyoutSalaryTier]:
    """Calculate B/R salary at each contract year option (1 through max_years).

    Uses the opening bid as the high_bid to show what salary would be at each
    contract length.
    """
    tiers: list[BuyoutSalaryTier] = []
    for years in range(1, max_years + 1):
        salary = calculate_br_salary(opening_bid, years, sd_minimum)
        tiers.append(BuyoutSalaryTier(contract_years=years, salary=salary))
    return tiers


# ---------------------------------------------------------------------------
# GM options builder
# ---------------------------------------------------------------------------


def _build_gm_options(
    opening_bid: Decimal,
    sd_minimum: Decimal,
    *,
    prior_designation: str = "",
    prior_years_remaining: int = -1,
) -> list[BuyoutOption]:
    """Build GM options from buyout_restructure_rules.

    Bylaws (B6-M): "Revert/Transfer... [This option is prohibited for tagged
    players on expired contracts]". If the prior contract was a franchise tag
    (EFT/NEFT/TT) AND was expired (years_remaining == 0), revert_transfer is excluded.
    """
    # Default salary at 1-year contract (no discount)
    default_salary = calculate_br_salary(opening_bid, contract_years=1, sd_minimum=sd_minimum)

    # Check if revert/transfer is prohibited (tagged player on expired contract)
    is_tagged = any(
        tag in prior_designation for tag in ("EFT", "NEFT", "TT")
    )
    revert_transfer_prohibited = is_tagged and prior_years_remaining == 0

    options = [
        BuyoutOption(
            action="retain_restructure",
            description="Re-sign player using high-bid to determine new salary structure",
            salary=default_salary,
            contract_years=1,
        ),
        BuyoutOption(
            action="retain_revert",
            description="Revert player's contract to exact pre-B/R status (salary, years, type)",
            salary=None,
            contract_years=None,
        ),
    ]

    if not revert_transfer_prohibited:
        options.append(
            BuyoutOption(
                action="revert_transfer",
                description=(
                    "Revert contract and release to high bidder, "
                    "with or without trade compensation"
                ),
                salary=None,
                contract_years=None,
            )
        )

    options.append(
        BuyoutOption(
            action="buyout_transfer",
            description="Release player to high bidder at new salary structure",
            salary=default_salary,
            contract_years=1,
        )
    )

    return options


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


async def check_buyout_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> tuple[bool, str | None]:
    """Check whether a player is eligible for a Buyout/Restructure.

    Returns ``(True, None)`` if eligible, ``(False, reason)`` if not.

    Any player with a contract is eligible EXCEPT players on Drafted Rookie
    or UDFA contracts who are not in the final year of their contract
    (including 5th year if 5YO exercised).
    """
    # Load current active contract
    result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == "active",
        )
        .order_by(Contract.salary.desc())
        .limit(1)
    )
    contract = result.scalar_one_or_none()

    if contract is None:
        return False, "No active contract found for this season"

    desig = contract.designation or ""

    # Check if player is on a Drafted Rookie or UDFA contract
    is_rookie = False
    # Rookie draft pick designation pattern: "YYYY R.PP" e.g. "2021 1.02"
    for r in range(1, 6):
        if f"{contract.signed_season} {r}." in desig:
            is_rookie = True
            break
    # Also check for "R/F" designation (drafted rookie)
    if "R/F" in desig:
        is_rookie = True

    is_udfa = "UDFA" in desig

    if (is_rookie or is_udfa) and contract.years_remaining > 1:
        return (
            False,
            "Players on Drafted Rookie or UDFA contracts are ineligible "
            "until the final year of their contract",
        )

    return True, None


# ---------------------------------------------------------------------------
# Main buyout orchestrator
# ---------------------------------------------------------------------------


async def calculate_buyout(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> BuyoutResult:
    """Calculate buyout/restructure options for a player.

    Returns a ``BuyoutResult`` with opening bid, salary tiers at each
    contract year option, and all 4 GM options.
    """
    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()
    if player is None:
        return BuyoutResult(
            player_id=player_id,
            player_name="Unknown",
            position="",
            current_salary=Decimal("0"),
            current_years=0,
            opening_bid=Decimal("0"),
            salary_tiers=[],
            gm_options=[],
            eligible=False,
            ineligibility_reason="Player not found",
        )

    # Load current active contract
    contract_result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == "active",
        )
        .order_by(Contract.salary.desc())
        .limit(1)
    )
    contract = contract_result.scalar_one_or_none()
    current_salary = Decimal(str(contract.salary)) if contract else Decimal("0")
    current_years = contract.years_remaining if contract else 0

    # Check eligibility
    eligible, reason = await check_buyout_eligibility(session, player_id, season)
    if not eligible:
        return BuyoutResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            current_salary=current_salary,
            current_years=current_years,
            opening_bid=Decimal("0"),
            salary_tiers=[],
            gm_options=[],
            eligible=False,
            ineligibility_reason=reason,
        )

    # Calculate opening bid and salary tiers
    sd_minimum = get_sd_minimum(season)
    opening_bid = calculate_br_opening_bid(season)
    salary_tiers = calculate_br_salary_tiers(opening_bid, sd_minimum)
    gm_options = _build_gm_options(
        opening_bid,
        sd_minimum,
        prior_designation=contract.designation or "" if contract else "",
        prior_years_remaining=contract.years_remaining if contract else -1,
    )

    return BuyoutResult(
        player_id=player_id,
        player_name=player.name,
        position=player.position,
        current_salary=current_salary,
        current_years=current_years,
        opening_bid=opening_bid,
        salary_tiers=salary_tiers,
        gm_options=gm_options,
        eligible=True,
        ineligibility_reason=None,
    )


# ===========================================================================
# 5th Year Option (5YO)
# ===========================================================================


@dataclass
class FifthYearOptionResult:
    """Full result from calculate_5yo."""

    player_id: int
    player_name: str
    position: str
    percentile_tier: str  # "top_87_5", "75_to_87_5", "25_to_75", "bottom_25"
    salary: Decimal  # 5YO salary based on tier
    guarantee_level: str  # Always "FG"
    eligible: bool
    ineligibility_reason: str | None


# ---------------------------------------------------------------------------
# Starter percentile calculation
# ---------------------------------------------------------------------------


async def _calculate_pr_starter_floor(
    session: AsyncSession,
    position: str,
    season: int,
) -> int:
    """Delegate to the shared implementation in ``epv.py``."""
    from app.services.epv import calculate_pr_starter_floor

    return calculate_pr_starter_floor(position)


async def calculate_starter_percentile(
    session: AsyncSession,
    player_id: int,
    position: str,
    season: int,
) -> float | None:
    """Calculate a player's percentile among starters at their position.

    Gets total YTD points for all players at position, ranks them, and
    returns the target player's percentile (0.0 to 1.0) among those at
    or above the PR Starter Floor.

    Y7-D/P7-D fix: Uses PR Starter Floor to determine the pool size
    instead of counting all scored players.

    Returns ``None`` if the player has no scores for the season.
    """
    # Get all YTD scores at this position for the season, descending
    result = await session.execute(
        select(
            PlayerScore.player_id,
            PlayerScore.points,
        )
        .join(Player, Player.id == PlayerScore.player_id)
        .where(
            Player.position == position,
            PlayerScore.season == season,
            PlayerScore.week == "YTD",
        )
        .order_by(PlayerScore.points.desc())
    )
    all_scores = result.all()

    if not all_scores:
        return None

    # Calculate PR Starter Floor for this position and season
    starter_floor = await _calculate_pr_starter_floor(session, position, season)

    # The starter floor caps the pool: only the top N players count as "starters"
    # where N = starter_floor. Players ranked below this are below the floor.
    total_starters = min(starter_floor, len(all_scores))

    # Find the target player's rank (1-based, descending by points)
    rank = 0
    for i, row in enumerate(all_scores):
        if row.player_id == player_id:
            rank = i + 1  # 1-based rank
            break

    if rank == 0:
        return None  # Player has no score

    # If player ranks below the starter floor, they are below the floor
    # (0th percentile effectively)
    if rank > total_starters:
        return 0.0

    # Percentile: (total_starters - rank) / total_starters
    # This yields 0.0 for the last starter and approaches 1.0 for rank 1.
    percentile = (total_starters - rank) / total_starters
    return percentile


# ---------------------------------------------------------------------------
# Modified TT salary (for 5YO lower tiers)
# ---------------------------------------------------------------------------


async def calculate_modified_tt_salary(
    session: AsyncSession,
    position: str,
    prev_salary: Decimal,
    season: int,
    start_rank: int,
    end_rank: int,
) -> Decimal:
    """Calculate a modified Transition Tag salary using a custom salary rank range.

    Same formula as TT but averaging salaries from rank ``start_rank`` to
    ``end_rank`` instead of top 10.  Applies the ADL Cap Percentage
    (``current_cap / previous_cap``) to the prior-season salary average,
    consistent with ``calculate_tag_salary``.
    """
    from app.services.rules import get_adl_salary_cap

    # Get enough salaries to cover the range
    top_salaries = await _get_top_n_positional_salaries(
        session, position, season, end_rank
    )

    # Extract salaries from start_rank to end_rank (1-indexed)
    if len(top_salaries) >= start_rank:
        selected = top_salaries[start_rank - 1 : end_rank]
    else:
        selected = top_salaries

    positional_avg = sum(selected) / len(selected) if selected else Decimal("0")

    # Apply ADL Cap Percentage: current_cap / previous_cap
    current_cap = get_adl_salary_cap(season)
    previous_cap = get_adl_salary_cap(season - 1)
    cap_pct = current_cap / previous_cap
    positional_avg = cap_pct * positional_avg

    salary_floor = Decimal("1.20") * prev_salary
    raw = max(positional_avg, salary_floor)
    return round_to_10k(raw)


# ---------------------------------------------------------------------------
# 5YO percentile tier determination
# ---------------------------------------------------------------------------


def _determine_5yo_tier(percentile: float) -> str:
    """Determine the 5YO salary tier from a percentile value.

    Tier boundaries (percentile = ``(floor - rank) / floor``):

    * **NEFT** (top_87_5): percentile *strictly greater than* 87.5 %
    * **TT** (75_to_87_5): percentile *strictly greater than* 75 % and ≤ 87.5 %
    * **5YO+** (25_to_75): percentile ≥ 25 % and ≤ 75 %
    * **5YO−** (bottom_25): percentile < 25 %

    The "> 87.5 %" and "> 75 %" boundaries are *exclusive* — a player sitting
    exactly at the threshold falls into the lower tier (validated against the
    authoritative PPE5YO spreadsheet).
    """
    if percentile > 0.875:
        return "top_87_5"
    elif percentile > 0.75:
        return "75_to_87_5"
    elif percentile >= 0.25:
        return "25_to_75"
    else:
        return "bottom_25"


# ---------------------------------------------------------------------------
# ADL draft round extraction
# ---------------------------------------------------------------------------

_ADL_DRAFT_RE = re.compile(r"^\d{4}\s+(\d+)\.\d+")


def _extract_adl_draft_round(designation: str | None) -> int | None:
    """Extract the ADL draft round from a contract designation.

    Designation format for drafted rookies is ``"YYYY R.PP"`` (e.g.
    ``"2023 1.04"``).  Returns the round number, or ``None`` if the
    designation does not match the pattern.
    """
    if not designation:
        return None
    m = _ADL_DRAFT_RE.match(designation)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# 5YO main orchestrator
# ---------------------------------------------------------------------------


async def calculate_5yo(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> FifthYearOptionResult:
    """Calculate the 5th Year Option salary for a first-round drafted rookie.

    Returns a ``FifthYearOptionResult`` with the calculated salary based on
    the player's percentile tier.
    """
    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()
    if player is None:
        return FifthYearOptionResult(
            player_id=player_id,
            player_name="Unknown",
            position="",
            percentile_tier="",
            salary=Decimal("0"),
            guarantee_level="FG",
            eligible=False,
            ineligibility_reason="Player not found",
        )

    # Eligibility: must have an active rookie contract in year 4
    constants = get_contract_constants()
    eligible_rounds = constants["fifth_year_option"]["eligible_rounds"]

    # Fetch ALL active contracts — in a two-conference league a player may
    # have multiple contracts.  We need to find the one drafted in an
    # eligible ADL round.
    contract_result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == "active",
        )
        .order_by(Contract.salary.desc())
    )
    all_contracts = contract_result.scalars().all()
    if not all_contracts:
        return FifthYearOptionResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            percentile_tier="",
            salary=Decimal("0"),
            guarantee_level="FG",
            eligible=False,
            ineligibility_reason="No active contract found",
        )

    # Find the contract with an eligible ADL draft round designation.
    # In a two-conference league a player might have round-1 in one
    # conference and round-2 in another.
    contract = None
    adl_draft_round = None
    for c in all_contracts:
        r = _extract_adl_draft_round(c.designation)
        if r in eligible_rounds:
            contract = c
            adl_draft_round = r
            break

    if contract is None:
        # No contract with an eligible ADL round — check if any have a round at all
        best = all_contracts[0]
        adl_draft_round = _extract_adl_draft_round(best.designation)
        return FifthYearOptionResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            percentile_tier="",
            salary=Decimal("0"),
            guarantee_level="FG",
            eligible=False,
            ineligibility_reason=(
                f"Player was drafted in ADL round {adl_draft_round}, "
                f"only rounds {eligible_rounds} are eligible"
            ),
        )

    if adl_draft_round not in eligible_rounds:
        return FifthYearOptionResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            percentile_tier="",
            salary=Decimal("0"),
            guarantee_level="FG",
            eligible=False,
            ineligibility_reason=(
                f"Player was drafted in ADL round {adl_draft_round}, "
                f"only rounds {eligible_rounds} are eligible"
            ),
        )

    # Check if this is the 4th year of the contract
    contract_year = season - contract.signed_season + 1
    if contract_year != 4:
        return FifthYearOptionResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            percentile_tier="",
            salary=Decimal("0"),
            guarantee_level="FG",
            eligible=False,
            ineligibility_reason=(
                f"Player is in year {contract_year} of contract, must be in year 4"
            ),
        )

    prev_salary = Decimal(str(contract.salary))

    # Calculate percentile based on prior season performance
    percentile = await calculate_starter_percentile(
        session, player_id, player.position, season - 1
    )
    if percentile is None:
        # No scores — default to bottom tier
        percentile = 0.0

    tier = _determine_5yo_tier(percentile)

    # Calculate salary based on tier
    if tier == "top_87_5":
        # NEFT price
        salary = await calculate_tag_salary(
            session, player.position, "NEFT", prev_salary, season
        )
    elif tier == "75_to_87_5":
        # TT price (top 10)
        salary = await calculate_tag_salary(
            session, player.position, "TT", prev_salary, season
        )
    elif tier == "25_to_75":
        # TT price using 3rd-20th highest salaries
        salary = await calculate_modified_tt_salary(
            session, player.position, prev_salary, season,
            start_rank=3, end_rank=20,
        )
    else:
        # bottom_25: TT price using 3rd-25th highest salaries
        salary = await calculate_modified_tt_salary(
            session, player.position, prev_salary, season,
            start_rank=3, end_rank=25,
        )

    return FifthYearOptionResult(
        player_id=player_id,
        player_name=player.name,
        position=player.position,
        percentile_tier=tier,
        salary=salary,
        guarantee_level="FG",
        eligible=True,
        ineligibility_reason=None,
    )


# ===========================================================================
# Proven Performance Escalator (PPE)
# ===========================================================================


@dataclass
class PPEResult:
    """Full result from calculate_ppe."""

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal
    escalator_salary: Decimal | None  # None if not eligible or current salary is higher
    escalator_level: str | None  # "level_3" or "level_1_2"
    eligible: bool
    ineligibility_reason: str | None


async def calculate_ppe(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> PPEResult:
    """Calculate the Proven Performance Escalator for a drafted rookie.

    Returns a ``PPEResult`` with the escalator salary if eligible, or
    ineligibility details.
    """
    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()
    if player is None:
        return PPEResult(
            player_id=player_id,
            player_name="Unknown",
            position="",
            current_salary=Decimal("0"),
            escalator_salary=None,
            escalator_level=None,
            eligible=False,
            ineligibility_reason="Player not found",
        )

    constants = get_contract_constants()
    ppe_config = constants["proven_performance_escalator"]

    # Eligibility: position must not be PK or PN
    ineligible_positions = ppe_config["ineligible_positions"]
    if player.position in ineligible_positions:
        return PPEResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            current_salary=Decimal("0"),
            escalator_salary=None,
            escalator_level=None,
            eligible=False,
            ineligibility_reason=(
                f"Position {player.position} is ineligible for PPE"
            ),
        )

    # Eligibility: must be in year 4 of rookie contract.
    # Fetch ALL active contracts — in a two-conference league a player may
    # have multiple contracts with different ADL draft rounds.
    contract_result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == "active",
        )
        .order_by(Contract.salary.desc())
    )
    all_contracts = contract_result.scalars().all()
    if not all_contracts:
        return PPEResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            current_salary=Decimal("0"),
            escalator_salary=None,
            escalator_level=None,
            eligible=False,
            ineligibility_reason="No active contract found",
        )

    # Find the contract with an eligible ADL draft round designation.
    eligible_rounds = ppe_config["eligible_rounds"]
    contract = None
    adl_draft_round = None
    for c in all_contracts:
        r = _extract_adl_draft_round(c.designation)
        if r in eligible_rounds:
            contract = c
            adl_draft_round = r
            break

    if contract is None:
        best = all_contracts[0]
        adl_draft_round = _extract_adl_draft_round(best.designation)
        contract = best

    if adl_draft_round not in eligible_rounds:
        return PPEResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            current_salary=Decimal("0"),
            escalator_salary=None,
            escalator_level=None,
            eligible=False,
            ineligibility_reason=(
                f"Player was drafted in ADL round {adl_draft_round}, "
                f"only rounds {eligible_rounds} are eligible"
            ),
        )

    current_salary = Decimal(str(contract.salary))
    contract_year = season - contract.signed_season + 1
    eligible_year = ppe_config["eligible_contract_year"]

    if contract_year != eligible_year:
        return PPEResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            current_salary=current_salary,
            escalator_salary=None,
            escalator_level=None,
            eligible=False,
            ineligibility_reason=(
                f"Player is in year {contract_year} of contract, must be in year {eligible_year}"
            ),
        )

    # Calculate percentile based on prior season performance
    percentile = await calculate_starter_percentile(
        session, player_id, player.position, season - 1
    )

    # Bylaws: PPE applies to players who "finished in the 0th-75th percentile
    # **above** his PR Starter Floor".  Players ranked below the starter floor
    # (percentile returned as 0.0 by calculate_starter_percentile but actually
    # outside the pool) or with no scores (None) do NOT qualify for any PPE.
    #
    # We must distinguish "rank == floor" (0th percentile, qualifies) from
    # "rank > floor" (below floor, does not qualify).  Re-derive from rank.
    from app.services.epv import calculate_pr_starter_floor

    starter_floor = calculate_pr_starter_floor(player.position)
    if percentile is None:
        # No scores → below starter floor → no escalation
        return PPEResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            current_salary=current_salary,
            escalator_salary=None,
            escalator_level=None,
            eligible=True,
            ineligibility_reason=None,
        )

    # percentile == 0.0 could mean rank == floor (qualifies) or rank > floor
    # (does not qualify).  Re-check by computing rank explicitly.
    if percentile == 0.0:
        # Need to verify the player is actually AT the floor, not below it.
        # calculate_starter_percentile returns 0.0 for both cases.
        # Re-derive: get player rank among scorers at this position.
        from app.models.player_score import PlayerScore as PS

        rank_result = await session.execute(
            select(PS.player_id, PS.points)
            .join(Player, Player.id == PS.player_id)
            .where(
                Player.position == player.position,
                PS.season == season - 1,
                PS.week == "YTD",
            )
            .order_by(PS.points.desc())
        )
        all_scores = rank_result.all()
        player_rank = 0
        for i, row in enumerate(all_scores):
            if row.player_id == player_id:
                player_rank = i + 1
                break
        total_starters = min(starter_floor, len(all_scores))
        if player_rank > total_starters:
            # Below the starter floor — no PPE escalation
            return PPEResult(
                player_id=player_id,
                player_name=player.name,
                position=player.position,
                current_salary=current_salary,
                escalator_salary=None,
                escalator_level=None,
                eligible=True,
                ineligibility_reason=None,
            )

    prev_salary = Decimal(str(contract.salary))

    # Determine escalator level and salary.
    # Bylaws: "SRFA tag price" / "ORFA tag price" — these are the raw NFL
    # RFA tag prices for the season, NOT the tender MAX(NFL, mult * salary).
    nfl_prices = _get_nfl_rfa_prices(season)
    if percentile >= 0.75:
        # Level 3: SRFA tag price
        escalator_level = "level_3"
        escalator_salary = round_to_10k(nfl_prices["SRFA"])
    else:
        # Level 1-2: ORFA tag price
        escalator_level = "level_1_2"
        escalator_salary = round_to_10k(nfl_prices["ORFA"])

    # If current salary is higher, player keeps current salary
    if current_salary > escalator_salary:
        escalator_salary = None

    return PPEResult(
        player_id=player_id,
        player_name=player.name,
        position=player.position,
        current_salary=current_salary,
        escalator_salary=escalator_salary,
        escalator_level=escalator_level,
        eligible=True,
        ineligibility_reason=None,
    )
