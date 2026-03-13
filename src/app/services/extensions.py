"""Extensions engine — EYS calculation, salary smoothing, and extension options service.

All salary math uses ``Decimal`` — floats from DB are converted at the boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.models.contract import Contract
from app.models.player import Player
from app.services.epv import EPVResult, calculate_epv, is_robust_season
from app.services.rules import (
    get_contract_constants,
    get_salary_growth_rate,
    get_sd_minimum,
    round_to_10k,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ExtensionOption:
    """A single extension option (e.g. 'extend 3 years')."""

    extension_years: int  # Number of years being added
    eys: Decimal  # Extension Year Salary
    smoothed_salary: Decimal  # Final annual salary after smoothing
    total_value: Decimal  # Total contract value across all years
    contract_type: str  # Always "SD" for extensions
    years_remaining_after: int  # Total years on new contract


@dataclass
class ExtensionResult:
    """Full result from calculate_extensions — all valid options for a player."""

    player_id: int
    player_name: str
    position: str
    current_salary: Decimal
    current_years_remaining: int
    epv_details: EPVResult | None  # Full EPV breakdown (None if ineligible before EPV)
    options: list[ExtensionOption] = field(default_factory=list)
    eligible: bool = True
    ineligibility_reason: str | None = None


# ---------------------------------------------------------------------------
# EYS calculation
# ---------------------------------------------------------------------------


def calculate_eys(
    epv: EPVResult,
    extension_years: int,
    has_5yo: bool = False,
) -> Decimal:
    """Calculate Extension Year Salary (EYS).

    Formula: ``MAX(EPV_curr, EPV_new, EPV_old, floor) × (1.15 - 0.05 × effective_years)``

    The floor is already included in ``epv.floor`` (75% active / 82.5% expired).
    """
    # Collect all non-None EPV candidates + floor
    candidates: list[Decimal] = [epv.floor]
    if epv.epv_curr is not None:
        candidates.append(epv.epv_curr)
    if epv.epv_new is not None:
        candidates.append(epv.epv_new)
    if epv.epv_old is not None:
        candidates.append(epv.epv_old)

    max_epv = max(candidates)

    # 5YO adjustment: effective years = extension_years - 1
    effective_years = extension_years - 1 if has_5yo else extension_years

    multiplier = Decimal("1.15") - Decimal("0.05") * effective_years
    return round_to_10k(max_epv * multiplier)


# ---------------------------------------------------------------------------
# Salary smoothing
# ---------------------------------------------------------------------------


def calculate_smoothed_salary(
    current_salary: Decimal,
    eys: Decimal,
    prev_years: int,
    ext_years: int,
) -> Decimal:
    """Calculate the smoothed annual salary after extension.

    Appends EYS years to remaining old contract years and smooths using
    compound salary growth (10% annual from contracts.json).
    """
    growth = Decimal("1") + get_salary_growth_rate()  # 1.10
    total_years = prev_years + ext_years

    # Past total: sum of current_salary * growth^i for i in [0, prev_years)
    past_total = sum(current_salary * growth**i for i in range(prev_years))

    # Future total: sum of eys * growth^i for i in [prev_years, total_years)
    future_total = sum(eys * growth**i for i in range(prev_years, total_years))

    # Denominator: sum of growth^i for i in [0, total_years)
    denominator = sum(growth**i for i in range(total_years))

    return round_to_10k((past_total + future_total) / denominator)


# ---------------------------------------------------------------------------
# Eligibility checking
# ---------------------------------------------------------------------------


async def check_extension_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> tuple[bool, str | None]:
    """Check whether a player is eligible for an extension.

    Returns ``(True, None)`` if eligible, ``(False, reason)`` if not.
    """
    constants = get_contract_constants()
    max_years = constants["max_contract_years"]

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

    # Rule: Players with Contract Years >= 2 are ineligible
    if contract.years_remaining >= 2:
        return (
            False,
            "Player has 2+ years remaining on contract (must be in final year or expired)",
        )

    # Rule: Players on rookie/UDFA contracts signed for less than max years are ineligible
    # Bylaws: "3 for UDFA and 4 for drafted rookies"
    desig = contract.designation or ""
    is_rookie_contract = any(
        f"{contract.signed_season} {pick}" in desig
        for pick in [f"{r}." for r in range(1, 6)]
    )
    is_udfa_contract = "UDFA" in desig

    if is_rookie_contract or is_udfa_contract:
        rookie_limits = constants["contract_year_limits"]
        if is_udfa_contract:
            max_rookie_years = rookie_limits.get("udfa", {}).get("max", 3)
        else:
            max_rookie_years = rookie_limits.get("drafted_rookie_all_rounds", {}).get("max", 4)
        # Calculate original contract length from signed_season
        original_years = season - contract.signed_season + contract.years_remaining
        if original_years < max_rookie_years:
            return (
                False,
                "Players on rookie/UDFA contracts signed for less than max years are ineligible",
            )

    # Rule: EXT cannot cause total years to exceed 6
    if contract.years_remaining >= max_years:
        return False, f"Contract already at maximum {max_years} years"

    # Rule: Players who received an EXT in current or prior window
    # E3-D note: This query works correctly when EXT creates a new contract row
    # with signed_season set to the season the EXT was applied. If the system
    # ever modifies contracts in-place, this check would need revision.
    ext_check = await session.execute(
        select(Contract.id)
        .where(
            Contract.player_id == player_id,
            Contract.designation.contains("EXT"),
            Contract.signed_season >= season - 1,
        )
        .limit(1)
    )
    if ext_check.scalar_one_or_none() is not None:
        return False, "Player received an EXT in the current or prior window"

    # Rule: Players must have at least one robust PR
    has_robust = False
    for s in [season, season - 1, season - 2]:
        if await is_robust_season(session, player_id, s):
            has_robust = True
            break
    if not has_robust:
        return False, "Player has no Robust PRs in recent seasons"

    return True, None


# ---------------------------------------------------------------------------
# Extension options calculator
# ---------------------------------------------------------------------------


async def calculate_extensions(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> ExtensionResult:
    """Calculate all valid extension options for a player.

    Returns an ``ExtensionResult`` with eligibility status and all valid
    ``ExtensionOption`` instances (1-year through max).
    """
    constants = get_contract_constants()
    max_contract_years = constants["max_contract_years"]

    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()
    if player is None:
        return ExtensionResult(
            player_id=player_id,
            player_name="Unknown",
            position="",
            current_salary=Decimal("0"),
            current_years_remaining=0,
            epv_details=None,
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
    eligible, reason = await check_extension_eligibility(session, player_id, season)
    if not eligible:
        return ExtensionResult(
            player_id=player_id,
            player_name=player.name,
            position=player.position,
            current_salary=current_salary,
            current_years_remaining=current_years,
            epv_details=None,
            eligible=False,
            ineligibility_reason=reason,
        )

    # Calculate EPV
    epv = await calculate_epv(
        session,
        player_id,
        season,
        previous_salary=current_salary,
        years_remaining=current_years,
    )

    # Determine 5YO status
    desig = (contract.designation or "") if contract else ""
    has_5yo = "+" in desig or "5YO" in desig.upper()

    # Calculate all valid extension options
    sd_minimum = get_sd_minimum(season)
    options: list[ExtensionOption] = []

    max_ext_years = max_contract_years - current_years
    for ext_years in range(1, max_ext_years + 1):
        total_years = current_years + ext_years

        # Calculate EYS
        eys = calculate_eys(epv, ext_years, has_5yo=has_5yo)

        # Calculate smoothed salary
        smoothed = calculate_smoothed_salary(current_salary, eys, current_years, ext_years)

        # Apply SD minimum floor
        if smoothed < sd_minimum:
            smoothed = sd_minimum

        # Total value: sum of smoothed_salary * growth^i for all years
        growth = Decimal("1") + get_salary_growth_rate()
        total_value = round_to_10k(sum(smoothed * growth**i for i in range(total_years)))

        options.append(
            ExtensionOption(
                extension_years=ext_years,
                eys=eys,
                smoothed_salary=smoothed,
                total_value=total_value,
                contract_type="SD",
                years_remaining_after=total_years,
            )
        )

    return ExtensionResult(
        player_id=player_id,
        player_name=player.name,
        position=player.position,
        current_salary=current_salary,
        current_years_remaining=current_years,
        epv_details=epv,
        options=options,
        eligible=True,
        ineligibility_reason=None,
    )
