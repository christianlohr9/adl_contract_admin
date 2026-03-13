"""Tender calculations — ERFA and RFA tender salary/bid services.

Calculates ERFA tender salaries and RFA tender opening bids (FRFA, SRFA, ORFA, RRFA)
with eligibility checks based on bylaws rules.

All salary math uses ``Decimal`` — floats from DB are converted at the boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.models.contract import Contract
from app.models.player import Player
from app.services.rules import (
    floor_100k,
    get_contract_constants,
    get_veteran_minimum,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TenderOption:
    """A single tender option (ERFA or one of four RFA tiers)."""

    tender_type: str  # "ERFA", "FRFA", "SRFA", "ORFA", "RRFA"
    salary: Decimal  # Calculated tender salary / opening bid
    contract_years: int  # 1 for ERFA, 1 default for RFA
    compensation: str  # Draft pick compensation description


@dataclass
class TenderResult:
    """Full result from calculate_tenders — all applicable tender options."""

    player_id: int
    player_name: str
    position: str
    previous_salary: Decimal
    erfa_option: TenderOption | None  # ERFA if eligible
    rfa_options: list[TenderOption] = field(default_factory=list)  # FRFA, SRFA, ORFA, RRFA
    erfa_eligible: bool = False
    rfa_eligible: bool = False
    ineligibility_reasons: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# ERFA tender calculation
# ---------------------------------------------------------------------------


def calculate_erfa_salary(prev_salary: Decimal, season: int) -> Decimal:
    """Calculate ERFA tender salary.

    Formula: MAX(veteran_minimum, 1.10 x prev_salary)
    """
    vet_min = get_veteran_minimum(season)
    erfa_pct = Decimal("1.10")
    return max(vet_min, erfa_pct * prev_salary)


# ---------------------------------------------------------------------------
# RFA tender calculations
# ---------------------------------------------------------------------------

def _get_nfl_rfa_prices(season: int) -> dict[str, Decimal]:
    """Load NFL RFA tender prices for the given season from contracts.json.

    Falls back to the latest available year if the exact season is missing.
    """
    constants = get_contract_constants()
    prices_by_year = constants.get("rfa_tenders", {}).get("nfl_rfa_prices_by_year", {})
    key = str(season)
    if key in prices_by_year:
        raw = prices_by_year[key]
    else:
        # Fall back to the latest available year
        if prices_by_year:
            latest_key = max(prices_by_year.keys(), key=int)
            raw = prices_by_year[latest_key]
        else:
            # Ultimate fallback — zeros (should not happen with proper config)
            raw = {"FRFA": 0, "SRFA": 0, "ORFA": 0, "RRFA": 0}
    return {k: Decimal(str(v)) for k, v in raw.items()}


def calculate_frfa_bid(
    prev_salary: Decimal,
    nfl_frfa_price: Decimal | None = None,
    *,
    season: int = 2026,
) -> Decimal:
    """Calculate FRFA (First Round RFA) opening bid.

    Formula: FLOOR_100K(MAX(NFL_FRFA_price, 2.20 x prev_salary))
    """
    constants = get_contract_constants()
    multiplier = Decimal(str(constants["rfa_tenders"]["frfa_salary_multiplier_vs_previous"]))
    nfl_price = nfl_frfa_price if nfl_frfa_price is not None else _get_nfl_rfa_prices(season)["FRFA"]
    return floor_100k(max(nfl_price, multiplier * prev_salary))


def calculate_srfa_bid(
    prev_salary: Decimal,
    nfl_srfa_price: Decimal | None = None,
    *,
    season: int = 2026,
) -> Decimal:
    """Calculate SRFA (Second Round RFA) opening bid.

    Formula: FLOOR_100K(MAX(NFL_SRFA_price, 1.65 x prev_salary))
    """
    constants = get_contract_constants()
    multiplier = Decimal(str(constants["rfa_tenders"]["srfa_salary_multiplier_vs_previous"]))
    nfl_price = nfl_srfa_price if nfl_srfa_price is not None else _get_nfl_rfa_prices(season)["SRFA"]
    return floor_100k(max(nfl_price, multiplier * prev_salary))


def calculate_orfa_bid(
    prev_salary: Decimal,
    nfl_orfa_price: Decimal | None = None,
    *,
    season: int = 2026,
) -> Decimal:
    """Calculate ORFA (Original Round RFA) opening bid.

    Formula: FLOOR_100K(MAX(NFL_ORFA_price, 1.10 x prev_salary))
    """
    constants = get_contract_constants()
    multiplier = Decimal(str(constants["rfa_tenders"]["orfa_salary_multiplier_vs_previous"]))
    nfl_price = nfl_orfa_price if nfl_orfa_price is not None else _get_nfl_rfa_prices(season)["ORFA"]
    return floor_100k(max(nfl_price, multiplier * prev_salary))


def calculate_rrfa_bid(
    nfl_rrfa_price: Decimal | None = None,
    *,
    season: int = 2026,
) -> Decimal:
    """Calculate RRFA (Right-of-Refusal RFA) opening bid.

    Formula: FLOOR_100K(NFL_RRFA_price)
    """
    nfl_price = nfl_rrfa_price if nfl_rrfa_price is not None else _get_nfl_rfa_prices(season)["RRFA"]
    return floor_100k(nfl_price)


# ---------------------------------------------------------------------------
# ERFA eligibility
# ---------------------------------------------------------------------------


async def check_erfa_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> tuple[bool, str | None]:
    """Check whether a player is eligible for an ERFA tender.

    Returns ``(True, None)`` if eligible, ``(False, reason)`` if not.

    Eligibility rules:
    - Player must have 0 years remaining (expired contract)
    - Previous contract must have been signed at or below the veteran minimum
      in one of the previous two league years
    - Previous contract must NOT be an ERFA contract itself
    """
    # Load expired contract (years_remaining == 0) — explicitly filter to avoid
    # picking an active contract when multiple exist per player/season (Q3-D fix)
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

    # Check if the expired contract is itself an ERFA contract
    desig = contract.designation or ""
    if "ERFA" in desig:
        return False, "Player's expired contract is an ERFA contract"

    # Contract must have been signed at or below veteran minimum in one of
    # the previous two league years
    prev_salary = Decimal(str(contract.salary))
    signed_season = contract.signed_season

    eligible_seasons = [season - 1, season - 2]
    if signed_season not in eligible_seasons:
        return False, "Contract was not signed in one of the previous two league years"

    vet_min_at_signing = get_veteran_minimum(signed_season)
    if prev_salary > vet_min_at_signing:
        return False, "Previous contract salary exceeds veteran minimum at time of signing"

    return True, None


# ---------------------------------------------------------------------------
# RFA eligibility
# ---------------------------------------------------------------------------


# Contract types that make a player ineligible for RFA if from 2021 or earlier
_INELIGIBLE_TYPES_2021_OR_EARLIER = frozenset({
    "EFT", "NEFT", "TT", "NEFToff", "TToff",
    "FRFA", "SRFA", "ORFA", "RRFA",
    "B/R", "EXT", "5YO",
})


async def check_rfa_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> tuple[bool, str | None]:
    """Check whether a player is eligible for RFA tenders.

    Returns ``(True, None)`` if eligible, ``(False, reason)`` if not.

    Eligibility rules:
    - Player must have 0 years remaining (expired contract)
    - Contract must not be 4+ years old
    - Contract must not include ineligible types from 2021 or earlier
    - Must not be a multi-year UFA from 2023 or earlier
    """
    # Load expired contract (years_remaining == 0) — explicitly filter to avoid
    # picking an active contract when multiple exist per player/season (Q3-D fix)
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

    desig = contract.designation or ""
    signed_season = contract.signed_season

    # Contract must not be 4+ years old
    contract_age = season - signed_season
    if contract_age >= 4:
        return False, "Contract is 4+ years old"

    # Check ineligible contract types from 2021 or earlier
    if signed_season <= 2021:
        for ineligible_type in _INELIGIBLE_TYPES_2021_OR_EARLIER:
            if ineligible_type in desig:
                return (
                    False,
                    f"Contract includes ineligible type '{ineligible_type}' from {signed_season}",
                )

    # Universal rule (R10-D fix): "a player's expiring contract must not be...
    # a previous RFA contract" — applies regardless of signed year.
    # The bylaws opening paragraph states this as a universal condition.
    _RFA_DESIGNATIONS = ("FRFA", "SRFA", "ORFA", "RRFA")
    for rfa_type in _RFA_DESIGNATIONS:
        if rfa_type in desig:
            return (
                False,
                f"Contract is a previous RFA contract ('{rfa_type}')",
            )

    # Must not be a multi-year UFA from 2023 or earlier (R4-D fix)
    if signed_season <= 2023 and "UFA" in desig:
        # Look up the original contract at signed_season to get original years_remaining.
        # This avoids the off-by-N error of using (season - signed_season) which
        # conflates carried-forward seasons with original contract length.
        orig_result = await session.execute(
            select(Contract.years_remaining)
            .where(
                Contract.player_id == player_id,
                Contract.signed_season == signed_season,
                Contract.season == signed_season,
            )
            .limit(1)
        )
        orig_years = orig_result.scalar_one_or_none()
        # If we can find the original contract, use its years_remaining as original length.
        # If not found (data gap), fall back to contract_age which may over-count but
        # errs on the side of blocking (safer).
        original_length = orig_years if orig_years is not None else (season - signed_season)
        if original_length > 1:
            return False, "Multi-year UFA contract from 2023 or earlier"

    return True, None


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


async def calculate_tenders(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> TenderResult:
    """Calculate all applicable tender options for a player.

    Returns a ``TenderResult`` with ERFA option (if eligible) and/or
    RFA options (FRFA, SRFA, ORFA, RRFA if eligible).
    """
    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()
    if player is None:
        return TenderResult(
            player_id=player_id,
            player_name="Unknown",
            position="",
            previous_salary=Decimal("0"),
            erfa_option=None,
            ineligibility_reasons=["Player not found"],
        )

    # Load previous expired contract (years_remaining=0) — explicitly filter
    # to avoid picking wrong contract when multiple exist (Q3-D fix)
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

    ineligibility_reasons: list[str] = []
    erfa_option: TenderOption | None = None
    rfa_options: list[TenderOption] = []

    # Check ERFA eligibility
    erfa_eligible, erfa_reason = await check_erfa_eligibility(session, player_id, season)
    if erfa_eligible:
        erfa_salary = calculate_erfa_salary(prev_salary, season)
        erfa_option = TenderOption(
            tender_type="ERFA",
            salary=erfa_salary,
            contract_years=1,
            compensation="None — exclusive rights retained",
        )
    elif erfa_reason:
        ineligibility_reasons.append(f"ERFA: {erfa_reason}")

    # Check RFA eligibility
    rfa_eligible, rfa_reason = await check_rfa_eligibility(session, player_id, season)
    if rfa_eligible:
        # Calculate all 4 RFA tender options
        rfa_options = [
            TenderOption(
                tender_type="FRFA",
                salary=calculate_frfa_bid(prev_salary, season=season),
                contract_years=1,
                compensation="1st round draft pick",
            ),
            TenderOption(
                tender_type="SRFA",
                salary=calculate_srfa_bid(prev_salary, season=season),
                contract_years=1,
                compensation="2nd round draft pick",
            ),
            TenderOption(
                tender_type="ORFA",
                salary=calculate_orfa_bid(prev_salary, season=season),
                contract_years=1,
                compensation="Draft pick per ADL draft position",
            ),
            TenderOption(
                tender_type="RRFA",
                salary=calculate_rrfa_bid(season=season),
                contract_years=1,
                compensation="None",
            ),
        ]
    elif rfa_reason:
        ineligibility_reasons.append(f"RFA: {rfa_reason}")

    return TenderResult(
        player_id=player_id,
        player_name=player.name,
        position=player.position,
        previous_salary=prev_salary,
        erfa_option=erfa_option,
        rfa_options=rfa_options,
        erfa_eligible=erfa_eligible,
        rfa_eligible=rfa_eligible,
        ineligibility_reasons=ineligibility_reasons,
    )
