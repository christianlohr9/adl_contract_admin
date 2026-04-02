"""Tender calculations — ERFA and RFA tender salary/bid services.

Calculates ERFA tender salaries and RFA tender opening bids (FRFA, SRFA, ORFA, RRFA)
with eligibility checks based on bylaws rules.

All salary math uses ``Decimal`` — floats from DB are converted at the boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from app.models.contract import Contract
from app.models.player import Player
from app.models.player_season import PlayerSeason
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
# Conference-scoped accrued seasons
# ---------------------------------------------------------------------------

# Team ID ranges per conference (DB IDs = MFL franchise ordinal + 128)
_NFC_TEAM_IDS = range(129, 145)  # 129-144
_AFC_TEAM_IDS = range(145, 161)  # 145-160


def _same_conference_team_ids(team_id: int) -> range:
    """Return the team_id range for the conference that *team_id* belongs to."""
    if team_id in _NFC_TEAM_IDS:
        return _NFC_TEAM_IDS
    return _AFC_TEAM_IDS


async def _get_conference_accrued_seasons(
    session: AsyncSession,
    player_id: int,
    team_id: int,
    season: int,
) -> int:
    """Count distinct prior seasons the player accrued in the same conference.

    Uses the player_seasons table (built from weekly MFL roster scans) as the
    golden source.  A season only counts as "accrued" if the player was on a
    conference roster for at least 6 weeks (matching the NFL's accrued season
    threshold).

    Falls back to contract history if player_seasons has no data.
    """
    conf_teams = _same_conference_team_ids(team_id)

    # Primary: player_seasons with 6-week minimum per conference per season.
    # Sum weeks across all teams in the conference for each season, then
    # count seasons where the total meets the threshold.
    from sqlalchemy import literal_column  # noqa: PLC0415

    conf_season_weeks = (
        select(
            PlayerSeason.season,
            func.sum(PlayerSeason.weeks_rostered).label("total_weeks"),
        )
        .where(
            PlayerSeason.player_id == player_id,
            PlayerSeason.team_id.in_(conf_teams),
            PlayerSeason.season < season,
        )
        .group_by(PlayerSeason.season)
        .having(func.sum(PlayerSeason.weeks_rostered) >= 6)
        .subquery()
    )
    ps_result = await session.execute(
        select(func.count()).select_from(conf_season_weeks)
    )
    ps_count = ps_result.scalar() or 0
    if ps_count > 0:
        return ps_count

    # Fallback: contract history (end-of-season snapshots — no week threshold)
    c_result = await session.execute(
        select(func.count(Contract.season.distinct())).where(
            Contract.player_id == player_id,
            Contract.team_id.in_(conf_teams),
            Contract.season < season,
        )
    )
    return c_result.scalar() or 0


# ---------------------------------------------------------------------------
# ERFA eligibility
# ---------------------------------------------------------------------------

# Maximum accrued seasons for ERFA eligibility (exclusive).
# Players with 3+ prior ADL seasons are not ERFA-eligible.
_ERFA_MAX_ACCRUED_SEASONS = 3


async def check_erfa_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
    team_id: int | None = None,
) -> tuple[bool, str | None]:
    """Check whether a player is eligible for an ERFA tender.

    Returns ``(True, None)`` if eligible, ``(False, reason)`` if not.

    Parameters
    ----------
    team_id : int | None
        Optional team to scope contract lookups to. In a two-conference league
        a player may have contracts on multiple teams; scoping ensures we check
        the correct team slot.

    Eligibility rules:
    - Player must have 0 years remaining (expired contract)
    - Must not also have an active contract on the same team (already re-signed)
    - Previous contract must have been signed at or below the veteran minimum
      in one of the previous two league years
    - Previous contract must NOT be an ERFA contract itself
    """
    # Load expired contract (years_remaining == 0) — scope to team if given
    expired_q = (
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.years_remaining == 0,
        )
    )
    if team_id is not None:
        expired_q = expired_q.where(Contract.team_id == team_id)
    expired_q = expired_q.order_by(Contract.salary.desc()).limit(1)

    result = await session.execute(expired_q)
    contract = result.scalar_one_or_none()

    if contract is None:
        return False, "No expired contract found (player may still have years remaining)"

    # If the player also has an active contract on the same team, they've been re-signed.
    # Scope to the same team_id as the expired contract.
    scope_team = team_id if team_id is not None else contract.team_id
    active_check = await session.execute(
        select(Contract.id)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.team_id == scope_team,
            Contract.years_remaining > 0,
        )
        .limit(1)
    )
    if active_check.scalar_one_or_none() is not None:
        return False, "Player has an active contract (already re-signed this season)"

    # Check if the expired contract is itself an ERFA contract
    desig = contract.designation or ""
    if "ERFA" in desig:
        return False, "Player's expired contract is an ERFA contract"

    # Contract must have been signed in one of the previous two league years
    signed_season = contract.signed_season

    eligible_seasons = [season - 1, season - 2]
    if signed_season not in eligible_seasons:
        return False, "Contract was not signed in one of the previous two league years"

    # Compare salary at signing against the veteran minimum at that time.
    # The current-season row may have a bumped salary (vet-min carry-forward),
    # so look up the original contract at signed_season for the true signing salary.
    # Scope to the same team to avoid picking a different conference's contract.
    orig_q = (
        select(Contract.salary)
        .where(
            Contract.player_id == player_id,
            Contract.team_id == contract.team_id,
            Contract.signed_season == signed_season,
            Contract.season == signed_season,
        )
        .limit(1)
    )
    orig_salary_result = await session.execute(orig_q)
    orig_salary_raw = orig_salary_result.scalar_one_or_none()
    # If original contract found, use its salary; else fall back to current row
    prev_salary = Decimal(str(orig_salary_raw)) if orig_salary_raw is not None else Decimal(str(contract.salary))

    vet_min_at_signing = get_veteran_minimum(signed_season)
    if prev_salary > vet_min_at_signing:
        return False, "Previous contract salary exceeds veteran minimum at time of signing"

    # ERFA requires fewer than 3 accrued seasons in the same conference.
    # Conference-scoped: only count seasons on teams in the same conference.
    accrued_seasons = await _get_conference_accrued_seasons(
        session, player_id, scope_team, season,
    )
    if accrued_seasons >= _ERFA_MAX_ACCRUED_SEASONS:
        return False, f"Player has {accrued_seasons} accrued seasons (ERFA requires < {_ERFA_MAX_ACCRUED_SEASONS})"

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

# Required accrued seasons for RFA eligibility (exactly this many).
# Players with fewer are ERFA-eligible, players with more are UFA.
_RFA_REQUIRED_ACCRUED_SEASONS = 3


async def check_rfa_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
    team_id: int | None = None,
) -> tuple[bool, str | None]:
    """Check whether a player is eligible for RFA tenders.

    Returns ``(True, None)`` if eligible, ``(False, reason)`` if not.

    Parameters
    ----------
    team_id : int | None
        Optional team to scope contract lookups to. In a two-conference league
        a player may have contracts on multiple teams; scoping ensures we check
        the correct team slot.

    Eligibility rules:
    - Player must have 0 years remaining (expired contract)
    - Must not also have an active contract on the same team (already re-signed)
    - Contract must not be 4+ years old
    - Contract must not include ineligible types from 2021 or earlier
    - Must not be a multi-year UFA from 2023 or earlier
    - Must not be a previous RFA contract (universal rule)
    """
    # Load expired contract (years_remaining == 0) — scope to team if given
    expired_q = (
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.years_remaining == 0,
        )
    )
    if team_id is not None:
        expired_q = expired_q.where(Contract.team_id == team_id)
    expired_q = expired_q.order_by(Contract.salary.desc()).limit(1)

    result = await session.execute(expired_q)
    contract = result.scalar_one_or_none()

    if contract is None:
        return False, "No expired contract found (player may still have years remaining)"

    # If the player also has an active contract on the same team, they've been re-signed.
    scope_team = team_id if team_id is not None else contract.team_id
    active_check = await session.execute(
        select(Contract.id)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.team_id == scope_team,
            Contract.years_remaining > 0,
        )
        .limit(1)
    )
    if active_check.scalar_one_or_none() is not None:
        return False, "Player has an active contract (already re-signed this season)"

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

    # RFA requires exactly 3 accrued seasons in the same conference.
    # Conference-scoped: only count seasons on teams in the same conference.
    accrued_seasons = await _get_conference_accrued_seasons(
        session, player_id, scope_team, season,
    )
    if accrued_seasons != _RFA_REQUIRED_ACCRUED_SEASONS:
        return (
            False,
            f"Player has {accrued_seasons} accrued seasons (RFA requires exactly {_RFA_REQUIRED_ACCRUED_SEASONS})",
        )

    return True, None


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


async def calculate_tenders(
    session: AsyncSession,
    player_id: int,
    season: int,
    team_id: int | None = None,
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

    # Load previous expired contract (years_remaining=0) — scope to team if given
    expired_q = (
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.years_remaining == 0,
        )
    )
    if team_id is not None:
        expired_q = expired_q.where(Contract.team_id == team_id)
    expired_q = expired_q.order_by(Contract.salary.desc()).limit(1)

    contract_result = await session.execute(expired_q)
    contract = contract_result.scalar_one_or_none()
    prev_salary = Decimal(str(contract.salary)) if contract else Decimal("0")

    ineligibility_reasons: list[str] = []
    erfa_option: TenderOption | None = None
    rfa_options: list[TenderOption] = []

    # Check ERFA eligibility
    erfa_eligible, erfa_reason = await check_erfa_eligibility(session, player_id, season, team_id)
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
    rfa_eligible, rfa_reason = await check_rfa_eligibility(session, player_id, season, team_id)
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
