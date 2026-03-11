"""Cap penalty calculator service — NG/SD/FG penalty calculations per bylaws rules.

All salary math uses ``Decimal`` — floats from DB are converted at the boundary.
Uses ``round_to_10k`` for penalty rounding and ``ceil_100k``/``floor_100k`` only
for FG multi-year split per bylaws.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.services.rules import (
    ceil_10k,
    floor_10k,
    get_salary_cap_constants,
    round_to_10k,
)

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class PenaltyResult:
    """Result of a cap penalty calculation."""

    contract_type: str
    salary: Decimal
    years_remaining: int
    year_1_penalty: Decimal
    additional_years_penalty: Decimal
    total_penalty: Decimal
    notes: str


# ---------------------------------------------------------------------------
# Per-type penalty calculators (pure functions)
# ---------------------------------------------------------------------------

_ZERO = Decimal("0")


def calculate_ng_penalty(salary: Decimal, years_remaining: int) -> PenaltyResult:
    """Calculate NG (Non-Guaranteed) penalty — always $0."""
    return PenaltyResult(
        contract_type="NG",
        salary=salary,
        years_remaining=years_remaining,
        year_1_penalty=_ZERO,
        additional_years_penalty=_ZERO,
        total_penalty=_ZERO,
        notes="NG penalty: no penalty regardless of remaining years",
    )


def calculate_sd_penalty(salary: Decimal, years_remaining: int) -> PenaltyResult:
    """Calculate SD (Standard) penalty per bylaws.

    year_1 = 60% of salary
    additional_years = 30% of salary per remaining year beyond year 1
    """
    rates = _get_penalty_rates()
    year1_pct = Decimal(str(rates["SD"]["year1_pct"]))
    addl_pct = Decimal(str(rates["SD"]["additional_years_pct_per_year"]))

    year_1 = round_to_10k(year1_pct * salary)

    if years_remaining > 1:
        additional = round_to_10k(addl_pct * salary * Decimal(str(years_remaining - 1)))
    else:
        additional = _ZERO

    total = year_1 + additional

    notes = f"SD penalty: {year1_pct:.0%} year-1"
    if years_remaining > 1:
        notes += f" + {addl_pct:.0%} x {years_remaining - 1} additional years"

    return PenaltyResult(
        contract_type="SD",
        salary=salary,
        years_remaining=years_remaining,
        year_1_penalty=year_1,
        additional_years_penalty=additional,
        total_penalty=total,
        notes=notes,
    )


def calculate_fg_penalty(salary: Decimal, years_remaining: int) -> PenaltyResult:
    """Calculate FG (Fully Guaranteed) penalty per bylaws.

    total = sum(salary * 1.1^n for n in 0..years_remaining-1)
    If multi-year: year_1 = ceil_10k(total/2), year_2 = floor_10k(total/2)
    """
    increase = Decimal(str(_get_penalty_rates()["FG"]["annual_increase"]))
    multiplier = Decimal("1") + increase  # 1.10

    total = _ZERO
    for n in range(years_remaining):
        total += salary * multiplier ** n
    total = round_to_10k(total)

    if years_remaining > 1:
        half = total / Decimal("2")
        year_1 = ceil_10k(half)
        year_2 = floor_10k(half)
    else:
        year_1 = total
        year_2 = _ZERO

    notes = f"FG penalty: sum of salary x 1.1^n for {years_remaining} years = ${total}m"
    if years_remaining > 1:
        notes += f", split: year-1=${year_1}m (ceil), year-2=${year_2}m (floor)"

    return PenaltyResult(
        contract_type="FG",
        salary=salary,
        years_remaining=years_remaining,
        year_1_penalty=year_1,
        additional_years_penalty=year_2,
        total_penalty=total,
        notes=notes,
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def calculate_penalty(
    contract_type: str,
    salary: Decimal,
    years_remaining: int,
    *,
    is_suspended: bool = False,
    is_retired: bool = False,
    is_deceased: bool = False,
    pre_july_1: bool = True,
) -> PenaltyResult:
    """Calculate cap penalty for a contract, applying modifiers.

    Args:
        contract_type: "NG", "SD", or "FG".
        salary: Contract salary in millions (Decimal).
        years_remaining: Years left including current season.
        is_suspended: Apply 50% suspended discount.
        is_retired: Retired players incur no penalty.
        is_deceased: Deceased players incur no penalty.
        pre_july_1: If True, accelerate total penalty into year 1 (lump sum).

    Returns:
        PenaltyResult with penalty breakdown and explanation notes.
    """
    # Retired/deceased: no penalty
    if is_retired or is_deceased:
        status = "retired" if is_retired else "deceased"
        return PenaltyResult(
            contract_type=contract_type,
            salary=salary,
            years_remaining=years_remaining,
            year_1_penalty=_ZERO,
            additional_years_penalty=_ZERO,
            total_penalty=_ZERO,
            notes=f"No penalty — player is {status}",
        )

    # Compute base penalty per contract type
    if contract_type == "NG":
        result = calculate_ng_penalty(salary, years_remaining)
    elif contract_type == "SD":
        result = calculate_sd_penalty(salary, years_remaining)
    elif contract_type == "FG":
        result = calculate_fg_penalty(salary, years_remaining)
    else:
        msg = f"Unknown contract type: {contract_type}"
        raise ValueError(msg)

    # Suspended: 50% discount on total
    if is_suspended:
        cap_constants = get_salary_cap_constants()
        discount = Decimal(str(cap_constants["suspended_player_penalty_discount"]))
        result.year_1_penalty = round_to_10k(result.year_1_penalty * discount)
        result.additional_years_penalty = round_to_10k(
            result.additional_years_penalty * discount
        )
        result.total_penalty = result.year_1_penalty + result.additional_years_penalty
        result.notes += f" (suspended: {discount:.0%} discount applied)"

    # Acceleration: pre-July 1 = lump sum in year 1
    if pre_july_1 and result.additional_years_penalty > _ZERO:
        result.year_1_penalty = result.year_1_penalty + result.additional_years_penalty
        result.additional_years_penalty = _ZERO
        result.notes += " [accelerated: pre-July 1 lump sum]"

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_penalty_rates() -> dict:
    """Return cap penalty rates from salary_cap.json, restructured for convenience."""
    cap = get_salary_cap_constants()
    raw = cap["cap_penalty_rates"]
    return {
        "NG": raw["NG"],
        "SD": raw["SD"],
        "FG": {
            "year1_pct": raw["FG"]["year1_pct"],
            "annual_increase": raw["FG"]["additional_years_annual_increase"],
            "multi_year_split_year1_rounded": raw["FG"]["multi_year_split_year1_rounded"],
            "multi_year_split_year2_rounded": raw["FG"]["multi_year_split_year2_rounded"],
        },
    }
