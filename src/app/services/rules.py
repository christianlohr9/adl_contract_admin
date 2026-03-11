"""Rules loader service — cached access to league rules JSON/YAML files and salary helpers."""

from __future__ import annotations

import json
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from functools import lru_cache
from pathlib import Path

import yaml

# Project root / rules directory, resolved relative to this file:
# services/ -> app/ -> src/ -> project root
_RULES_DIR = Path(__file__).resolve().parent.parent.parent.parent / "rules"


# ---------------------------------------------------------------------------
# Generic loaders (cached)
# ---------------------------------------------------------------------------


@lru_cache(maxsize=16)
def load_constants(name: str) -> dict:
    """Load ``rules/constants/{name}.json`` and return the parsed dict."""
    path = _RULES_DIR / "constants" / f"{name}.json"
    with path.open() as f:
        return json.load(f)


@lru_cache(maxsize=16)
def load_formulas(name: str) -> dict:
    """Load ``rules/formulas/{name}.yaml`` and return the parsed dict."""
    path = _RULES_DIR / "formulas" / f"{name}.yaml"
    with path.open() as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------


def get_contract_constants() -> dict:
    """Return contracts.json constants."""
    return load_constants("contracts")


def get_salary_cap_constants() -> dict:
    """Return salary_cap.json constants."""
    return load_constants("salary_cap")


def get_league_constants() -> dict:
    """Return league.json constants."""
    return load_constants("league")


def get_extension_formulas() -> dict:
    """Return extensions.yaml formulas."""
    return load_formulas("extensions")


def get_free_agency_formulas() -> dict:
    """Return free_agency.yaml formulas."""
    return load_formulas("free_agency")


# ---------------------------------------------------------------------------
# Salary rounding helpers (Decimal in, Decimal out — salaries in millions)
# ---------------------------------------------------------------------------


def round_to_10k(amount: Decimal) -> Decimal:
    """Round to nearest $10k (0.01 in millions), using ROUND_HALF_UP."""
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def round_to_100k(amount: Decimal) -> Decimal:
    """Round to nearest $100k (0.1 in millions), using ROUND_HALF_UP."""
    return amount.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


def floor_100k(amount: Decimal) -> Decimal:
    """Floor to $100k (0.1 in millions)."""
    return amount.quantize(Decimal("0.1"), rounding=ROUND_FLOOR)


def ceil_100k(amount: Decimal) -> Decimal:
    """Ceil to $100k (0.1 in millions)."""
    return amount.quantize(Decimal("0.1"), rounding=ROUND_CEILING)


def round_to_nearest_4(value: int) -> int:
    """Round to nearest multiple of 4 (for PR Starter Floor)."""
    return int(4 * round(value / 4))


# ---------------------------------------------------------------------------
# Season-specific value getters
# ---------------------------------------------------------------------------


def _lookup_by_year(mapping: dict[str, object], season: int) -> Decimal:
    """Look up a value by season year, falling back to the latest available year."""
    key = str(season)
    if key in mapping:
        return Decimal(str(mapping[key]))
    # Fall back to the latest available year
    latest_key = max(mapping.keys(), key=int)
    return Decimal(str(mapping[latest_key]))


def get_veteran_minimum(season: int) -> Decimal:
    """Return the veteran minimum salary (in millions) for the given season."""
    c = get_contract_constants()
    return _lookup_by_year(c["salary_minimums"]["veteran_minimum_by_year"], season)


def get_rookie_minimum(season: int) -> Decimal:
    """Return the rookie minimum salary (in millions) for the given season."""
    c = get_contract_constants()
    return _lookup_by_year(c["salary_minimums"]["rookie_minimum_by_year"], season)


def get_sd_minimum(season: int) -> Decimal:
    """Return the SD minimum salary (in millions) for the given season."""
    c = get_contract_constants()
    return _lookup_by_year(c["salary_minimums"]["sd_minimum_by_year"], season)


def get_salary_growth_rate() -> Decimal:
    """Return the annual salary growth rate as a Decimal (e.g. ``Decimal('0.10')``)."""
    c = get_contract_constants()
    return Decimal(str(c["salary_growth_rate"]))


# Suppress unused import warning — math is used by round_to_nearest_4's round()
__all__ = [
    "load_constants",
    "load_formulas",
    "get_contract_constants",
    "get_salary_cap_constants",
    "get_league_constants",
    "get_extension_formulas",
    "get_free_agency_formulas",
    "round_to_10k",
    "round_to_100k",
    "floor_100k",
    "ceil_100k",
    "round_to_nearest_4",
    "get_veteran_minimum",
    "get_rookie_minimum",
    "get_sd_minimum",
    "get_salary_growth_rate",
]
