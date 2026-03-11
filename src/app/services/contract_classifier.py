"""Contract type classifier — determines NG/SD/FG classification per bylaws rules.

Pure function (no DB access) — all inputs passed as parameters.
"""

from __future__ import annotations

import re
from decimal import Decimal  # noqa: TC003 - used at runtime for salary comparison

from app.models.contract import ContractType
from app.services.rules import get_sd_minimum

# Patterns for designation parsing
_DRAFT_PICK_RE = re.compile(r"\b(\d{4})\s+(\d)\.(\d+)\b")  # e.g. "2021 1.02" or "2024 2.15"
_TAG_RE = re.compile(r"\b(EFT|NEFT|TT)\b")  # Franchise/transition tags (word boundary)
_EXT_RE = re.compile(r"\bEXT\b")  # Extension contracts


def classify_contract_type(
    salary: Decimal,
    designation: str,
    signed_season: int,
    season: int,
    draft_round: int | None = None,
) -> ContractType:
    """Classify a contract as NG, SD, or FG per ADL bylaws rules.

    Classification priority (checked in order):
    1. **FG** — 1st round drafted rookies, 5YO exercised, franchise-tagged players
    2. **SD** — EXT contracts (always SD), 2nd+ round rookies at/above SD min,
       all others at/above SD minimum
    3. **NG** — Below SD minimum salary, or unsigned drafted rookies

    Args:
        salary: Contract salary in millions (Decimal).
        designation: Contract info string (e.g. "2024 1.05", "2022 EXT", "2024 UFA").
        signed_season: Season the contract was originally signed.
        season: Current ADL season year (for SD minimum lookup).
        draft_round: NFL draft round (1-7) if known, else None.
            Falls back to parsing from designation.

    Returns:
        ContractType enum value (NG, SD, or FG).
    """
    desig_upper = designation.upper()

    # ── FG checks ──

    # 5YO exercised: "+" in designation or "5YO" in designation
    if "+" in desig_upper or "5YO" in desig_upper:
        return ContractType.FG

    # Franchise/transition tags: EFT, NEFT, TT as whole words
    if _TAG_RE.search(desig_upper):
        return ContractType.FG

    # 1st round drafted rookie (from draft_round param or parsed from designation)
    effective_round = draft_round
    if effective_round is None:
        match = _DRAFT_PICK_RE.search(designation)
        if match:
            effective_round = int(match.group(2))

    if effective_round == 1:
        return ContractType.FG

    # ── SD checks ──

    # EXT contracts are always SD per bylaws
    if _EXT_RE.search(desig_upper):
        return ContractType.SD

    # At or above SD minimum -> SD
    sd_min = get_sd_minimum(season)
    if salary >= sd_min:
        return ContractType.SD

    # ── Default: NG ──
    return ContractType.NG
