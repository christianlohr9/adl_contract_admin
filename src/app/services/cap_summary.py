"""Team cap summary service — per-player cap breakdown and team-level aggregation.

Async functions that query the DB for active contracts and compute cap penalties.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from app.models.contract import Contract, ContractStatus
from app.services.cap_penalties import PenaltyResult, calculate_penalty

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

_ZERO = Decimal("0")


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class PlayerCapDetail:
    """Per-player cap breakdown including penalty if dropped."""

    player_id: int
    player_name: str
    position: str
    salary: Decimal
    contract_type: str
    years_remaining: int
    designation: str
    penalty_if_dropped: PenaltyResult


@dataclass
class TeamCapSummary:
    """Team-level cap summary with per-player details."""

    team_id: int
    team_name: str
    season: int
    total_salary: Decimal
    salary_by_type: dict[str, Decimal] = field(default_factory=dict)
    total_penalty_exposure: Decimal = _ZERO
    penalty_by_type: dict[str, Decimal] = field(default_factory=dict)
    player_details: list[PlayerCapDetail] = field(default_factory=list)
    roster_count: int = 0


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------


async def get_player_cap_detail(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> PlayerCapDetail | None:
    """Load a single player's active contract and compute cap detail.

    Returns None if no active contract found for the player+season.
    """
    stmt = (
        select(Contract)
        .options(joinedload(Contract.player))
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == ContractStatus.ACTIVE,
        )
    )
    contract = (await session.execute(stmt)).scalar_one_or_none()
    if contract is None:
        return None

    return _build_player_detail(contract)


async def get_team_cap_summary(
    session: AsyncSession,
    team_id: int,
    season: int,
) -> TeamCapSummary:
    """Build a full team cap summary with per-player breakdown.

    Uses a single DB query joining Contract + Player, then computes
    penalties in Python (no N+1 queries).
    """
    # Single query: all active contracts for team+season with player eagerly loaded
    stmt = (
        select(Contract)
        .options(joinedload(Contract.player), joinedload(Contract.team))
        .where(
            Contract.team_id == team_id,
            Contract.season == season,
            Contract.status == ContractStatus.ACTIVE,
        )
    )
    result = await session.execute(stmt)
    contracts = result.scalars().unique().all()

    # Aggregate
    total_salary = _ZERO
    salary_by_type: dict[str, Decimal] = {"NG": _ZERO, "SD": _ZERO, "FG": _ZERO}
    total_penalty_exposure = _ZERO
    penalty_by_type: dict[str, Decimal] = {"NG": _ZERO, "SD": _ZERO, "FG": _ZERO}
    player_details: list[PlayerCapDetail] = []

    for contract in contracts:
        detail = _build_player_detail(contract)
        player_details.append(detail)

        sal = detail.salary
        ct = detail.contract_type
        total_salary += sal
        salary_by_type[ct] = salary_by_type.get(ct, _ZERO) + sal

        y1_penalty = detail.penalty_if_dropped.year_1_penalty
        total_penalty_exposure += y1_penalty
        penalty_by_type[ct] = penalty_by_type.get(ct, _ZERO) + y1_penalty

    # Sort by salary descending
    player_details.sort(key=lambda d: d.salary, reverse=True)

    # Resolve team name from first contract (all share same team)
    team_name = ""
    if contracts:
        team_name = contracts[0].team.name if contracts[0].team else ""

    return TeamCapSummary(
        team_id=team_id,
        team_name=team_name,
        season=season,
        total_salary=total_salary,
        salary_by_type=salary_by_type,
        total_penalty_exposure=total_penalty_exposure,
        penalty_by_type=penalty_by_type,
        player_details=player_details,
        roster_count=len(player_details),
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_player_detail(contract: Contract) -> PlayerCapDetail:
    """Build a PlayerCapDetail from a Contract (with player eagerly loaded)."""
    salary = Decimal(str(contract.salary))
    ct = str(contract.contract_type)

    penalty = calculate_penalty(
        contract_type=ct,
        salary=salary,
        years_remaining=contract.years_remaining,
    )

    player = contract.player
    return PlayerCapDetail(
        player_id=contract.player_id,
        player_name=player.name if player else "Unknown",
        position=player.position if player else "??",
        salary=salary,
        contract_type=ct,
        years_remaining=contract.years_remaining,
        designation=contract.designation,
        penalty_if_dropped=penalty,
    )
