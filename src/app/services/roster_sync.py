"""Roster and contract sync service — upserts MFL rosters into RosterEntry and Contract tables."""

from __future__ import annotations

import logging
import re
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import distinct, select

from app.mfl.models import MFLRostersResponse
from app.models.contract import Contract, ContractStatus
from app.models.player import Player
from app.models.roster import RosterEntry, RosterStatus
from app.models.team import Team
from app.services.contract_classifier import classify_contract_type
from app.services.team_sync import SyncResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.mfl.client import MFLClient

logger = logging.getLogger(__name__)

# Mapping from MFL status strings to RosterStatus enum
_MFL_STATUS_MAP: dict[str, RosterStatus] = {
    "ROSTER": RosterStatus.ROSTER,
    "INJURED_RESERVE": RosterStatus.INJURED_RESERVE,
    "TAXI_SQUAD": RosterStatus.TAXI_SQUAD,
}

# Regex to extract a 4-digit year from the designation string (e.g. "2019 EXT" -> 2019)
_YEAR_RE = re.compile(r"\b(\d{4})\b")


def _parse_roster_status(mfl_status: str) -> RosterStatus:
    """Map MFL status string to RosterStatus, defaulting to ROSTER."""
    status = _MFL_STATUS_MAP.get(mfl_status)
    if status is None:
        logger.warning("Unknown MFL roster status '%s', defaulting to ROSTER", mfl_status)
        return RosterStatus.ROSTER
    return status


def _parse_signed_season(designation: str, current_season: int) -> int:
    """Extract signed season from designation string, falling back to current season."""
    match = _YEAR_RE.search(designation)
    if match:
        return int(match.group(1))
    return current_season


async def sync_rosters(
    client: MFLClient, session: AsyncSession, season: int
) -> SyncResult:
    """Sync MFL rosters into the RosterEntry and Contract tables.

    Fetches the rosters endpoint, resolves players and teams, and upserts
    RosterEntry and Contract records. Does NOT commit the session — the
    caller controls the transaction boundary.

    Requires Team and Player tables to be already populated (team_sync and
    player_sync must have run first).

    Args:
        client: Authenticated MFLClient instance.
        session: Async SQLAlchemy session (caller manages commit).
        season: The ADL season year to sync rosters for.

    Returns:
        SyncResult with created/updated counts and any errors.
    """
    result = SyncResult()

    raw = await client.rosters()
    rosters_resp = MFLRostersResponse.model_validate(raw)

    # Build lookup: franchise_id -> Team.id
    team_rows = (await session.execute(select(Team))).scalars().all()
    team_lookup: dict[str, int] = {t.franchise_id: t.id for t in team_rows}

    # Build lookups: mfl_id -> Player.id and mfl_id -> Player object
    player_rows = (await session.execute(select(Player))).scalars().all()
    player_lookup: dict[int, int] = {p.mfl_id: p.id for p in player_rows}
    player_lookup_objs: dict[int, Player] = {p.mfl_id: p for p in player_rows}

    logger.info(
        "Roster sync: %d teams, %d players in lookup, %d franchises from MFL",
        len(team_lookup),
        len(player_lookup),
        len(rosters_resp.franchises),
    )

    for franchise in rosters_resp.franchises:
        team_id = team_lookup.get(franchise.id)
        if team_id is None:
            error_msg = f"Unknown franchise_id '{franchise.id}' — skipping"
            logger.error(error_msg)
            result.errors.append(error_msg)
            continue

        for roster_player in franchise.player:
            try:
                mfl_id = int(roster_player.id)
                player_id = player_lookup.get(mfl_id)
                if player_id is None:
                    logger.warning(
                        "Player mfl_id=%d not in player_lookup — skipping", mfl_id
                    )
                    continue

                roster_status = _parse_roster_status(roster_player.status)

                # ── Upsert RosterEntry (player_id, season) ──
                stmt = select(RosterEntry).where(
                    RosterEntry.player_id == player_id,
                    RosterEntry.season == season,
                )
                existing_entry = (await session.execute(stmt)).scalar_one_or_none()

                if existing_entry:
                    existing_entry.team_id = team_id
                    existing_entry.roster_status = roster_status
                    await session.flush()
                else:
                    entry = RosterEntry(
                        team_id=team_id,
                        player_id=player_id,
                        season=season,
                        roster_status=roster_status,
                    )
                    session.add(entry)
                    await session.flush()

                # ── Upsert Contract (team_id, player_id, season) ──
                salary = float(Decimal(roster_player.salary) if roster_player.salary else Decimal(0))
                years_remaining = int(roster_player.contract_year)
                designation = roster_player.contract_info
                signed_season = _parse_signed_season(designation, season)

                # Resolve draft round: prefer Player model, fall back to designation
                player_row = player_lookup_objs.get(mfl_id)
                p_draft_round = player_row.draft_round if player_row else None

                contract_type = classify_contract_type(
                    salary=Decimal(roster_player.salary),
                    designation=designation,
                    signed_season=signed_season,
                    season=season,
                    draft_round=p_draft_round,
                )

                contract_stmt = select(Contract).where(
                    Contract.team_id == team_id,
                    Contract.player_id == player_id,
                    Contract.season == season,
                )
                existing_contract = (
                    await session.execute(contract_stmt)
                ).scalar_one_or_none()

                contract_obj: Contract
                if existing_contract:
                    existing_contract.salary = salary
                    existing_contract.years_remaining = years_remaining
                    existing_contract.contract_type = contract_type
                    existing_contract.designation = designation
                    existing_contract.status = ContractStatus.ACTIVE
                    existing_contract.signed_season = signed_season
                    await session.flush()
                    result.updated += 1
                    contract_obj = existing_contract
                else:
                    contract_obj = Contract(
                        team_id=team_id,
                        player_id=player_id,
                        season=season,
                        salary=salary,
                        years_remaining=years_remaining,
                        contract_type=contract_type,
                        designation=designation,
                        status=ContractStatus.ACTIVE,
                        signed_season=signed_season,
                    )
                    session.add(contract_obj)
                    await session.flush()
                    result.created += 1

                # Link contract to roster entry
                if not existing_entry:
                    # Re-fetch to get the entry we just created
                    re_stmt = select(RosterEntry).where(
                        RosterEntry.player_id == player_id,
                        RosterEntry.season == season,
                    )
                    existing_entry = (
                        await session.execute(re_stmt)
                    ).scalar_one_or_none()

                if existing_entry:
                    existing_entry.contract_id = contract_obj.id
                    await session.flush()

            except Exception as exc:  # noqa: BLE001
                error_msg = (
                    f"Error syncing roster player {roster_player.id} "
                    f"on franchise {franchise.id}: {exc}"
                )
                logger.error(error_msg)
                result.errors.append(error_msg)

    logger.info(
        "Roster sync complete: %d created, %d updated, %d errors",
        result.created,
        result.updated,
        len(result.errors),
    )
    return result


async def detect_contract_gaps(
    session: AsyncSession,
    years: list[int],
) -> list[int]:
    """Detect which seasons have no contract records.

    Args:
        session: Async SQLAlchemy session.
        years: List of season years to check.

    Returns:
        List of years that have zero contract records (need syncing).
    """
    result = await session.execute(
        select(distinct(Contract.season)).where(Contract.season.in_(years))
    )
    existing_seasons = {row[0] for row in result}
    return [y for y in years if y not in existing_seasons]
