"""Sync player-season data from MFL weekly roster snapshots.

Scans weeks 1-17 for each season and records every (player, team, season)
combination found. This captures players who were rostered mid-season but
dropped before the end-of-season snapshot.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.mfl.models import MFLRostersResponse
from app.models.player import Player
from app.models.player_season import PlayerSeason
from app.models.team import Team
from app.services.team_sync import SyncResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.mfl.client import MFLClient

logger = logging.getLogger(__name__)


async def sync_player_seasons(
    client: MFLClient,
    session: AsyncSession,
    season: int,
) -> SyncResult:
    """Scan all 17 weekly rosters and upsert PlayerSeason records.

    For each week, fetches rosters and records every (player, team, season)
    tuple. Duplicates are skipped via the unique constraint.
    """
    result = SyncResult()

    # Build lookups
    team_rows = (await session.execute(select(Team))).scalars().all()
    team_lookup: dict[str, int] = {t.franchise_id: t.id for t in team_rows}

    player_rows = (await session.execute(select(Player))).scalars().all()
    player_lookup: dict[int, int] = {p.mfl_id: p.id for p in player_rows}

    # Load existing player_seasons for this season to avoid redundant inserts
    existing = await session.execute(
        select(PlayerSeason.player_id, PlayerSeason.team_id)
        .where(PlayerSeason.season == season)
    )
    seen: set[tuple[int, int]] = {(r[0], r[1]) for r in existing}

    for week in range(1, 18):
        try:
            raw = await client.weekly_rosters(week)
        except Exception:
            logger.exception("Failed to fetch week %d rosters for %d", week, season)
            result.errors.append(f"Week {week} fetch failed")
            continue

        rosters_resp = MFLRostersResponse.model_validate(raw)

        for franchise in rosters_resp.franchises:
            team_id = team_lookup.get(franchise.id)
            if team_id is None:
                continue

            for roster_player in franchise.player:
                try:
                    mfl_id = int(roster_player.id)
                except (ValueError, TypeError):
                    continue

                player_id = player_lookup.get(mfl_id)
                if player_id is None:
                    continue

                key = (player_id, team_id)
                if key in seen:
                    continue

                session.add(PlayerSeason(
                    player_id=player_id,
                    team_id=team_id,
                    season=season,
                ))
                seen.add(key)
                result.created += 1

        await session.flush()
        await asyncio.sleep(client._request_delay)  # noqa: SLF001

    logger.info(
        "Player-season sync for %d: %d new records, %d errors",
        season, result.created, len(result.errors),
    )
    return result
