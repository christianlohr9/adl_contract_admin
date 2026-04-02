"""Sync player-season data from MFL weekly roster snapshots.

Scans weeks 1-17 for each season and records every (player, team, season)
combination found, with weeks_rostered count. This captures players who were
rostered mid-season but dropped before the end-of-season snapshot.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
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

    Two-pass approach:
    1. Scan all 17 weeks and count appearances per (player, team).
    2. Upsert PlayerSeason records with weeks_rostered.
    """
    result = SyncResult()

    # Build lookups
    team_rows = (await session.execute(select(Team))).scalars().all()
    team_lookup: dict[str, int] = {t.franchise_id: t.id for t in team_rows}

    player_rows = (await session.execute(select(Player))).scalars().all()
    player_lookup: dict[int, int] = {p.mfl_id: p.id for p in player_rows}

    # Pass 1: count weeks per (player_id, team_id)
    week_counts: Counter[tuple[int, int]] = Counter()

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

                week_counts[(player_id, team_id)] += 1

        await asyncio.sleep(client._request_delay)  # noqa: SLF001

    # Pass 2: upsert PlayerSeason records with weeks_rostered
    existing_result = await session.execute(
        select(PlayerSeason)
        .where(PlayerSeason.season == season)
    )
    existing: dict[tuple[int, int], PlayerSeason] = {
        (ps.player_id, ps.team_id): ps
        for ps in existing_result.scalars().all()
    }

    for (player_id, team_id), weeks in week_counts.items():
        key = (player_id, team_id)
        if key in existing:
            if existing[key].weeks_rostered != weeks:
                existing[key].weeks_rostered = weeks
                result.updated += 1
        else:
            session.add(PlayerSeason(
                player_id=player_id,
                team_id=team_id,
                season=season,
                weeks_rostered=weeks,
            ))
            result.created += 1

    await session.flush()

    logger.info(
        "Player-season sync for %d: %d created, %d updated, %d errors",
        season, result.created, result.updated, len(result.errors),
    )
    return result
