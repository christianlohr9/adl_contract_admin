"""Team sync service — upserts MFL franchises into the Team table."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.mfl.models import MFLLeagueResponse
from app.models.team import Team

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.mfl.client import MFLClient

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    """Result of a sync operation."""

    created: int = 0
    updated: int = 0
    errors: list[str] = field(default_factory=list)


async def sync_teams(client: MFLClient, session: AsyncSession) -> SyncResult:
    """Sync MFL franchises into the Team table.

    Fetches the league endpoint, parses franchises, and upserts each one
    using ``franchise_id`` as the match key. Does NOT commit the session —
    the caller controls the transaction boundary.

    Args:
        client: Authenticated MFLClient instance.
        session: Async SQLAlchemy session (caller manages commit).

    Returns:
        SyncResult with created/updated counts and any errors.
    """
    result = SyncResult()

    raw = await client.league()
    league_resp = MFLLeagueResponse.model_validate(raw)

    for franchise in league_resp.franchises:
        try:
            stmt = select(Team).where(Team.franchise_id == franchise.id)
            existing = (await session.execute(stmt)).scalar_one_or_none()

            conference = franchise.conference if franchise.conference else ""
            division = franchise.division if franchise.division else ""

            if not franchise.conference:
                logger.warning(
                    "Franchise %s (%s): conference missing, defaulting to empty string",
                    franchise.id,
                    franchise.name,
                )
            if not franchise.division:
                logger.warning(
                    "Franchise %s (%s): division missing, defaulting to empty string",
                    franchise.id,
                    franchise.name,
                )

            if existing:
                existing.name = franchise.name
                existing.conference = conference
                existing.division = division
                await session.flush()
                result.updated += 1
                logger.info("Updated team %s: %s", franchise.id, franchise.name)
            else:
                team = Team(
                    franchise_id=franchise.id,
                    name=franchise.name,
                    conference=conference,
                    division=division,
                )
                session.add(team)
                await session.flush()
                result.created += 1
                logger.info("Created team %s: %s", franchise.id, franchise.name)

        except Exception as exc:  # noqa: BLE001
            error_msg = f"Error syncing franchise {franchise.id}: {exc}"
            logger.error(error_msg)
            result.errors.append(error_msg)

    logger.info(
        "Team sync complete: %d created, %d updated, %d errors",
        result.created,
        result.updated,
        len(result.errors),
    )
    return result
