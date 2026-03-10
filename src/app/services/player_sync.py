"""Player sync service — batch upserts MFL players into the Player table."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.mfl.models import MFLPlayersResponse
from app.models.player import Player
from app.services.team_sync import SyncResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from app.mfl.client import MFLClient
    from app.mfl.models import MFLPlayer

logger = logging.getLogger(__name__)

_BATCH_SIZE = 500


def _parse_birthdate(value: str | None) -> datetime | None:
    """Convert MFL epoch-seconds string to a date, or None."""
    if not value or value == "0":
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=UTC)
    except (ValueError, OSError):
        return None


def _parse_optional_int(value: str | None) -> int | None:
    """Parse an optional string to int, or None if empty/missing."""
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _player_fields(player: MFLPlayer) -> dict[str, object]:
    """Extract DB-ready field values from an MFL player record."""
    birthdate_dt = _parse_birthdate(player.birthdate)
    return {
        "mfl_id": int(player.id),
        "name": player.name,
        "position": player.position,
        "nfl_team": player.team if player.team else None,
        "birthdate": birthdate_dt.date() if birthdate_dt else None,
        "draft_year": _parse_optional_int(player.draft_year),
        "draft_round": _parse_optional_int(player.draft_round),
    }


async def sync_players(client: MFLClient, session: AsyncSession) -> SyncResult:
    """Sync MFL players into the Player table.

    Fetches all players from MFL, builds a lookup of existing records,
    and batch-upserts new or changed players. Does NOT commit the session —
    the caller controls the transaction boundary.

    Args:
        client: Authenticated MFLClient instance.
        session: Async SQLAlchemy session (caller manages commit).

    Returns:
        SyncResult with created/updated counts and any errors.
    """
    result = SyncResult()

    raw = await client.players()
    players_resp = MFLPlayersResponse.model_validate(raw)

    logger.info("Fetched %d players from MFL", len(players_resp.players))

    # Build lookup of existing players: mfl_id -> Player
    stmt = select(Player)
    existing_rows = (await session.execute(stmt)).scalars().all()
    lookup: dict[int, Player] = {p.mfl_id: p for p in existing_rows}

    new_players: list[Player] = []

    for mfl_player in players_resp.players:
        try:
            fields = _player_fields(mfl_player)
            mfl_id = fields["mfl_id"]
            assert isinstance(mfl_id, int)  # noqa: S101

            if mfl_id in lookup:
                existing = lookup[mfl_id]
                changed = False
                for attr, value in fields.items():
                    if attr == "mfl_id":
                        continue
                    if getattr(existing, attr) != value:
                        setattr(existing, attr, value)
                        changed = True
                if changed:
                    result.updated += 1
            else:
                new_players.append(Player(**fields))
                result.created += 1

                # Flush in batches
                if len(new_players) >= _BATCH_SIZE:
                    session.add_all(new_players)
                    await session.flush()
                    new_players = []

        except Exception as exc:  # noqa: BLE001
            error_msg = f"Error syncing player {mfl_player.id}: {exc}"
            logger.error(error_msg)
            result.errors.append(error_msg)

    # Flush remaining new players
    if new_players:
        session.add_all(new_players)
        await session.flush()

    logger.info(
        "Synced %d players: %d new, %d updated, %d errors",
        result.created + result.updated,
        result.created,
        result.updated,
        len(result.errors),
    )
    return result
