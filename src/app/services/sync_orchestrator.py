"""Sync orchestrator — coordinates MFL sync services in correct order."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from app.core.db import async_session
from app.mfl.client import MFLClient
from app.services.player_sync import sync_players
from app.services.roster_sync import sync_rosters
from app.services.score_sync import sync_historical_scores, sync_scores
from app.services.team_sync import SyncResult, sync_teams

if TYPE_CHECKING:
    from app.core.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class SyncStatus:
    """In-memory tracking of the most recent sync state."""

    last_sync: datetime | None = None
    in_progress: bool = False
    last_result: dict[str, Any] | None = None
    last_error: str | None = None


_sync_status = SyncStatus()


def get_sync_status() -> SyncStatus:
    """Return the current sync status."""
    return _sync_status


def _result_to_dict(result: SyncResult) -> dict[str, Any]:
    """Convert a SyncResult dataclass to a serialisable dict."""
    return {
        "created": result.created,
        "updated": result.updated,
        "errors": list(result.errors),
    }


async def run_full_sync(settings: Settings) -> dict[str, Any]:
    """Execute a full MFL sync in the correct order.

    Order: teams -> players -> rosters -> scores (current season YTD).
    All operations share a single session/transaction for atomicity.

    Args:
        settings: Application settings with MFL and sync configuration.

    Returns:
        Dict mapping service name to result dict.
    """
    _sync_status.in_progress = True
    _sync_status.last_error = None
    results: dict[str, Any] = {}

    try:
        async with MFLClient.from_settings(settings) as client, async_session() as session:
                teams_result = await sync_teams(client, session)
                results["teams"] = _result_to_dict(teams_result)

                players_result = await sync_players(client, session)
                results["players"] = _result_to_dict(players_result)

                rosters_result = await sync_rosters(client, session, season=settings.mfl_year)
                results["rosters"] = _result_to_dict(rosters_result)

                scores_result = await sync_scores(
                    client, session, season=settings.mfl_year
                )
                results["scores"] = _result_to_dict(scores_result)

                await session.commit()

        _sync_status.last_sync = datetime.now(UTC)
        _sync_status.last_result = results
        logger.info("Full sync completed: %s", results)

    except Exception as exc:
        error_msg = f"Full sync failed: {exc}"
        _sync_status.last_error = error_msg
        logger.exception(error_msg)
        raise

    finally:
        _sync_status.in_progress = False

    return results


async def run_historical_sync(settings: Settings) -> dict[int, Any]:
    """Sync YTD scores for historical seasons.

    Creates a year-scoped MFLClient for each year and syncs scores.
    Does NOT sync teams/players/rosters for past years.

    Args:
        settings: Application settings with sync_historical_years list.

    Returns:
        Dict mapping year to result dict.
    """
    _sync_status.in_progress = True
    _sync_status.last_error = None
    results: dict[int, Any] = {}

    def client_factory(year: int) -> MFLClient:
        return MFLClient(
            year=year,
            league_id=settings.mfl_league_id,
            base_url=settings.mfl_base_url,
            api_key=settings.mfl_api_key,
            username=settings.mfl_username,
            password=settings.mfl_password,
            request_delay=settings.mfl_request_delay,
        )

    try:
        async with async_session() as session:
            year_results = await sync_historical_scores(
                client_factory=client_factory,
                session=session,
                years=settings.sync_historical_years,
            )
            for year, sync_result in year_results.items():
                results[year] = _result_to_dict(sync_result)

            await session.commit()

        _sync_status.last_sync = datetime.now(UTC)
        _sync_status.last_result = {str(k): v for k, v in results.items()}
        logger.info("Historical sync completed: %s", results)

    except Exception as exc:
        error_msg = f"Historical sync failed: {exc}"
        _sync_status.last_error = error_msg
        logger.exception(error_msg)
        raise

    finally:
        _sync_status.in_progress = False

    return results
