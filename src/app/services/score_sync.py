"""Player scores sync service — upserts MFL scoring data into the PlayerScore table."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import distinct, select

from app.mfl.models import MFLPlayerScoresResponse
from app.models.player import Player
from app.models.player_score import PlayerScore
from app.services.team_sync import SyncResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from app.mfl.client import MFLClient

logger = logging.getLogger(__name__)

_BATCH_SIZE = 500


async def sync_scores(
    client: MFLClient,
    session: AsyncSession,
    season: int,
    week: str = "YTD",
) -> SyncResult:
    """Sync MFL player scores into the PlayerScore table.

    Fetches the playerScores endpoint and upserts records using the
    (player_id, season, week) unique constraint. Does NOT commit the
    session — the caller controls the transaction boundary.

    Args:
        client: Authenticated MFLClient instance.
        session: Async SQLAlchemy session (caller manages commit).
        season: The ADL season year.
        week: Week identifier — "YTD" for season totals, "1"-"17" for weekly.

    Returns:
        SyncResult with created/updated counts and any errors.
    """
    result = SyncResult()

    raw = await client.player_scores(week=week, year=season)
    scores_resp = MFLPlayerScoresResponse.model_validate(raw)

    logger.info(
        "Fetched %d player scores from MFL (season=%d, week=%s)",
        len(scores_resp.scores),
        season,
        week,
    )

    # Build lookup: mfl_id -> Player.id
    player_rows = (await session.execute(select(Player))).scalars().all()
    player_lookup: dict[int, int] = {p.mfl_id: p.id for p in player_rows}

    new_scores: list[PlayerScore] = []

    for mfl_score in scores_resp.scores:
        try:
            mfl_id = int(mfl_score.id)
            player_id = player_lookup.get(mfl_id)
            if player_id is None:
                # Not all MFL players are in our system — skip silently
                continue

            points = float(mfl_score.score)

            # Check for existing record
            stmt = select(PlayerScore).where(
                PlayerScore.player_id == player_id,
                PlayerScore.season == season,
                PlayerScore.week == week,
            )
            existing = (await session.execute(stmt)).scalar_one_or_none()

            if existing:
                if existing.points != points:
                    existing.points = points
                    result.updated += 1
            else:
                new_scores.append(
                    PlayerScore(
                        player_id=player_id,
                        season=season,
                        week=week,
                        points=points,
                    )
                )
                result.created += 1

                # Flush in batches
                if len(new_scores) >= _BATCH_SIZE:
                    session.add_all(new_scores)
                    await session.flush()
                    new_scores = []

        except Exception as exc:  # noqa: BLE001
            error_msg = f"Error syncing score for player {mfl_score.id}: {exc}"
            logger.error(error_msg)
            result.errors.append(error_msg)

    # Flush remaining new scores
    if new_scores:
        session.add_all(new_scores)
        await session.flush()

    logger.info(
        "Score sync complete (season=%d, week=%s): %d created, %d updated, %d errors",
        season,
        week,
        result.created,
        result.updated,
        len(result.errors),
    )
    return result


# All expected weeks per season: YTD + weeks 1-17
_ALL_WEEKS: set[str] = {"YTD"} | {str(w) for w in range(1, 18)}


async def detect_score_gaps(
    session: AsyncSession,
    years: list[int],
) -> dict[int, list[str]]:
    """Detect missing (season, week) combos in the PlayerScore table.

    For each year, checks which of the 18 expected weeks (YTD + 1-17)
    have at least one PlayerScore record. Returns only years that have
    missing weeks.

    Args:
        session: Async SQLAlchemy session.
        years: List of season years to check.

    Returns:
        Dict mapping year to sorted list of missing week strings.
        Years with no gaps are omitted from the result.
    """
    gaps: dict[int, list[str]] = {}
    for year in years:
        result = await session.execute(
            select(distinct(PlayerScore.week)).where(PlayerScore.season == year)
        )
        existing_weeks = {row[0] for row in result}
        missing = _ALL_WEEKS - existing_weeks
        if missing:
            # Sort numerically: YTD first, then 1-17
            gaps[year] = sorted(missing, key=lambda w: (w != "YTD", int(w) if w != "YTD" else 0))
    return gaps


async def sync_historical_scores(
    client_factory: Callable[[int], Any],
    session: AsyncSession,
    years: list[int],
) -> dict[int, SyncResult]:
    """Sync weekly scores (1-17) and YTD for multiple historical seasons.

    Uses gap detection to skip weeks that already exist in the database.
    Creates a new MFLClient instance per year (MFL API URLs are year-scoped)
    and syncs each missing week sequentially with rate limiting.

    Args:
        client_factory: Callable that accepts a year (int) and returns an
            MFLClient instance (as an async context manager).
        session: Async SQLAlchemy session (caller manages commit).
        years: List of season years to sync.

    Returns:
        Dict mapping year to its aggregated SyncResult.
    """
    # Detect which weeks are missing before making any API calls
    gaps = await detect_score_gaps(session, years)

    if not gaps:
        logger.info("Historical score sync: no gaps detected for years %s", years)
        return {}

    logger.info(
        "Historical score sync: detected gaps in %d seasons: %s",
        len(gaps),
        {y: len(weeks) for y, weeks in gaps.items()},
    )

    results: dict[int, SyncResult] = {}

    for year in years:
        missing_weeks = gaps.get(year)
        if not missing_weeks:
            continue

        logger.info(
            "Syncing %d missing weeks for season %d: %s",
            len(missing_weeks),
            year,
            missing_weeks,
        )

        year_result = SyncResult()
        async with client_factory(year) as client:
            for week in missing_weeks:
                week_result = await sync_scores(
                    client, session, season=year, week=week
                )
                year_result.created += week_result.created
                year_result.updated += week_result.updated
                year_result.errors.extend(week_result.errors)
                # Respect rate limits between requests
                await asyncio.sleep(client._request_delay)  # noqa: SLF001

        results[year] = year_result

    logger.info(
        "Historical score sync complete for %d seasons: %s",
        len(results),
        {y: f"{r.created}c/{r.updated}u/{len(r.errors)}e" for y, r in results.items()},
    )
    return results
