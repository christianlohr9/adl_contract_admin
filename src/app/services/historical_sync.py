"""Historical backfill orchestrator — detects missing data and syncs in background."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.core.db import async_session
from app.mfl.client import MFLClient
from app.services.roster_sync import detect_contract_gaps, sync_rosters
from app.services.score_sync import detect_score_gaps, sync_historical_scores

if TYPE_CHECKING:
    from app.core.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class BackfillStatus:
    """In-memory tracking of historical backfill completeness."""

    in_progress: bool = False
    scores_complete: bool = False
    contracts_complete: bool = False
    missing_score_years: list[int] = field(default_factory=list)
    missing_contract_years: list[int] = field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


_backfill_status = BackfillStatus()


def get_backfill_status() -> BackfillStatus:
    """Return the current backfill status."""
    return _backfill_status


async def run_historical_backfill(settings: Settings) -> None:
    """Orchestrate full historical backfill: scores then contracts.

    Detects gaps first, then fetches only missing data. Updates
    BackfillStatus throughout so callers can check completeness.

    This function is designed to be called via asyncio.create_task()
    so it never blocks the application startup.

    Args:
        settings: Application settings with MFL and sync configuration.
    """
    _backfill_status.in_progress = True
    _backfill_status.started_at = datetime.now(UTC)
    _backfill_status.error = None

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
            # ── Detect gaps ──
            score_gaps = await detect_score_gaps(
                session, settings.sync_historical_years
            )
            contract_gaps = await detect_contract_gaps(
                session, settings.sync_historical_years
            )

            _backfill_status.missing_score_years = sorted(score_gaps.keys())
            _backfill_status.missing_contract_years = sorted(contract_gaps)

            if not score_gaps and not contract_gaps:
                logger.info(
                    "Historical backfill: no gaps detected for years %s",
                    settings.sync_historical_years,
                )
                _backfill_status.scores_complete = True
                _backfill_status.contracts_complete = True
                _backfill_status.in_progress = False
                _backfill_status.completed_at = datetime.now(UTC)
                return

            # ── Sync missing scores ──
            if score_gaps:
                logger.info(
                    "Historical backfill: syncing scores for %d seasons with gaps",
                    len(score_gaps),
                )
                years_with_gaps = sorted(score_gaps.keys())
                await sync_historical_scores(
                    client_factory=client_factory,
                    session=session,
                    years=years_with_gaps,
                )
                await session.commit()
                logger.info("Historical backfill: score sync committed")

            _backfill_status.scores_complete = True

            # ── Sync missing contracts ──
            if contract_gaps:
                logger.info(
                    "Historical backfill: syncing contracts for %d seasons: %s",
                    len(contract_gaps),
                    contract_gaps,
                )
                for year in contract_gaps:
                    logger.info(
                        "Historical backfill: syncing rosters for season %d", year
                    )
                    async with client_factory(year) as client:
                        await sync_rosters(client, session, season=year)
                        await asyncio.sleep(client._request_delay)  # noqa: SLF001
                await session.commit()
                logger.info("Historical backfill: contract sync committed")

            _backfill_status.contracts_complete = True
            _backfill_status.completed_at = datetime.now(UTC)
            logger.info("Historical backfill completed successfully")

    except Exception as exc:
        error_msg = f"Historical backfill failed: {exc}"
        _backfill_status.error = error_msg
        logger.exception(error_msg)

    finally:
        _backfill_status.in_progress = False
