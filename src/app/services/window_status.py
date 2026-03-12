"""Window status service — determines whether contract action windows are open.

Maps each contract action to its relevant SeasonCalendar date fields and
checks whether the current date falls within the action's window.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.models.season_calendar import SeasonCalendar

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


# ---------------------------------------------------------------------------
# Action type constants (mirrored from eligibility.py to avoid circular imports)
# ---------------------------------------------------------------------------

ACTION_EXTENSION = "extension"
ACTION_FRANCHISE_TAG = "franchise_tag"
ACTION_ERFA_TENDER = "erfa_tender"
ACTION_RFA_TENDER = "rfa_tender"
ACTION_BUYOUT_RESTRUCTURE = "buyout_restructure"
ACTION_FIFTH_YEAR_OPTION = "fifth_year_option"
ACTION_PROVEN_PERFORMANCE_ESCALATOR = "proven_performance_escalator"

_ALL_ACTIONS = (
    ACTION_EXTENSION,
    ACTION_FRANCHISE_TAG,
    ACTION_ERFA_TENDER,
    ACTION_RFA_TENDER,
    ACTION_BUYOUT_RESTRUCTURE,
    ACTION_FIFTH_YEAR_OPTION,
    ACTION_PROVEN_PERFORMANCE_ESCALATOR,
)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class WindowStatus:
    """Status of a contract action's window for a given season."""

    action: str
    status: str  # "open", "closed", "unconfigured"
    opens: date | None = None
    closes: date | None = None
    reason: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _check_deadline(
    action: str,
    deadline: date | None,
    field_name: str,
    today: date,
) -> WindowStatus:
    """Check a single-deadline action (open if today <= deadline)."""
    if deadline is None:
        return WindowStatus(
            action=action,
            status="unconfigured",
            reason=f"{field_name} has not been set in the season calendar",
        )
    if today <= deadline:
        return WindowStatus(
            action=action,
            status="open",
            closes=deadline,
            reason=f"Window open until {deadline}",
        )
    return WindowStatus(
        action=action,
        status="closed",
        closes=deadline,
        reason=f"Window closed on {deadline}",
    )


def _check_extension_window(
    calendar: SeasonCalendar,
    today: date,
) -> WindowStatus:
    """Extensions have dual windows: oEXT deadline and iEXT start/end."""
    oext = calendar.oext_deadline
    iext_start = calendar.iext_window_start
    iext_end = calendar.iext_window_end

    # Check if oEXT window is open
    oext_open = oext is not None and today <= oext

    # Check if iEXT window is open
    iext_open = (
        iext_start is not None
        and iext_end is not None
        and iext_start <= today <= iext_end
    )

    if oext_open:
        return WindowStatus(
            action=ACTION_EXTENSION,
            status="open",
            closes=oext,
            reason=f"Offseason extension window open until {oext}",
        )

    if iext_open:
        return WindowStatus(
            action=ACTION_EXTENSION,
            status="open",
            opens=iext_start,
            closes=iext_end,
            reason=f"In-season extension window open {iext_start} – {iext_end}",
        )

    # All dates unconfigured
    if oext is None and (iext_start is None or iext_end is None):
        return WindowStatus(
            action=ACTION_EXTENSION,
            status="unconfigured",
            reason=(
                "Extension window dates have not been set in the season calendar "
                "(oext_deadline, iext_window_start/end)"
            ),
        )

    # Both windows exist but are closed — report the most relevant one
    closes = iext_end if iext_end is not None else oext
    return WindowStatus(
        action=ACTION_EXTENSION,
        status="closed",
        closes=closes,
        reason="Both offseason and in-season extension windows are closed",
    )


def _resolve_window(
    action: str,
    calendar: SeasonCalendar,
    today: date,
) -> WindowStatus:
    """Route an action to its calendar field check."""
    if action == ACTION_EXTENSION:
        return _check_extension_window(calendar, today)

    if action == ACTION_FRANCHISE_TAG:
        return _check_deadline(action, calendar.tag_deadline, "tag_deadline", today)

    if action in (ACTION_ERFA_TENDER, ACTION_RFA_TENDER):
        return _check_deadline(
            action, calendar.tender_deadline, "tender_deadline", today,
        )

    if action == ACTION_BUYOUT_RESTRUCTURE:
        return _check_deadline(action, calendar.br_deadline, "br_deadline", today)

    if action == ACTION_FIFTH_YEAR_OPTION:
        return _check_deadline(action, calendar.fyo_deadline, "fyo_deadline", today)

    if action == ACTION_PROVEN_PERFORMANCE_ESCALATOR:
        # PPE is automatic/performance-based — always open
        return WindowStatus(
            action=action,
            status="open",
            reason="PPE is automatic based on performance — no deadline window",
        )

    return WindowStatus(
        action=action,
        status="unconfigured",
        reason=f"Unknown action type: {action}",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def get_window_status(
    session: AsyncSession,
    season: int,
    action: str,
    *,
    _today: date | None = None,
) -> WindowStatus:
    """Determine window status for a single contract action.

    Parameters
    ----------
    session : AsyncSession
        Database session.
    season : int
        ADL season year.
    action : str
        Contract action type (e.g. "extension", "franchise_tag").
    _today : date | None
        Override for current date (testing only). Defaults to date.today().
    """
    today = _today or date.today()

    result = await session.execute(
        select(SeasonCalendar).where(SeasonCalendar.season == season)
    )
    calendar = result.scalar_one_or_none()

    if calendar is None:
        return WindowStatus(
            action=action,
            status="unconfigured",
            reason=f"No season calendar configured for {season}",
        )

    return _resolve_window(action, calendar, today)


async def get_all_window_statuses(
    session: AsyncSession,
    season: int,
    *,
    _today: date | None = None,
) -> dict[str, WindowStatus]:
    """Get window status for all 7 contract action types.

    Loads the calendar once and checks each action.

    Parameters
    ----------
    session : AsyncSession
        Database session.
    season : int
        ADL season year.
    _today : date | None
        Override for current date (testing only). Defaults to date.today().
    """
    today = _today or date.today()

    result = await session.execute(
        select(SeasonCalendar).where(SeasonCalendar.season == season)
    )
    calendar = result.scalar_one_or_none()

    if calendar is None:
        return {
            action: WindowStatus(
                action=action,
                status="unconfigured",
                reason=f"No season calendar configured for {season}",
            )
            for action in _ALL_ACTIONS
        }

    return {
        action: _resolve_window(action, calendar, today)
        for action in _ALL_ACTIONS
    }
