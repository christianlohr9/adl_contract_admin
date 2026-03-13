"""Tests for NFL kickoff eligibility gate on rookie/UDFA extensions."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.extensions import check_extension_eligibility


def _make_contract(**overrides):
    """Create a mock Contract with sensible defaults."""
    defaults = {
        "id": 1,
        "player_id": 100,
        "season": 2025,
        "status": "active",
        "salary": 1.0,
        "years_remaining": 1,
        "signed_season": 2022,
        "designation": "2022 1.05",  # drafted rookie by default
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_calendar(**overrides):
    """Create a mock SeasonCalendar with sensible defaults."""
    defaults = {
        "season": 2025,
        "regular_season_start": date(2025, 9, 4),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _mock_result(value):
    """Create a mock result with scalar_one_or_none returning value."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def _build_session(contract, calendar=None, ext_check=None, robust_seasons=None):
    """Build an AsyncMock session that returns the right objects for each query.

    Query order in check_extension_eligibility:
    1. Contract query
    2. SeasonCalendar query (only if rookie/UDFA and final year)
    3. EXT cooldown check
    4. Robust PR checks (up to 3 seasons)
    """
    session = AsyncMock()
    side_effects = [_mock_result(contract)]

    if contract is not None:
        desig = contract.designation or ""
        is_rookie = any(
            f"{contract.signed_season} {pick}" in desig
            for pick in [f"{r}." for r in range(1, 6)]
        )
        is_udfa = "UDFA" in desig

        if is_rookie or is_udfa:
            # Will reach kickoff check if contract passes max-years check
            if contract.years_remaining <= 1:
                side_effects.append(_mock_result(calendar))
                # If calendar blocks, we won't reach further checks
                if calendar is not None and calendar.regular_season_start is not None:
                    from datetime import date as real_date
                    # Only add further mocks if kickoff check passes
                    # (determined by the patched date)
                    # We add them unconditionally; if kickoff blocks, they won't be called
                    if ext_check is not None:
                        side_effects.append(_mock_result(ext_check))
                    else:
                        side_effects.append(_mock_result(None))  # no EXT found

    session.execute = AsyncMock(side_effect=side_effects)
    return session


class TestRookieFinalYearBlockedBeforeKickoff:
    """Rookie contract in final year should be blocked before NFL kickoff."""

    async def test_rookie_final_year_blocked_before_kickoff(self):
        contract = _make_contract(
            designation="2022 1.05",
            signed_season=2022,
            years_remaining=1,
        )
        calendar = _make_calendar(regular_season_start=date(2025, 9, 4))
        session = _build_session(contract, calendar)

        with patch("app.services.extensions.date") as mock_date:
            mock_date.today.return_value = date(2025, 7, 15)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            eligible, reason = await check_extension_eligibility(session, 100, 2025)

        assert eligible is False
        assert "NFL kickoff" in reason
        assert "Sep 04, 2025" in reason


class TestRookieFinalYearAllowedAfterKickoff:
    """Rookie contract in final year should pass kickoff check after season starts."""

    async def test_rookie_final_year_allowed_after_kickoff(self):
        contract = _make_contract(
            designation="2022 1.05",
            signed_season=2022,
            years_remaining=1,
        )
        calendar = _make_calendar(regular_season_start=date(2025, 9, 4))
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=[
            _mock_result(contract),       # Contract query
            _mock_result(calendar),       # Calendar query
            _mock_result(None),           # EXT cooldown check (none found)
        ])

        with patch("app.services.extensions.date") as mock_date:
            mock_date.today.return_value = date(2025, 9, 10)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

            with patch("app.services.extensions.is_robust_season", new_callable=AsyncMock) as mock_robust:
                mock_robust.return_value = True
                eligible, reason = await check_extension_eligibility(session, 100, 2025)

        # Passes kickoff check; may fail on subsequent checks but should NOT
        # fail with a kickoff-related reason
        if not eligible:
            assert "kickoff" not in reason.lower()
            assert "NFL season start" not in reason


class TestUDFAFinalYearBlockedBeforeKickoff:
    """UDFA contract in final year should be blocked before NFL kickoff."""

    async def test_udfa_final_year_blocked_before_kickoff(self):
        contract = _make_contract(
            designation="2023 UDFA",
            signed_season=2023,
            years_remaining=1,
        )
        calendar = _make_calendar(regular_season_start=date(2025, 9, 4))
        session = _build_session(contract, calendar)

        with patch("app.services.extensions.date") as mock_date:
            mock_date.today.return_value = date(2025, 8, 1)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            eligible, reason = await check_extension_eligibility(session, 100, 2025)

        assert eligible is False
        assert "NFL kickoff" in reason


class TestNonRookieUnaffected:
    """Standard (non-rookie/UDFA) contract should not be affected by kickoff rule."""

    async def test_non_rookie_unaffected(self):
        contract = _make_contract(
            designation="SD",
            signed_season=2023,
            years_remaining=1,
        )
        # No calendar needed since it's not a rookie/UDFA contract
        session = AsyncMock()
        # Query order for non-rookie: contract, EXT check, robust PR checks
        session.execute = AsyncMock(side_effect=[
            _mock_result(contract),       # Contract query
            _mock_result(None),           # EXT cooldown (none found)
            _mock_result(None),           # robust PR season checks - handled differently
        ])

        with patch("app.services.extensions.date") as mock_date:
            mock_date.today.return_value = date(2025, 7, 15)  # before kickoff
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)

            with patch("app.services.extensions.is_robust_season", new_callable=AsyncMock) as mock_robust:
                mock_robust.return_value = True
                eligible, reason = await check_extension_eligibility(session, 100, 2025)

        # Should NOT fail with a kickoff reason
        if not eligible:
            assert "kickoff" not in reason.lower()
            assert "NFL season start" not in reason


class TestRookieMultiYearUnaffected:
    """Rookie contract with 2+ years remaining should be blocked by years rule, not kickoff."""

    async def test_rookie_multi_year_unaffected(self):
        contract = _make_contract(
            designation="2022 1.05",
            signed_season=2022,
            years_remaining=2,
        )
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=[
            _mock_result(contract),
        ])

        eligible, reason = await check_extension_eligibility(session, 100, 2025)

        assert eligible is False
        assert "2+ years remaining" in reason
        # Should NOT mention kickoff
        assert "kickoff" not in reason.lower()


class TestNullCalendarBlocksConservatively:
    """Missing calendar data should block conservatively with clear message."""

    async def test_null_calendar_blocks_conservatively(self):
        contract = _make_contract(
            designation="2022 1.05",
            signed_season=2022,
            years_remaining=1,
        )
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=[
            _mock_result(contract),       # Contract query
            _mock_result(None),           # Calendar query returns None
        ])

        eligible, reason = await check_extension_eligibility(session, 100, 2025)

        assert eligible is False
        assert "not yet configured" in reason
