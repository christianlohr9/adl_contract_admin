"""Unified contract action eligibility validation service.

Single entry point for checking whether a player qualifies for any contract action.
Returns clear yes/no answers with bylaws rule citations.

Delegates to existing action-specific eligibility checkers where available,
and checks allotment limits via the allotments service.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import select

from app.models.contract import Contract
from app.models.player import Player
from app.services.buyouts import check_buyout_eligibility
from app.services.extensions import check_extension_eligibility
from app.services.franchise_tags import check_tag_eligibility, get_consecutive_tag_count
from app.services.rules import get_contract_constants
from app.services.tenders import check_erfa_eligibility, check_rfa_eligibility

if TYPE_CHECKING:
    from datetime import date

    from sqlalchemy.ext.asyncio import AsyncSession

# Result dataclass


@dataclass


class EligibilityResult:
    """Result of an eligibility check for a specific contract action."""

    action: str
    eligible: bool
    reason: str | None = None
    rule_citation: str | None = None
    prerequisites: list[str] = field(default_factory=list)
    window_status: str | None = None
    window_closes: date | None = None

# Action type constants

ACTION_EXTENSION = "extension"
ACTION_FRANCHISE_TAG = "franchise_tag"
ACTION_ERFA_TENDER = "erfa_tender"
ACTION_RFA_TENDER = "rfa_tender"
ACTION_BUYOUT_RESTRUCTURE = "buyout_restructure"
ACTION_FIFTH_YEAR_OPTION = "fifth_year_option"
ACTION_PROVEN_PERFORMANCE_ESCALATOR = "proven_performance_escalator"

_VALID_ACTIONS = frozenset({
    ACTION_EXTENSION,
    ACTION_FRANCHISE_TAG,
    ACTION_ERFA_TENDER,
    ACTION_RFA_TENDER,
    ACTION_BUYOUT_RESTRUCTURE,
    ACTION_FIFTH_YEAR_OPTION,
    ACTION_PROVEN_PERFORMANCE_ESCALATOR,
})

# Helpers


async def _get_team_id_for_player(
    session: AsyncSession,
    player_id: int,
    season: int,
    *,
    check_previous_season: bool = False,
) -> int | None:
    """Resolve the team_id that owns this player's contract.

    For actions on expired contracts (tags, tenders), set
    ``check_previous_season=True`` to look at season - 1.
    For actions on active contracts (extension, B/R, 5YO, PPE), uses current season.
    """
    target_season = season - 1 if check_previous_season else season
    result = await session.execute(
        select(Contract.team_id)
        .where(
            Contract.player_id == player_id,
            Contract.season == target_season,
        )
        .order_by(Contract.salary.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()

# Main entry point


async def check_eligibility(
    session: AsyncSession,
    player_id: int,
    season: int,
    action: str,
    team_id: int | None = None,
) -> EligibilityResult:
    """Check whether a player is eligible for a specific contract action.

    Dispatches to action-specific checkers. Each checker loads the player's
    active contract and validates against bylaws rules.

    When ``team_id`` is provided, contract lookups are scoped to that team.
    This is important in a two-conference league where a player has separate
    contracts in each conference.

    Parameters
    ----------
    session : AsyncSession
        Database session.
    player_id : int
        The player to check.
    season : int
        The ADL season year.
    action : str
        One of: extension, franchise_tag, erfa_tender, rfa_tender,
        buyout_restructure, fifth_year_option, proven_performance_escalator.
    team_id : int | None
        Optional team to scope contract lookups to.

    Returns
    -------
    EligibilityResult
        Contains eligible flag, reason (if ineligible), rule citation, and prerequisites.
    """
    if action not in _VALID_ACTIONS:
        return EligibilityResult(
            action=action,
            eligible=False,
            reason=f"Unknown action type: {action}. Valid actions: {sorted(_VALID_ACTIONS)}",
        )

    # Window gating — check if the action's calendar window is open
    from app.services.window_status import get_window_status

    window = await get_window_status(session, season, action)

    if window.status == "unconfigured":
        return EligibilityResult(
            action=action,
            eligible=False,
            reason=window.reason,
            rule_citation=(
                "No season calendar configured — commissioner must set "
                "dates before contract tools are available"
            ),
            window_status=window.status,
            window_closes=window.closes,
        )

    if window.status == "closed":
        return EligibilityResult(
            action=action,
            eligible=False,
            reason=window.reason,
            rule_citation="Contract action window has closed for this season",
            window_status=window.status,
            window_closes=window.closes,
        )

    # Window is open — proceed to player-level eligibility checks
    dispatch = {
        ACTION_EXTENSION: _check_extension,
        ACTION_FRANCHISE_TAG: _check_franchise_tag,
        ACTION_ERFA_TENDER: _check_erfa_tender,
        ACTION_RFA_TENDER: _check_rfa_tender,
        ACTION_BUYOUT_RESTRUCTURE: _check_buyout_restructure,
        ACTION_FIFTH_YEAR_OPTION: _check_fifth_year_option,
        ACTION_PROVEN_PERFORMANCE_ESCALATOR: _check_proven_performance_escalator,
    }

    checker = dispatch[action]
    # Conference-scoped checkers need team_id
    if action in (ACTION_FRANCHISE_TAG, ACTION_EXTENSION, ACTION_ERFA_TENDER, ACTION_RFA_TENDER):
        result = await checker(session, player_id, season, team_id)
    else:
        result = await checker(session, player_id, season)
    result.window_status = window.status
    result.window_closes = window.closes
    return result

# Extension eligibility


async def _check_extension(
    session: AsyncSession,
    player_id: int,
    season: int,
    team_id: int | None = None,
) -> EligibilityResult:
    """Delegate to existing extension eligibility checker."""
    eligible, reason = await check_extension_eligibility(session, player_id, season, team_id)

    if eligible:
        return EligibilityResult(
            action=ACTION_EXTENSION,
            eligible=True,
            rule_citation=(
                "Extensions: Players in their final contract year "
                "with a Robust PR are eligible"
            ),
            prerequisites=["Must declare via Message Board during extension window"],
        )

    return EligibilityResult(
        action=ACTION_EXTENSION,
        eligible=False,
        reason=reason,
        rule_citation=(
            "Extensions: Players must be in final contract year, "
            "on max-length rookie deals, with a Robust PR in recent seasons"
        ),
    )

# Franchise tag eligibility


async def _check_franchise_tag(
    session: AsyncSession,
    player_id: int,
    season: int,
    team_id: int | None = None,
) -> EligibilityResult:
    """Check franchise tag (EFT/NEFT/TT) eligibility."""
    eligible, reason = await check_tag_eligibility(session, player_id, season, team_id)

    if not eligible:
        return EligibilityResult(
            action=ACTION_FRANCHISE_TAG,
            eligible=False,
            reason=reason,
            rule_citation=(
                "Franchise Tags: Players must have 0 years remaining; "
                "max 3 consecutive EFT/NEFT tags per player"
            ),
        )

    # Check allotment — use provided team_id or resolve from previous season contract
    allotment_team_id = team_id or await _get_team_id_for_player(
        session, player_id, season, check_previous_season=True,
    )
    if allotment_team_id is not None:
        from app.services.allotments import has_allotment

        has_tag_allotment = await has_allotment(
            session, allotment_team_id, season, "franchise_tag",
        )
        if not has_tag_allotment:
            return EligibilityResult(
                action=ACTION_FRANCHISE_TAG,
                eligible=False,
                reason="Team has already used their franchise tag allotment this season",
                rule_citation=(
                    "Franchise Tags: Each team may use 1 franchise tag per season"
                ),
            )

    consecutive = await get_consecutive_tag_count(session, player_id, season, team_id)
    prerequisites = ["Must declare simultaneously which player and which tag type"]
    if consecutive == 2:
        prerequisites.append(
            "This would be the 3rd consecutive EFT/NEFT tag — salary premium applies"
        )

    return EligibilityResult(
        action=ACTION_FRANCHISE_TAG,
        eligible=True,
        rule_citation=(
            "Franchise Tags: Player has 0 years remaining "
            "and tag allotment is available"
        ),
        prerequisites=prerequisites,
    )

# ERFA tender eligibility


async def _check_erfa_tender(
    session: AsyncSession,
    player_id: int,
    season: int,
    team_id: int | None = None,
) -> EligibilityResult:
    """Check ERFA tender eligibility."""
    eligible, reason = await check_erfa_eligibility(session, player_id, season, team_id)

    if not eligible:
        return EligibilityResult(
            action=ACTION_ERFA_TENDER,
            eligible=False,
            reason=reason,
            rule_citation=(
                "ERFA Tenders: Player must have 0 years remaining, previous contract "
                "signed at or below veteran minimum in one of previous two league years, "
                "and expired contract must not be an ERFA contract"
            ),
        )

    # Check tender allotment (shared 2 between RFA and ERFA)
    team_id = await _get_team_id_for_player(
        session, player_id, season, check_previous_season=True,
    )
    if team_id is not None:
        from app.services.allotments import has_allotment

        has_tender_allotment = await has_allotment(
            session, team_id, season, "tender",
        )
        if not has_tender_allotment:
            return EligibilityResult(
                action=ACTION_ERFA_TENDER,
                eligible=False,
                reason=(
                    "Team has used all tender allotments this season "
                    "(2 shared between RFA and ERFA)"
                ),
                rule_citation=(
                    "ERFA Tenders: Teams have 2 tenders per season "
                    "shared between RFA and ERFA"
                ),
            )

    return EligibilityResult(
        action=ACTION_ERFA_TENDER,
        eligible=True,
        rule_citation=(
            "ERFA Tenders: Player is eligible — "
            "expired contract at or below veteran minimum"
        ),
        prerequisites=["Player does not enter auction; exclusive rights retained"],
    )

# RFA tender eligibility


async def _check_rfa_tender(
    session: AsyncSession,
    player_id: int,
    season: int,
    team_id: int | None = None,
) -> EligibilityResult:
    """Check RFA tender eligibility."""
    eligible, reason = await check_rfa_eligibility(session, player_id, season, team_id)

    if not eligible:
        return EligibilityResult(
            action=ACTION_RFA_TENDER,
            eligible=False,
            reason=reason,
            rule_citation=(
                "RFA Tenders: Player must have 0 years remaining, contract must not "
                "include ineligible types from 2021 or earlier, must not be multi-year "
                "UFA from 2023 or earlier"
            ),
        )

    # Check tender allotment (shared 2 between RFA and ERFA)
    team_id = await _get_team_id_for_player(
        session, player_id, season, check_previous_season=True,
    )
    if team_id is not None:
        from app.services.allotments import has_allotment

        has_tender_allotment = await has_allotment(
            session, team_id, season, "tender",
        )
        if not has_tender_allotment:
            return EligibilityResult(
                action=ACTION_RFA_TENDER,
                eligible=False,
                reason=(
                    "Team has used all tender allotments this season "
                    "(2 shared between RFA and ERFA)"
                ),
                rule_citation=(
                    "RFA Tenders: Teams have 2 tenders per season "
                    "shared between RFA and ERFA"
                ),
            )

    return EligibilityResult(
        action=ACTION_RFA_TENDER,
        eligible=True,
        rule_citation=(
            "RFA Tenders: Player is eligible for restricted free agent tender"
        ),
        prerequisites=[
            "Must choose tender type: FRFA, SRFA, ORFA, or RRFA",
            "Match window is 48 hours after auction",
        ],
    )

# Buyout/Restructure eligibility


async def _check_buyout_restructure(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> EligibilityResult:
    """Check buyout/restructure eligibility."""
    eligible, reason = await check_buyout_eligibility(session, player_id, season)

    if not eligible:
        return EligibilityResult(
            action=ACTION_BUYOUT_RESTRUCTURE,
            eligible=False,
            reason=reason,
            rule_citation=(
                "Buyout/Restructure: Any player with a contract is eligible except "
                "players on Drafted Rookie or UDFA contracts until the final year"
            ),
        )

    # Check B/R allotment (1 per season)
    team_id = await _get_team_id_for_player(session, player_id, season)
    if team_id is not None:
        from app.services.allotments import has_allotment

        has_br_allotment = await has_allotment(
            session, team_id, season, "buyout_restructure",
        )
        if not has_br_allotment:
            return EligibilityResult(
                action=ACTION_BUYOUT_RESTRUCTURE,
                eligible=False,
                reason=(
                    "Team has already used their buyout/restructure "
                    "allotment this season"
                ),
                rule_citation=(
                    "Buyout/Restructure: Each team may use 1 B/R per season"
                ),
            )

    return EligibilityResult(
        action=ACTION_BUYOUT_RESTRUCTURE,
        eligible=True,
        rule_citation="Buyout/Restructure: Player is eligible for B/R",
        prerequisites=[
            "Must declare via Message Board by mid-April deadline",
            "Current contract is voided without Salary Cap Penalty",
            "Player enters B/R Auction with former team's bid at SD minimum",
        ],
    )

# Fifth Year Option eligibility


async def _check_fifth_year_option(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> EligibilityResult:
    """Check 5th Year Option eligibility."""
    constants = get_contract_constants()

    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()

    if player is None:
        return EligibilityResult(
            action=ACTION_FIFTH_YEAR_OPTION,
            eligible=False,
            reason="Player not found",
        )

    # Must be 1st round drafted rookie
    eligible_rounds = constants["fifth_year_option"]["eligible_rounds"]
    if player.draft_round not in eligible_rounds:
        return EligibilityResult(
            action=ACTION_FIFTH_YEAR_OPTION,
            eligible=False,
            reason=(
                f"Player was drafted in round {player.draft_round}, "
                f"only round(s) {eligible_rounds} are eligible for 5YO"
            ),
            rule_citation=(
                "5th Year Option: Only first-round drafted rookies are eligible"
            ),
        )

    # Load current active contract
    contract_result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == "active",
        )
        .order_by(Contract.salary.desc())
        .limit(1)
    )
    contract = contract_result.scalar_one_or_none()

    if contract is None:
        return EligibilityResult(
            action=ACTION_FIFTH_YEAR_OPTION,
            eligible=False,
            reason="No active contract found for this season",
            rule_citation="5th Year Option: Player must have an active contract",
        )

    # Must be in 4th season of contract (signed_season + 3 == current season)
    contract_year = season - contract.signed_season + 1
    if contract_year != 4:
        return EligibilityResult(
            action=ACTION_FIFTH_YEAR_OPTION,
            eligible=False,
            reason=(
                f"Player is in year {contract_year} of contract, must be in year 4"
            ),
            rule_citation=(
                "5th Year Option: Must be exercised in the player's 4th season"
            ),
        )

    # Must not have already exercised 5YO
    desig = contract.designation or ""
    if "+" in desig or "5YO" in desig.upper():
        return EligibilityResult(
            action=ACTION_FIFTH_YEAR_OPTION,
            eligible=False,
            reason="5th Year Option has already been exercised on this contract",
            rule_citation=(
                "5th Year Option: Cannot be exercised more than once per contract"
            ),
        )

    return EligibilityResult(
        action=ACTION_FIFTH_YEAR_OPTION,
        eligible=True,
        rule_citation=(
            "5th Year Option: First-round rookie in 4th season is eligible"
        ),
        prerequisites=[
            "Must declare via Message Board by July 1",
            "Immediately Fully Guaranteed and irreversible",
        ],
    )

# Proven Performance Escalator eligibility


async def _check_proven_performance_escalator(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> EligibilityResult:
    """Check Proven Performance Escalator eligibility."""
    constants = get_contract_constants()
    ppe_config = constants["proven_performance_escalator"]

    # Load player
    player_result = await session.execute(
        select(Player).where(Player.id == player_id)
    )
    player = player_result.scalar_one_or_none()

    if player is None:
        return EligibilityResult(
            action=ACTION_PROVEN_PERFORMANCE_ESCALATOR,
            eligible=False,
            reason="Player not found",
        )

    # Must be rounds 2-5 drafted rookie
    eligible_rounds = ppe_config["eligible_rounds"]
    if player.draft_round not in eligible_rounds:
        return EligibilityResult(
            action=ACTION_PROVEN_PERFORMANCE_ESCALATOR,
            eligible=False,
            reason=(
                f"Player was drafted in round {player.draft_round}, "
                f"only rounds {eligible_rounds} are eligible for PPE"
            ),
            rule_citation=(
                "Proven Performance Escalator: "
                "Only rounds 2-5 drafted rookies are eligible"
            ),
        )

    # Position must not be PK or PN
    ineligible_positions = ppe_config["ineligible_positions"]
    if player.position in ineligible_positions:
        return EligibilityResult(
            action=ACTION_PROVEN_PERFORMANCE_ESCALATOR,
            eligible=False,
            reason=f"Position {player.position} is ineligible for PPE",
            rule_citation=(
                "Proven Performance Escalator: "
                f"Positions {ineligible_positions} are ineligible"
            ),
        )

    # Load current active contract
    contract_result = await session.execute(
        select(Contract)
        .where(
            Contract.player_id == player_id,
            Contract.season == season,
            Contract.status == "active",
        )
        .order_by(Contract.salary.desc())
        .limit(1)
    )
    contract = contract_result.scalar_one_or_none()

    if contract is None:
        return EligibilityResult(
            action=ACTION_PROVEN_PERFORMANCE_ESCALATOR,
            eligible=False,
            reason="No active contract found for this season",
            rule_citation=(
                "Proven Performance Escalator: "
                "Player must have an active contract"
            ),
        )

    # Must be in 4th year of contract
    contract_year = season - contract.signed_season + 1
    eligible_year = ppe_config["eligible_contract_year"]
    if contract_year != eligible_year:
        return EligibilityResult(
            action=ACTION_PROVEN_PERFORMANCE_ESCALATOR,
            eligible=False,
            reason=(
                f"Player is in year {contract_year} of contract, "
                f"must be in year {eligible_year}"
            ),
            rule_citation=(
                "Proven Performance Escalator: "
                f"Must be in year {eligible_year} of contract"
            ),
        )

    return EligibilityResult(
        action=ACTION_PROVEN_PERFORMANCE_ESCALATOR,
        eligible=True,
        rule_citation=(
            "Proven Performance Escalator: "
            "Rounds 2-5 rookie in 4th year is eligible"
        ),
        prerequisites=[
            "Escalator salary determined by prior season performance percentile",
        ],
    )
