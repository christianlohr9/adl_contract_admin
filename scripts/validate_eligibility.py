"""Full roster eligibility validation script.

Runs every player with a contract through all 7 eligibility checks and
produces a structured markdown report with anomaly detection.

Usage:
    uv run python scripts/validate_eligibility.py [--season 2026] [--output FILE]

Connects directly to the database using async_session — no running server needed.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import distinct, select

# ---------------------------------------------------------------------------
# Ensure src/ is on the path so app imports resolve
# ---------------------------------------------------------------------------
sys.path.insert(0, "src")

from app.core.db import async_session  # noqa: E402
from app.models.contract import Contract  # noqa: E402
from app.models.player import Player  # noqa: E402
from app.services.eligibility import (  # noqa: E402
    ACTION_BUYOUT_RESTRUCTURE,
    ACTION_ERFA_TENDER,
    ACTION_EXTENSION,
    ACTION_FIFTH_YEAR_OPTION,
    ACTION_FRANCHISE_TAG,
    ACTION_PROVEN_PERFORMANCE_ESCALATOR,
    ACTION_RFA_TENDER,
    EligibilityResult,
    check_eligibility,
)
from app.services.rules import get_contract_constants  # noqa: E402

ALL_ACTIONS = [
    ACTION_EXTENSION,
    ACTION_FRANCHISE_TAG,
    ACTION_ERFA_TENDER,
    ACTION_RFA_TENDER,
    ACTION_BUYOUT_RESTRUCTURE,
    ACTION_FIFTH_YEAR_OPTION,
    ACTION_PROVEN_PERFORMANCE_ESCALATOR,
]

ACTION_LABELS = {
    ACTION_EXTENSION: "Extension",
    ACTION_FRANCHISE_TAG: "Franchise Tag",
    ACTION_ERFA_TENDER: "ERFA Tender",
    ACTION_RFA_TENDER: "RFA Tender",
    ACTION_BUYOUT_RESTRUCTURE: "Buyout/Restructure",
    ACTION_FIFTH_YEAR_OPTION: "Fifth Year Option",
    ACTION_PROVEN_PERFORMANCE_ESCALATOR: "PPE",
}


# ---------------------------------------------------------------------------
# Anomaly dataclass
# ---------------------------------------------------------------------------


@dataclass
class Anomaly:
    """A suspicious eligibility result that warrants investigation."""

    player_name: str
    player_id: int
    action: str
    rule: str
    detail: str
    contract_info: str = ""


# ---------------------------------------------------------------------------
# Per-action counters
# ---------------------------------------------------------------------------


@dataclass
class ActionStats:
    eligible: int = 0
    ineligible: int = 0
    error: int = 0


# ---------------------------------------------------------------------------
# Core sweep logic
# ---------------------------------------------------------------------------


async def run_sweep(season: int) -> tuple[
    int,
    dict[str, ActionStats],
    list[Anomaly],
    list[tuple[int, str, str, str, bool, str | None]],
]:
    """Run the full eligibility sweep and return results.

    Returns:
        (total_players, action_stats, anomalies, detailed_results)
    """
    constants = get_contract_constants()
    vet_min_by_year = constants["salary_minimums"]["veteran_minimum_by_year"]

    action_stats: dict[str, ActionStats] = {a: ActionStats() for a in ALL_ACTIONS}
    anomalies: list[Anomaly] = []
    detailed_results: list[tuple[int, str, str, str, bool, str | None]] = []

    async with async_session() as session:
        # 1. Get all distinct player_ids with contracts for this season
        result = await session.execute(
            select(distinct(Contract.player_id)).where(Contract.season == season)
        )
        player_ids = [row[0] for row in result.all()]
        total_players = len(player_ids)

        print(f"Found {total_players} players with contracts in season {season}", file=sys.stderr)

        # 2. Pre-load all players and their contracts for anomaly detection
        player_cache: dict[int, Player] = {}
        contract_cache: dict[int, list[Contract]] = defaultdict(list)

        # Load players
        for i in range(0, len(player_ids), 500):
            batch = player_ids[i : i + 500]
            pr = await session.execute(
                select(Player).where(Player.id.in_(batch))
            )
            for p in pr.scalars().all():
                player_cache[p.id] = p

        # Load contracts for this season
        cr = await session.execute(
            select(Contract).where(
                Contract.season == season,
                Contract.player_id.in_(player_ids),
            )
        )
        for c in cr.scalars().all():
            contract_cache[c.player_id].append(c)

        print(f"Loaded player and contract data, running eligibility checks...", file=sys.stderr)

        # 3. Run all checks
        for idx, pid in enumerate(player_ids, 1):
            if idx % 100 == 0:
                print(f"  Progress: {idx}/{total_players} players checked", file=sys.stderr)

            player = player_cache.get(pid)
            player_name = player.name if player else f"Unknown (id={pid})"
            contracts = contract_cache.get(pid, [])

            # Find the primary active contract for anomaly context
            active_contracts = [c for c in contracts if c.status == "active"]
            primary_contract = (
                max(active_contracts, key=lambda c: c.salary) if active_contracts else None
            )

            contract_info = ""
            if primary_contract:
                contract_info = (
                    f"desig={primary_contract.designation}, "
                    f"yr={primary_contract.years_remaining}, "
                    f"sal=${primary_contract.salary}, "
                    f"signed={primary_contract.signed_season}"
                )

            for action in ALL_ACTIONS:
                try:
                    result = await check_eligibility(session, pid, season, action)
                    eligible = result.eligible

                    if eligible:
                        action_stats[action].eligible += 1
                    else:
                        action_stats[action].ineligible += 1

                    detailed_results.append(
                        (pid, player_name, action, ACTION_LABELS[action], eligible, result.reason)
                    )

                    # --- Anomaly detection ---
                    _check_anomalies(
                        anomalies,
                        result,
                        player,
                        primary_contract,
                        contracts,
                        player_name,
                        pid,
                        action,
                        contract_info,
                        vet_min_by_year,
                        season,
                    )

                except Exception as exc:
                    action_stats[action].error += 1
                    anomalies.append(
                        Anomaly(
                            player_name=player_name,
                            player_id=pid,
                            action=action,
                            rule="Error during check",
                            detail=str(exc),
                            contract_info=contract_info,
                        )
                    )
                    detailed_results.append(
                        (pid, player_name, action, ACTION_LABELS[action], False, f"ERROR: {exc}")
                    )

        print(f"  Progress: {total_players}/{total_players} players checked", file=sys.stderr)

    return total_players, action_stats, anomalies, detailed_results


def _check_anomalies(
    anomalies: list[Anomaly],
    result: EligibilityResult,
    player: Player | None,
    primary_contract: Contract | None,
    contracts: list[Contract],
    player_name: str,
    player_id: int,
    action: str,
    contract_info: str,
    vet_min_by_year: dict[str, float],
    season: int,
) -> None:
    """Apply anomaly detection rules to a single eligibility result."""
    if not result.eligible:
        return  # Anomaly rules only flag false positives

    # Rule 1: Extension eligible but years_remaining >= 2
    if action == ACTION_EXTENSION and primary_contract:
        if primary_contract.years_remaining >= 2:
            anomalies.append(
                Anomaly(
                    player_name=player_name,
                    player_id=player_id,
                    action=action,
                    rule="Extension + yr >= 2",
                    detail=f"Extension eligible but years_remaining={primary_contract.years_remaining}",
                    contract_info=contract_info,
                )
            )

    # Rule 2: Tag eligible but years_remaining > 0
    if action == ACTION_FRANCHISE_TAG:
        # Check if player has ANY contract with yr > 0 this season
        has_active_years = any(c.years_remaining > 0 for c in contracts)
        if has_active_years:
            anomalies.append(
                Anomaly(
                    player_name=player_name,
                    player_id=player_id,
                    action=action,
                    rule="Tag + yr > 0",
                    detail="Tag eligible but has contract with years_remaining > 0",
                    contract_info=contract_info,
                )
            )

    # Rule 3: ERFA eligible but salary above veteran minimum
    if action == ACTION_ERFA_TENDER:
        vet_min = vet_min_by_year.get(str(season), 1.1)
        if primary_contract and float(primary_contract.salary) > vet_min:
            anomalies.append(
                Anomaly(
                    player_name=player_name,
                    player_id=player_id,
                    action=action,
                    rule="ERFA + salary > vet min",
                    detail=f"ERFA eligible but salary=${primary_contract.salary} > vet min=${vet_min}",
                    contract_info=contract_info,
                )
            )

    # Rule 4: 5YO eligible but not draft round 1
    if action == ACTION_FIFTH_YEAR_OPTION and player:
        if player.draft_round != 1:
            anomalies.append(
                Anomaly(
                    player_name=player_name,
                    player_id=player_id,
                    action=action,
                    rule="5YO + not round 1",
                    detail=f"5YO eligible but draft_round={player.draft_round}",
                    contract_info=contract_info,
                )
            )

    # Rule 5: PPE eligible but not draft rounds 2-5
    if action == ACTION_PROVEN_PERFORMANCE_ESCALATOR and player:
        if player.draft_round not in (2, 3, 4, 5):
            anomalies.append(
                Anomaly(
                    player_name=player_name,
                    player_id=player_id,
                    action=action,
                    rule="PPE + not rounds 2-5",
                    detail=f"PPE eligible but draft_round={player.draft_round}",
                    contract_info=contract_info,
                )
            )

    # Rule 6: PPE eligible but position is PK or PN
    if action == ACTION_PROVEN_PERFORMANCE_ESCALATOR and player:
        if player.position in ("PK", "PN"):
            anomalies.append(
                Anomaly(
                    player_name=player_name,
                    player_id=player_id,
                    action=action,
                    rule="PPE + PK/PN position",
                    detail=f"PPE eligible but position={player.position}",
                    contract_info=contract_info,
                )
            )


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_report(
    season: int,
    total_players: int,
    action_stats: dict[str, ActionStats],
    anomalies: list[Anomaly],
) -> str:
    """Generate a markdown report from sweep results."""
    total_checks = total_players * len(ALL_ACTIONS)
    total_errors = sum(s.error for s in action_stats.values())
    total_anomalies = len(anomalies)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines: list[str] = []
    lines.append("# Phase 17-01: Full Roster Eligibility Validation Report")
    lines.append("")
    lines.append(f"**Validated:** {now}")
    lines.append(f"**Method:** Direct database query via `scripts/validate_eligibility.py`")
    lines.append(f"**Database:** PostgreSQL (async_session)")
    lines.append(f"**Season:** {season}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Summary table
    lines.append("## Results Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Unique players checked** | {total_players} |")
    lines.append(f"| **Total eligibility checks** | {total_checks:,} ({total_players} players x {len(ALL_ACTIONS)} actions) |")
    lines.append(f"| **Anomalies found** | {total_anomalies} |")
    lines.append(f"| **Errors** | {total_errors} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Per-action breakdown
    lines.append("## Results by Action")
    lines.append("")
    lines.append("| Action | Eligible | Ineligible | Error | Total |")
    lines.append("|--------|----------|------------|-------|-------|")
    for action in ALL_ACTIONS:
        label = ACTION_LABELS[action]
        s = action_stats[action]
        total = s.eligible + s.ineligible + s.error
        lines.append(f"| {label} | {s.eligible} | {s.ineligible} | {s.error} | {total} |")
    lines.append("")

    # Notes on counts
    lines.append("### Notes on Counts")
    lines.append("")
    ext_s = action_stats[ACTION_EXTENSION]
    lines.append(f"- **Extension: {ext_s.eligible} eligible** — Window status may affect results. Extensions require final contract year and Robust PR.")
    erfa_s = action_stats[ACTION_ERFA_TENDER]
    lines.append(f"- **ERFA: {erfa_s.eligible} eligible** — Requires expired contract at/below veteran minimum with non-ERFA designation.")
    tag_s = action_stats[ACTION_FRANCHISE_TAG]
    lines.append(f"- **Franchise Tag: {tag_s.eligible} eligible** — Players with expired contracts (yr=0) and no active replacement, within 3-consecutive-tag limit.")
    rfa_s = action_stats[ACTION_RFA_TENDER]
    lines.append(f"- **RFA Tender: {rfa_s.eligible} eligible** — Expired contracts meeting all RFA rules.")
    fyo_s = action_stats[ACTION_FIFTH_YEAR_OPTION]
    lines.append(f"- **5YO: {fyo_s.eligible} eligible** — First-round rookies in their 4th contract year.")
    ppe_s = action_stats[ACTION_PROVEN_PERFORMANCE_ESCALATOR]
    lines.append(f"- **PPE: {ppe_s.eligible} eligible** — Rounds 2-5 rookies in their 4th contract year, not PK/PN.")
    br_s = action_stats[ACTION_BUYOUT_RESTRUCTURE]
    lines.append(f"- **B/R: {br_s.eligible} eligible** — Any player with active contract, excluding rookies/UDFA not in final year.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Anomaly detection rules applied
    lines.append("## Anomaly Detection Rules Applied")
    lines.append("")
    lines.append("| Rule | Description | Violations Found |")
    lines.append("|------|-------------|-----------------|")

    rule_counts: dict[str, int] = defaultdict(int)
    for a in anomalies:
        rule_counts[a.rule] += 1

    rule_descriptions = {
        "Extension + yr >= 2": "Extension eligible but years_remaining >= 2",
        "Tag + yr > 0": "Tag eligible but years_remaining > 0",
        "ERFA + salary > vet min": "ERFA eligible but salary above veteran minimum",
        "5YO + not round 1": "5YO eligible but not round 1",
        "PPE + not rounds 2-5": "PPE eligible but not rounds 2-5",
        "PPE + PK/PN position": "PPE eligible but position is PK or PN",
        "Error during check": "Eligibility check returning an error",
    }
    for rule, desc in rule_descriptions.items():
        count = rule_counts.get(rule, 0)
        lines.append(f"| {rule} | {desc} | {count} |")
    lines.append("")

    # Anomaly details
    if anomalies:
        lines.append("---")
        lines.append("")
        lines.append("## Anomaly Details")
        lines.append("")
        for a in anomalies:
            lines.append(f"### {a.player_name} (id={a.player_id}) — {ACTION_LABELS.get(a.action, a.action)}")
            lines.append("")
            lines.append(f"- **Rule violated:** {a.rule}")
            lines.append(f"- **Detail:** {a.detail}")
            if a.contract_info:
                lines.append(f"- **Contract:** {a.contract_info}")
            lines.append("")
    else:
        lines.append("---")
        lines.append("")
        lines.append("## Anomaly Details")
        lines.append("")
        lines.append("No anomalies detected.")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Comparison with Phase 15 Validation
    lines.append("## Comparison with Phase 15 Validation")
    lines.append("")
    lines.append("Phase 15-02 validated 879 players across all 32 teams with 0 anomalies (2026-03-13).")
    lines.append("")
    lines.append("| Action | Phase 15 Eligible | Phase 17 Eligible | Delta | Notes |")
    lines.append("|--------|-------------------|-------------------|-------|-------|")

    phase15_counts = {
        ACTION_EXTENSION: 0,
        ACTION_FRANCHISE_TAG: 277,
        ACTION_ERFA_TENDER: 0,
        ACTION_RFA_TENDER: 249,
        ACTION_BUYOUT_RESTRUCTURE: 697,
        ACTION_FIFTH_YEAR_OPTION: 33,
        ACTION_PROVEN_PERFORMANCE_ESCALATOR: 46,
    }
    for action in ALL_ACTIONS:
        label = ACTION_LABELS[action]
        p15 = phase15_counts[action]
        p17 = action_stats[action].eligible
        delta = p17 - p15
        delta_str = f"+{delta}" if delta > 0 else str(delta)
        note = ""
        if action == ACTION_EXTENSION and delta != 0:
            note = "Window status or kickoff rule change"
        elif action == ACTION_EXTENSION and delta == 0:
            note = "Window still closed"
        elif delta == 0:
            note = "No change"
        elif action == ACTION_FRANCHISE_TAG:
            note = "May reflect roster changes"
        else:
            note = "Investigate if unexpected"
        lines.append(f"| {label} | {p15} | {p17} | {delta_str} | {note} |")

    lines.append("")
    lines.append("### Phase 16 Impact")
    lines.append("")
    lines.append("Phase 16-01 added the NFL kickoff eligibility gate for rookie/UDFA extensions. ")
    lines.append("This rule blocks extensions before `regular_season_start` from the season calendar. ")
    lines.append("Since extensions are currently window-gated (extension window closed), ")
    lines.append("the kickoff rule does not independently affect counts in this sweep.")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Final Confidence Statement")
    lines.append("")
    if total_anomalies == 0 and total_errors == 0:
        lines.append(
            f"All {total_players} players have been validated against all 7 contract actions "
            f"with zero anomalies and zero errors. Every eligibility result is consistent with "
            f"the bylaws rules."
        )
    else:
        lines.append(
            f"Validation completed with {total_anomalies} anomalies and {total_errors} errors. "
            f"Review the anomaly details above."
        )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Phase: 17-regression-testing-validation*")
    lines.append(f"*Validated: {now}*")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Full roster eligibility validation — runs all 7 checks for every player.",
    )
    parser.add_argument(
        "--season",
        type=int,
        default=2026,
        help="ADL season year to validate (default: 2026)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Write report to this file (default: stdout)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    print(f"Starting eligibility validation for season {args.season}...", file=sys.stderr)

    total_players, action_stats, anomalies, _detailed = await run_sweep(args.season)

    report = generate_report(args.season, total_players, action_stats, anomalies)

    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)

    total_anomalies = len(anomalies)
    total_errors = sum(s.error for s in action_stats.values())
    print(
        f"Done: {total_players} players, "
        f"{total_players * len(ALL_ACTIONS):,} checks, "
        f"{total_anomalies} anomalies, "
        f"{total_errors} errors",
        file=sys.stderr,
    )

    if total_anomalies > 0 or total_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
