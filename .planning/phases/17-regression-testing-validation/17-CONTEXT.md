# Phase 17: Regression Testing & Validation - Context

**Gathered:** 2026-03-13
**Status:** Ready for planning

<vision>
## How This Should Work

A full roster sweep that runs every player on every roster through all 7 contract action eligibility checks. It's a CLI script — run it once, get a report, review the results, and know whether the system is correct enough to ship v1.2.

The report shows each player alongside each action and the specific reasoning for why they passed or failed. This gives enough detail to quickly trace the logic and judge whether a result is correct or a real bug.

This isn't a permanent testing framework — it's a final confidence check. Run it, review it, fix anything that's wrong, and call v1.2 done.

</vision>

<essential>
## What Must Be Nailed

- **Confidence to ship** — The primary goal is knowing the system is trustworthy. After this phase, there should be no lingering doubt about eligibility correctness.
- **Traceable reasoning** — For every player/action combination, show the "why" so issues can be quickly triaged (player + action + reasoning).
- **Fix what's found** — This isn't just a report. If the sweep finds real bugs, fix them as part of this phase. Don't ship v1.2 with known issues.

</essential>

<boundaries>
## What's Out of Scope

- No UI changes — this is pure backend/CLI validation, don't touch the frontend
- Not building a permanent automated test suite — this is a one-time validation pass
- Not a performance exercise — correctness is the only concern

</boundaries>

<specifics>
## Specific Ideas

- CLI script that dumps results to terminal or file
- Report format: player + action + pass/fail + reasoning for each combination
- Every player on every roster gets checked — no sampling, complete coverage
- Any bugs discovered get fixed inline as part of this phase

</specifics>

<notes>
## Additional Context

This is the capstone of v1.2 (Data Integrity & Eligibility Accuracy). Phases 14-16 imported historical data, audited eligibility logic, and added the NFL kickoff rule. This phase validates that all of that work is correct when applied across the full league.

The user wants to come out of this phase with full confidence that the 7 contract actions (extensions, tags, tenders, buyouts, etc.) produce correct eligibility results for every player in the league.

</notes>

---

*Phase: 17-regression-testing-validation*
*Context gathered: 2026-03-13*
