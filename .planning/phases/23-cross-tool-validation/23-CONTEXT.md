# Phase 23: Cross-Tool Validation - Context

**Gathered:** 2026-04-04
**Status:** Ready for planning

<vision>
## How This Should Work

One big sweep across all 1,549 players, running every tool's eligibility and pricing calculations and comparing them against the corresponding spreadsheet tabs. This is the capstone of v1.3 — the moment that proves the app matches the spreadsheet end-to-end.

The output is a discrepancy-only report. If everything matches, it's a clean bill of health. If there are mismatches, they surface with enough context to diagnose — player, tool, expected vs computed values. The goal is either confirming zero discrepancies or clearly identifying what's left to fix.

</vision>

<essential>
## What Must Be Nailed

- **Confidence signal** — prove the app matches the spreadsheet so it can be confidently retired
- **Catch stragglers** — find any edge cases or cross-tool interactions that slipped through the per-tool validations in phases 18-22
- **Full coverage** — both eligibility flags AND computed prices/values for every tool (FT, EXT, Tenders, 5YO/PPE, B/R)

</essential>

<boundaries>
## What's Out of Scope

- Performance optimization — correctness is all that matters, don't worry about speed
- Fixing discrepancies — this phase reports only; fixes (if needed) happen separately
- New features, UI changes, or engine modifications — purely a validation exercise

</boundaries>

<specifics>
## Specific Ideas

- Discrepancy-only output — don't flood with matching rows, only surface mismatches
- Enough diagnostic context per mismatch to pinpoint the issue (player, tool, field, expected vs actual)

</specifics>

<notes>
## Additional Context

This is the final phase of v1.3 Data Integrity 2. Phases 18-22 validated each tool individually — this phase confirms everything holds together across the full player population. A clean result means the spreadsheet is officially redundant.

</notes>

---

*Phase: 23-cross-tool-validation*
*Context gathered: 2026-04-04*
