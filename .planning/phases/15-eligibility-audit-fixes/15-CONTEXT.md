# Phase 15: Eligibility Audit & Fixes - Context

**Gathered:** 2026-03-13
**Status:** Ready for planning

<vision>
## How This Should Work

A systematic, two-pass audit of all 7 contract action eligibility checks. First pass: compare every eligibility rule in the code against the bylaws line by line, cataloging every discrepancy across all 7 actions. Second pass: fix them systematically once the full picture is known.

Validation follows the same layered approach — use targeted scenarios (rookies, veterans, franchise-tagged players, edge cases) to find and fix bugs, then do a full sweep across all 32 teams' rosters to confirm nothing was missed.

The end state is total trust in the output. When the app says a player is eligible or not eligible for a contract action, it's correct. Period. No need to double-check against the bylaws manually.

</vision>

<essential>
## What Must Be Nailed

- **Complete coverage of all 7 contract actions** — every single action gets the full audit treatment, no shortcuts, no skipping
- **Zero known discrepancies** — every bylaw rule is reflected in code, and no player on any roster shows a wrong eligibility result (except NFL kickoff rule, which is Phase 16)
- **Catalog first, fix second** — get the full picture of all discrepancies before fixing anything, no whack-a-mole
- **Audit trail document** — a bylaw-to-code mapping that shows which code implements each bylaw rule, serving as a living reference for future bylaw changes

</essential>

<boundaries>
## What's Out of Scope

- No UI changes — this is purely backend eligibility logic fixes
- No NFL kickoff rule — that's Phase 16 (UAT-001)
- No new features or eligibility actions beyond what the bylaws define

</boundaries>

<specifics>
## Specific Ideas

- Bylaws are the definitive source of truth — if code doesn't match, it's a code bug, not a bylaws interpretation issue
- Validation approach: targeted edge-case scenarios first, then full roster sweep to confirm
- The audit should produce a bylaw-to-code mapping document as a lasting artifact

</specifics>

<notes>
## Additional Context

No specific eligibility bugs are known yet — the audit itself is expected to surface the problems. Phase 14 just completed, bringing in historical scores and contract data that enable accurate eligibility checks for the first time.

The user wants to be able to show eligibility results to any GM with full confidence they match the bylaws.

</notes>

---

*Phase: 15-eligibility-audit-fixes*
*Context gathered: 2026-03-13*
