# Phase 1: Rules Extraction - Context

**Gathered:** 2026-03-10
**Status:** Ready for planning

<vision>
## How This Should Work

Machine-readable output is the priority. The bylaws need to be parsed into JSON constants and YAML formulas that the contract engine can consume directly. Every contract type needs coverage — extensions (X-A/B), franchise/transition tags (X-C), ERFA tenders (X-D), buyouts/restructures (X-E), and salary cap penalties.

The existing bylaws markdown just gets moved into `rules/docs/` as-is — no rewriting or polishing needed. All the real effort goes into the JSON and YAML files.

Files should be organized by how the engine will use them — grouped by contract type (extensions, tags, tenders, buyouts, cap) rather than by bylaw article number.

</vision>

<essential>
## What Must Be Nailed

- **Accuracy** — Every number, threshold, and formula must exactly match the bylaws. One wrong constant breaks the whole engine later.
- **Completeness** — Every rule that affects contract calculations must be captured. No gaps that force going back to re-read bylaws in later phases.

</essential>

<boundaries>
## What's Out of Scope

- No calculation code or engine logic — strictly extraction and structuring
- No database schemas or models — that's Phase 2
- No doc polishing — existing bylaws markdown goes into docs as-is

</boundaries>

<specifics>
## Specific Ideas

- JSON/YAML files should be raw values only — no descriptions or bylaw section references. Keep them lean.
- Group by use/contract type, not by bylaw article structure
- The bylaws themselves serve as the source of truth for tracing values

</specifics>

<notes>
## Additional Context

The user sees the docs as unnecessary for both humans and machines — the bylaws are the reference. The JSON constants and YAML formulas are the only outputs that matter for this phase.

</notes>

---

*Phase: 01-rules-extraction*
*Context gathered: 2026-03-10*
