# Phase 5: Salary Cap & Validation - Context

**Gathered:** 2026-03-11
**Status:** Ready for planning

<vision>
## How This Should Work

When a GM looks at their team, they see the full cap picture — team-level summary of total cap hit broken down by contract type (NG/SD/FG) with penalties itemized, plus the ability to drill down to individual players to understand what each one really costs (base salary + penalty breakdown).

More importantly, before any contract action happens (extend, tag, buyout, tender), the system checks whether that action is legal for that player. If a GM tries to tag a player who's already been tagged twice, the system knows that's not allowed — before they waste time running calculations. It's a gate, not a report.

Phase 4 built the calculation engine — how to compute extensions, tags, tenders, buyouts. Phase 5 layers on top: "should you be allowed to do this?" and "what does it cost against the cap?" The contract engine stays as-is; this phase adds the rules enforcement and cap impact layer.

</vision>

<essential>
## What Must Be Nailed

- **Eligibility gates** — The system must know whether a specific player qualifies for a specific contract action, and return a clear yes/no with the bylaws rule that applies. This is the priority: preventing illegal moves before they happen.
- **Accurate cap math** — NG/SD/FG penalty calculations must match the bylaws exactly. Team-level rollup and per-player breakdown.
- **Clear reasoning** — When something isn't eligible, the response cites the specific rule. No ambiguity.

</essential>

<boundaries>
## What's Out of Scope

- No trade validation — just individual contract eligibility and cap penalties
- No UI — this is all backend logic; the cap dashboard UI comes in Phase 8
- No modification to Phase 4 contract engine — this layers on top, doesn't change existing calculations

</boundaries>

<specifics>
## Specific Ideas

- Eligibility results should be simple: eligible or not, with a clear explanation citing the bylaws rule
- Focus on wrong-action prevention (e.g., tagging a player who can't be tagged, extending a player who isn't eligible for extension)
- Two levels of cap view: team summary (total cap hit by contract type) and per-player detail (base salary + penalty)

</specifics>

<notes>
## Additional Context

The NG/SD/FG contract type classification was deferred in Phase 3 (set to NG placeholder). This phase needs to either implement proper classification or work with it as a prerequisite.

Priority order: eligibility gates first, then cap penalty calculations. The gates are the real value — preventing mistakes is more important than reporting numbers.

</notes>

---

*Phase: 05-salary-cap-validation*
*Context gathered: 2026-03-11*
