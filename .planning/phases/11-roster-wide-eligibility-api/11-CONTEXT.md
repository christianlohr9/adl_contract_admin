# Phase 11: Roster-Wide Eligibility API - Context

**Gathered:** 2026-03-12
**Status:** Ready for planning

<vision>
## How This Should Work

A GM hits one endpoint with their team ID and gets back a complete summary of every available contract action across their entire roster, grouped by action type. Instead of clicking through 53 players one by one, they see organized sections: "Here are your tag candidates," "here are your tender candidates," "here are your extension-eligible players," etc.

Each player entry within a group is lean — just the essentials: name, position, current salary, and the key calculated number for that action (tag price, tender amount, extension headline value, buyout cost). The full calculations are already available on each player's detail page for anyone who wants to drill in.

Only actions with currently-open windows appear. If the tag deadline has passed, there's no tag section at all. This keeps the response actionable — everything shown is something the GM can actually do right now.

Players eligible for multiple actions appear in every relevant group. A player who qualifies for both a tag and an extension shows up in both sections — no hidden candidates.

</vision>

<essential>
## What Must Be Nailed

- **One-call convenience** — The entire value is not having to click through players individually. One API call, whole roster, grouped by action type.
- **Lean summaries** — Key numbers only per player per action. Don't dump full calculation trees.
- **Window-gated** — Only show action groups for currently-open windows. Keep it actionable.

</essential>

<boundaries>
## What's Out of Scope

- No frontend UI — the dashboard that displays this data is Phase 12
- No new calculation logic — strictly reuse existing per-player services, just aggregate them
- No closed-window actions — don't include action groups where the window isn't open

</boundaries>

<specifics>
## Specific Ideas

- Players appear in every action group they're eligible for (duplicated across groups, not primary-group-only)
- Action groups: tags, tenders, extensions, B/R, 5YO, PPE
- Each player entry: name, position, current salary, headline calculated value for that action

</specifics>

<notes>
## Additional Context

This is the aggregation layer that makes Phase 12's contract management dashboard possible. The per-player tools and window gating already exist from Phases 4-5 and 10 — this phase wires them together into a roster-wide view.

</notes>

---

*Phase: 11-roster-wide-eligibility-api*
*Context gathered: 2026-03-12*
