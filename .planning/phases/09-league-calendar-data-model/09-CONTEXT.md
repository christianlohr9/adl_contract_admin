# Phase 9: League Calendar Data Model - Context

**Gathered:** 2026-03-12
**Status:** Ready for planning

<vision>
## How This Should Work

A simple, admin-configured date store. Once a year, I (as commissioner) enter all the key league dates for the upcoming season — extension deadlines, tag/tender windows, buyout/restructure deadlines, 5YO deadline, auction dates, everything that matters for contract administration.

The system then just *knows* what the current dates are. No complex period logic in this phase — just accurate, reliable storage of when every deadline and window falls. Set it once at the start of each league year and it stays put.

</vision>

<essential>
## What Must Be Nailed

- **Accurate deadline storage** — Every contract tool must have its exact window dates captured: when it opens, when it closes. No guessing, no ambiguity.
- **Complete coverage** — All dates from the bylaws are represented: oEXT deadline, iEXT window, tag/tender deadlines, B/R deadline, 5YO deadline, all auction dates.
- **Per-year configuration** — Each season gets its own set of dates, so historical lookups work too.

</essential>

<boundaries>
## What's Out of Scope

- No tool gating — actually enabling/disabling contract tools based on dates is Phase 10
- No calendar visualization — visual timeline or calendar UI is Phase 13
- No period detection logic — determining "what period are we in right now" is Phase 10
- This phase: data model, CRUD API, and a basic admin form for entering dates

</boundaries>

<specifics>
## Specific Ideas

All dates/periods to capture come from the bylaws — oEXT, iEXT, tags, tenders, B/R, 5YO, PPE, auction sequence, etc. No external sources needed.

</specifics>

<notes>
## Additional Context

This is the foundation that makes the rest of v1.1 possible. Phases 10-13 all depend on having accurate, queryable season dates. Getting the data model right here pays dividends downstream.

Commissioner is the only user entering dates — no multi-user admin needed.

</notes>

---

*Phase: 09-league-calendar-data-model*
*Context gathered: 2026-03-12*
