# Phase 10: Period Detection & Date-Aware Eligibility - Context

**Gathered:** 2026-03-12
**Status:** Ready for planning

<vision>
## How This Should Work

Each contract tool checks its own relevant calendar dates to determine whether it's currently available. There's no abstract "period" concept — extensions check the oEXT/iEXT windows, tags check the tag deadline, tenders check the tender deadline, and so on. Tool-centric date checking, not period-centric.

When a GM opens a player's contract tools outside a window, the tool appears greyed out with a read-only preview of the calculated values. A banner shows when the window opens or that it has closed — so GMs can plan ahead but can't act until the right time.

If no season calendar is configured, nothing is available. The commissioner must set up the calendar before any contract tools become usable. This forces proper setup and avoids ambiguity.

</vision>

<essential>
## What Must Be Nailed

- **Tool-centric date gating** — Each contract tool checks its own calendar dates to determine open/closed status. No abstract period layer.
- **Eligibility integration** — Date constraints woven into the existing eligibility checks so the backend enforces windows, not just the UI.
- **Strict calendar requirement** — No calendar configured = no tools available. Forces commissioner to set dates first.

</essential>

<boundaries>
## What's Out of Scope

- Frontend changes — the greyed-out UI treatment, banners, and dimmed tabs come in a later phase
- Roster-wide aggregation — "show me all tag candidates" is Phase 11
- This is backend-only: period detection logic + eligibility gating enforcement

</boundaries>

<specifics>
## Specific Ideas

- Mixed approach to gating: tools always show calculated values (read-only preview for planning) but are hard-gated for action during their windows
- Each tool independently checks its relevant dates from SeasonCalendar — no shared "current period" enum
- API responses should include window status info (open/closed, dates) — exact shape to be determined during planning based on existing patterns

</specifics>

<notes>
## Additional Context

This phase resolves ISS-001 (extension window awareness) at the backend level. The frontend manifestation of greyed-out tools with previews will come in Phase 12 (Contract Management Dashboard) when the UI is built to consume these new eligibility signals.

The strict fallback (no calendar = nothing available) aligns with the admin-first approach from Phase 9 where the commissioner manually configures all dates.

</notes>

---

*Phase: 10-period-detection-eligibility*
*Context gathered: 2026-03-12*
