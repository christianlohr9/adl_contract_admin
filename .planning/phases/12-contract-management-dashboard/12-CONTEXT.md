# Phase 12: Contract Management Dashboard - Context

**Gathered:** 2026-03-12
**Status:** Ready for planning

<vision>
## How This Should Work

When a GM navigates to the Contract Management Dashboard, they see a status bar at the top showing which action windows are currently open or closed (tags, tenders, extensions, B/R, 5YO, PPE). At a glance, before even looking at the roster, they know what's in play right now based on the league calendar.

Below the status bar is a single data table — one unified roster view. Each row is a player, and columns show what actions they're eligible for along with key numbers (tag cost, tender amount, extension values, buyout penalties). The table is sortable and filterable. By default it shows only players eligible for at least one action, keeping the view clean and focused. A toggle lets GMs switch to the full roster when they need broader context.

The whole thing is built with Tailwind and shadcn/ui — clean, modern, consistent with the existing app. It's an information layer, not a transaction layer.

</vision>

<essential>
## What Must Be Nailed

- **Window-awareness front and center** — GMs need to instantly see which contract actions are currently available based on the league calendar. This is the primary value over the existing player-by-player views.
- **Status bar showing open/closed windows** — Before looking at any player data, the GM knows what's in play right now.
- **Eligible-only default with full roster toggle** — Clean default view that only shows actionable players, with the option to see everyone.

</essential>

<boundaries>
## What's Out of Scope

- Executing actions — this is view-only, no submitting tags, tenders, or extensions from this dashboard
- Multi-team comparison — only your own roster, no cross-team or league-wide views
- Action submission workflows — deferred to a future phase

</boundaries>

<specifics>
## Specific Ideas

- Status bar/strip at the top of the page showing current window statuses before the table
- Data table with sortable/filterable columns for each action type and their calculated values
- Toggle between "eligible only" (default) and "full roster" views
- Clean, modern look using existing Tailwind + shadcn/ui patterns from the app

</specifics>

<notes>
## Additional Context

The roster-wide eligibility API (Phase 11) already returns all the data needed — this phase is about presenting it well. The existing player detail page (Phase 8) shows contract tools per-player; this dashboard gives the team-wide view so GMs can compare without clicking into individual players.

</notes>

---

*Phase: 12-contract-management-dashboard*
*Context gathered: 2026-03-12*
