# Phase 13: Calendar/Timeline & Deadline Countdowns - Context

**Gathered:** 2026-03-12
**Status:** Ready for research

<vision>
## How This Should Work

When a GM opens the contract management dashboard, they immediately see countdown cards showing upcoming deadlines — tag deadline, tender deadline, B/R deadline, extension windows, etc. Each card shows the deadline name, how many days remain, and is color-coded by urgency: green when there's plenty of time, yellow as it approaches, red when it's imminent.

This replaces the existing window status bar from Phase 12 with something more useful and visually clear. Instead of just showing open/closed states, the cards communicate urgency and timing — "you have 12 days" hits differently than "open."

Cards only show upcoming deadlines. Once a deadline passes, its card disappears. The dashboard stays clean and actionable — no clutter from things GMs can no longer act on.

Each card includes a subtle hint about affected players — a small count like "3 eligible" — so GMs get a nudge about what needs attention without the card becoming a full summary. The data table below already has the details.

</vision>

<essential>
## What Must Be Nailed

- **Deadline urgency at a glance** — Color-coded countdown cards (green/yellow/red) that make it impossible to miss an approaching deadline
- **Period awareness** — GMs always know what period they're in and what actions are currently available
- **Replaces the status bar** — The countdown cards are the evolution of the Phase 12 window status bar, not an addition alongside it

</essential>

<boundaries>
## What's Out of Scope

- No notifications (email, push, Slack) — visual indicators on the dashboard only
- No dedicated calendar/timeline page — countdown cards live on the contract management dashboard
- No passed deadlines shown — only upcoming deadlines appear
- No team dashboard integration — cards live on the contract management page only

</boundaries>

<specifics>
## Specific Ideas

- Countdown cards with "Tag Deadline: 12 days" format and green/yellow/red urgency coloring
- Subtle player count hints on each card (e.g., "3 eligible") connecting deadlines to the GM's roster
- Cards replace the existing Phase 12 window status bar — this is an upgrade, not an addition
- Only upcoming deadlines shown — passed deadlines disappear to keep the view clean and actionable

</specifics>

<notes>
## Additional Context

The Phase 9 SeasonCalendar model already stores all the deadline dates. Phase 10 built window status detection. This phase is the visual layer that makes that infrastructure useful to GMs at a glance.

The contract management dashboard (Phase 12) is the home for these cards — that's where GMs go to take action, so deadline context belongs there.

</notes>

---

*Phase: 13-calendar-timeline-deadline-countdowns*
*Context gathered: 2026-03-12*
