# Phase 16: NFL Kickoff Rule - Context

**Gathered:** 2026-03-13
**Status:** Ready for research

<vision>
## How This Should Work

The NFL kickoff rule is a nuanced eligibility gate — not a blanket block. Some contract actions are genuinely gated by whether the NFL season has started (per the bylaws), while others like in-season extensions remain available regardless. The system needs to know which actions depend on kickoff and which don't.

When a GM looks at a player whose action IS blocked by the kickoff rule, the action appears greyed out with a clear reason — something like "Available after NFL Week 1 kickoff (Sep 4)" showing exactly when it unlocks. GMs can see what's coming and plan ahead, but can't act until the season starts.

The kickoff date lives in the existing league calendar system (from Phase 9) as a calendar entry. An admin sets it once per year. No auto-fetching of NFL schedules needed.

</vision>

<essential>
## What Must Be Nailed

- **Correct action mapping** — Getting the bylaw logic right is the core value. Exactly which contract actions are kickoff-gated vs. which aren't must match the bylaws precisely. Only bylaw-specified actions are blocked — everything else stays available year-round.
- **Clear messaging** — When something IS blocked, the reason needs to be crystal clear. GMs should never be confused about WHY an action is unavailable. Confusing messaging undermines trust in the system more than any other failure mode.

</essential>

<boundaries>
## What's Out of Scope

- Auto-updating/auto-fetching NFL schedules — manually setting the kickoff date each year via league calendar is fine
- Game-level granularity — we only care about "has the NFL season started," not individual game times, bye weeks, or weekly matchups
- Blocking ALL contract actions — only the ones the bylaws explicitly specify as requiring the season to start

</boundaries>

<specifics>
## Specific Ideas

- Use the existing league calendar system (Phase 9) to store the NFL kickoff date as a calendar entry/period
- Greyed-out UI treatment for blocked actions with the specific unlock date displayed
- Eligibility engine already has period-aware logic from Phase 10 — kickoff rule should plug into that pattern

</specifics>

<notes>
## Additional Context

This resolves UAT-001 from Phase 4 which identified the missing NFL kickoff eligibility rule. The user's primary concern is clarity of messaging — GMs understanding WHY something is blocked matters more than the blocking itself. False blocks and false allows are secondary concerns to confusing UX.

The user sees this as a surgical addition: the eligibility system already works, this just adds one more condition that certain actions check against.

</notes>

---

*Phase: 16-nfl-kickoff-rule*
*Context gathered: 2026-03-13*
