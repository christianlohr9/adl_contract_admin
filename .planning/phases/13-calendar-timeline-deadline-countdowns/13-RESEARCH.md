# Phase 13: Calendar/Timeline & Deadline Countdowns - Research

**Researched:** 2026-03-12
**Domain:** React countdown cards with urgency color-coding on existing dashboard
**Confidence:** HIGH

<research_summary>
## Summary

Researched the React ecosystem for building deadline countdown cards with urgency color-coding to replace the Phase 12 WindowStatusBar. The existing project stack (shadcn/ui Card + Tailwind + React Query) already provides everything needed — no new dependencies required.

Key finding: This is commodity frontend work, not a niche domain. The countdown libraries (react-countdown, react-timer-hook, react-countdown-circle-timer) are designed for second-level ticking timers (sales countdowns, event timers). This phase needs day-level "12 days remaining" displays that only update on page load — native JavaScript Date arithmetic handles this trivially. Adding a date library like date-fns for a single `differenceInDays` call would be over-engineering.

The urgency color-coding pattern (green/yellow/red traffic light) is a well-established UX convention used in project management dashboards and status systems. Standard thresholds: green (>14 days), yellow (7-14 days), red (≤7 days). These map directly to existing Tailwind color utilities already in use in the WindowStatusBar component.

**Primary recommendation:** Build countdown cards using existing shadcn/ui Card components + Tailwind conditional classes + native Date math. Zero new dependencies. Replace WindowStatusBar inline.
</research_summary>

<standard_stack>
## Standard Stack

### Core (Already Installed — No New Dependencies)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| React | 19.2.0 | Component rendering | Already installed |
| shadcn/ui Card | - | Card layout primitives | Already used in WindowStatusBar |
| Tailwind CSS | 4.2.1 | Conditional urgency colors | Already used for green/yellow/red badges |
| TanStack React Query | - | Data fetching (calendar + eligibility) | Already has `useCalendar()` and `useRosterEligibility()` hooks |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Native Date math | date-fns `differenceInDays` | date-fns adds ~3KB for a single function; `Math.ceil((target - now) / 86400000)` is sufficient for day-level countdown |
| Custom cards | react-countdown | react-countdown is for second-level ticking timers; day-level display doesn't tick |
| Custom cards | react-countdown-circle-timer | SVG circle animation is flashy but mismatches the existing dashboard aesthetic |
| Tailwind classes | CSS-in-JS urgency themes | Tailwind conditional classes are already the project pattern |

### Why No New Dependencies
The countdown card requirement is:
1. Calculate days between now and a deadline date → native `Date` arithmetic
2. Map day count to urgency color → conditional Tailwind classes
3. Display in a card layout → existing shadcn/ui Card component
4. Get deadline data → existing `useCalendar()` hook

None of these require a library the project doesn't already have.
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Approach
Replace `WindowStatusBar` component with a `DeadlineCountdown` component in the same location on `ContractManagementPage`. No structural changes to page layout.

```
frontend/src/components/contracts/
├── DeadlineCountdown.tsx        # New: replaces WindowStatusBar.tsx
├── useDeadlineCards.ts          # New: hook to compute card data from calendar + eligibility
├── WindowStatusBar.tsx          # Removed (replaced)
├── useEligibilityTable.ts       # Existing (unchanged)
└── eligibility-columns.tsx      # Existing (unchanged)
```

### Pattern 1: Derive Countdown Data from Existing Sources
**What:** Combine `SeasonCalendarSchema` deadline dates with `WindowStatusSchema` status and eligible player counts from `RosterEligibilitySchema` into a single card data model
**When to use:** When multiple data sources need to merge into a single UI component
**Example:**
```typescript
interface DeadlineCard {
  action: string;
  label: string;
  daysRemaining: number;
  urgency: "green" | "yellow" | "red";
  closesDate: string;
  eligibleCount: number;
}

function getDaysRemaining(dateStr: string): number {
  const now = new Date();
  const target = new Date(dateStr);
  // Reset time portions for day-level accuracy
  now.setHours(0, 0, 0, 0);
  target.setHours(0, 0, 0, 0);
  return Math.ceil((target.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
}

function getUrgency(days: number): "green" | "yellow" | "red" {
  if (days <= 7) return "red";
  if (days <= 14) return "yellow";
  return "green";
}
```

### Pattern 2: Urgency Color Mapping with Tailwind
**What:** Map urgency levels to Tailwind color classes, following existing project patterns
**When to use:** Conditional color-coding in cards/badges
**Example:**
```typescript
const urgencyStyles = {
  green: {
    border: "border-green-200 dark:border-green-800",
    bg: "bg-green-50 dark:bg-green-950",
    text: "text-green-700 dark:text-green-300",
    badge: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  },
  yellow: {
    border: "border-yellow-200 dark:border-yellow-800",
    bg: "bg-yellow-50 dark:bg-yellow-950",
    text: "text-yellow-700 dark:text-yellow-300",
    badge: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  },
  red: {
    border: "border-red-200 dark:border-red-800",
    bg: "bg-red-50 dark:bg-red-950",
    text: "text-red-700 dark:text-red-300",
    badge: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
  },
} as const;
```

### Pattern 3: Filter to Upcoming Only
**What:** Only show cards for deadlines that are in the future and have a configured date
**When to use:** Keeping the dashboard clean and actionable per CONTEXT.md requirements
**Example:**
```typescript
// Filter: only upcoming deadlines with configured dates
const cards = allDeadlines
  .filter((d) => d.closesDate !== null)
  .map((d) => ({ ...d, daysRemaining: getDaysRemaining(d.closesDate) }))
  .filter((d) => d.daysRemaining > 0) // Only future deadlines
  .sort((a, b) => a.daysRemaining - b.daysRemaining); // Most urgent first
```

### Anti-Patterns to Avoid
- **Real-time ticking for day-level display:** Don't use `setInterval` to update a "12 days" counter — it only changes at midnight. Recalculate on component mount/data fetch is sufficient.
- **Fetching calendar data separately:** The calendar dates can be derived from the existing `useCalendar()` hook + eligibility data. Don't create a new API endpoint.
- **Keeping WindowStatusBar alongside countdown cards:** CONTEXT.md explicitly says cards *replace* the status bar, not supplement it.
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Card layout | Custom div grid | shadcn/ui Card components | Already in use, consistent with project |
| Color theming | Custom CSS variables | Tailwind utility classes | Project uses Tailwind throughout |
| Data fetching | New API endpoint | Existing `useCalendar()` + `useRosterEligibility()` | All needed data already available |
| Badge urgency labels | Custom styled spans | Existing Badge component with urgency variants | Consistent with existing WindowStatusBar pattern |

**Key insight:** This phase is purely a UI component swap. All backend data (calendar deadlines, window statuses, eligible player counts) already exists from Phases 9-12. The only work is reshaping existing data into a more useful visual format.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Timezone-Sensitive Date Math
**What goes wrong:** `new Date("2026-06-15")` is parsed as midnight UTC, which in US timezones shows as the previous day. A deadline that's "15 days away" might display as "14 days."
**Why it happens:** ISO date strings without time/timezone are parsed as UTC by the Date constructor.
**How to avoid:** Either append `T00:00:00` to force local parsing, or normalize both dates to midnight before computing the difference: `date.setHours(0,0,0,0)`.
**Warning signs:** Off-by-one errors in day counts, especially around midnight.

### Pitfall 2: Stale Countdown After Midnight
**What goes wrong:** If a GM leaves the dashboard open overnight, day counts don't update until page refresh.
**Why it happens:** The component renders once and the day calculation is based on the initial `new Date()`.
**How to avoid:** React Query's `staleTime` (5 min) handles this — data refetches naturally. No `setInterval` needed. For extra safety, key the countdown calculation to the current date string so React re-renders on day change when data refetches.
**Warning signs:** A card shows "1 day" when the deadline was yesterday.

### Pitfall 3: Empty State When No Deadlines Configured
**What goes wrong:** If the SeasonCalendar has no dates set (all null), the countdown area shows nothing — GM thinks the page is broken.
**Why it happens:** Filtering out null dates and past dates leaves an empty array.
**How to avoid:** Show an explicit "No upcoming deadlines configured" message when the card list is empty, distinct from the loading state.
**Warning signs:** Blank space where cards should be.

### Pitfall 4: Eligible Count Mismatch
**What goes wrong:** Card shows "3 eligible" but the data table below shows different count.
**Why it happens:** Eligible count derived differently than the table's eligibleOnly filter.
**How to avoid:** Derive both from the same `action_groups` data in `RosterEligibilitySchema`. The card count should match the number of rows that would appear in the table for that action type.
**Warning signs:** Numbers don't match between cards and table.
</common_pitfalls>

<code_examples>
## Code Examples

### Day-Level Countdown Calculation
```typescript
// Native Date — no library needed
function getDaysRemaining(dateStr: string): number {
  const now = new Date();
  const target = new Date(dateStr + "T00:00:00"); // Force local timezone
  now.setHours(0, 0, 0, 0);
  return Math.ceil((target.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));
}
```

### Mapping Calendar Deadlines to Card Data
```typescript
// Map SeasonCalendarSchema fields to action-specific deadlines
const DEADLINE_MAP: Record<string, { label: string; field: keyof SeasonCalendarSchema }> = {
  extension: { label: "Extension Deadline", field: "oext_deadline" },
  franchise_tag: { label: "Tag Deadline", field: "tag_deadline" },
  erfa_tender: { label: "Tender Deadline", field: "tender_deadline" },
  rfa_tender: { label: "Tender Deadline", field: "tender_deadline" },
  buyout_restructure: { label: "B/R Deadline", field: "br_deadline" },
  fifth_year_option: { label: "5YO Deadline", field: "fyo_deadline" },
};
```

### Countdown Card Component Structure
```tsx
// Follows existing shadcn/ui Card pattern from WindowStatusBar
function DeadlineCard({ card }: { card: DeadlineCard }) {
  const styles = urgencyStyles[card.urgency];
  return (
    <div className={cn("rounded-lg border p-4", styles.border, styles.bg)}>
      <div className="flex items-baseline justify-between">
        <span className="text-sm font-medium">{card.label}</span>
        <Badge className={styles.badge}>
          {card.daysRemaining}d
        </Badge>
      </div>
      <div className={cn("text-2xl font-bold mt-1", styles.text)}>
        {card.daysRemaining} {card.daysRemaining === 1 ? "day" : "days"}
      </div>
      {card.eligibleCount > 0 && (
        <span className="text-xs text-muted-foreground mt-1">
          {card.eligibleCount} eligible
        </span>
      )}
    </div>
  );
}
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| moment.js for date math | Native Date or date-fns v3 | 2020+ | moment.js is deprecated; native Date handles day-level math fine |
| CSS animations for urgency | Tailwind conditional classes | Ongoing | Static color coding preferred over animated urgency in dashboards (less distracting) |
| Separate countdown timer libraries | Built into component | Ongoing | Day-level countdowns don't justify a library; second-level timers do |

**New tools/patterns to consider:**
- **Temporal API:** Stage 3 TC39 proposal for better date handling. Not yet available in browsers. Not relevant for this simple use case.
- **CSS `color-mix()`:** Could be used for urgency gradients but Tailwind utility classes are simpler and match project patterns.

**Deprecated/outdated:**
- **moment.js:** Deprecated, massive bundle size. Not relevant here since no date library needed.
- **react-countdown for day-level displays:** Overkill — designed for second-level ticking, not "12 days remaining" static cards.
</sota_updates>

<open_questions>
## Open Questions

1. **Urgency threshold tuning**
   - What we know: Standard thresholds are green (>14d), yellow (7-14d), red (≤7d)
   - What's unclear: Whether ADL GMs would prefer different thresholds (e.g., yellow at 10 days instead of 14)
   - Recommendation: Start with standard thresholds, can be easily adjusted as constants. No need to make configurable.

2. **In-season extension window display**
   - What we know: iEXT has a start AND end date (a window), not just a deadline
   - What's unclear: Should the card show "starts in X days" before the window opens AND "closes in X days" once open?
   - Recommendation: Show "opens in X days" before window, "X days remaining" during window. Both are countdowns to action-relevant dates.

3. **PPE display**
   - What we know: PPE is always-open per Phase 10 decision (no deadline window)
   - What's unclear: Should PPE appear as a card at all? It has no countdown.
   - Recommendation: Omit PPE from countdown cards — it has no deadline to count down to. Its always-available status is implied by its presence in the data table.
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- Existing codebase: `WindowStatusBar.tsx`, `ContractManagementPage.tsx`, `calendar.ts`, `eligibility.ts`, `types.ts` — current implementation patterns
- Phase 13 CONTEXT.md — explicit scope (countdown cards, no timeline page, replaces status bar)
- Phase 9-12 decisions in STATE.md — calendar model, window status service, eligibility API

### Secondary (MEDIUM confidence)
- [Astro UX Design System - Status System](https://www.astrouxds.com/patterns/status-system/) — green/yellow/red urgency convention
- [Carbon Design System - Status Indicators](https://carbondesignsystem.com/patterns/status-indicator-pattern/) — traffic light color patterns
- [date-fns documentation](https://github.com/date-fns/date-fns) via Context7 — confirmed day-level math doesn't need a library
- [You might not need date-fns](https://dev.to/dmtrkovalenko/you-might-not-need-date-fns-23f7) — native Date sufficient for simple calculations

### Tertiary (LOW confidence - needs validation)
- None — all findings verified against codebase and official design system docs
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: React + shadcn/ui + Tailwind (existing stack)
- Ecosystem: Evaluated countdown timer libraries (react-countdown, react-timer-hook, react-countdown-circle-timer), date libraries (date-fns, dayjs), animation libraries — none needed
- Patterns: Urgency color-coding, countdown card layout, deadline-to-card data mapping
- Pitfalls: Timezone date math, stale counts, empty states, count mismatches

**Confidence breakdown:**
- Standard stack: HIGH — no new dependencies, verified existing stack covers all needs
- Architecture: HIGH — simple component replacement, data sources already exist
- Pitfalls: HIGH — timezone and off-by-one issues well-documented in JS ecosystem
- Code examples: HIGH — based on existing project patterns and native JS APIs

**Research date:** 2026-03-12
**Valid until:** 2026-04-12 (30 days — no external dependencies to go stale)
</metadata>

---

*Phase: 13-calendar-timeline-deadline-countdowns*
*Research completed: 2026-03-12*
*Ready for planning: yes*
