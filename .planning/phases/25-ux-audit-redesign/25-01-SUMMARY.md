---
phase: 25-ux-audit-redesign
plan: 01
subsystem: ui
tags: [tailwind, css-custom-properties, oklch, dark-theme, nfl-teams]

requires:
  - phase: 24-repo-polish
    provides: clean repo baseline
provides:
  - Dark-only Sleeper-inspired base theme via CSS custom properties
  - 32 NFL team accent color overrides via [data-team] selectors
  - useTeamSelection hook with localStorage persistence
  - restoreTeamTheme() for pre-mount theme application
affects: [25-ux-audit-redesign]

tech-stack:
  added: []
  patterns: [data-team attribute theming, OKLCH color space for team accents, pre-mount theme restore]

key-files:
  created: [frontend/src/hooks/useTeamSelection.ts]
  modified: [frontend/src/index.css, frontend/src/main.tsx]

key-decisions:
  - "Dark-mode only — removed :root light block entirely"
  - "OKLCH lightness >= 0.45 for dark-bg problem teams (16 teams boosted or using secondary color)"
  - "4 CSS variables per team max: --primary, --primary-foreground, --ring, --sidebar-primary"
  - "Pre-mount restoreTeamTheme() prevents flash of unthemed content"

patterns-established:
  - "Team theming: [data-team=XXX] selector on documentElement overrides accent variables"
  - "Theme persistence: localStorage key adl-selected-team with {id, abbr} JSON"

issues-created: []

duration: 8min
completed: 2026-04-04
---

# Phase 25-01: Dark Base Theme + Team Color System Summary

**Sleeper-inspired dark-only base theme with 32 NFL team accent color overrides via OKLCH CSS custom properties and localStorage-persistent team selection hook**

## Performance

- **Duration:** 8 min
- **Started:** 2026-04-04
- **Completed:** 2026-04-04
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Replaced light/dark dual theme with Sleeper-inspired dark-only base (deeper blacks, layered card surfaces)
- Added all 32 NFL team accent colors as [data-team] CSS variable overrides with OKLCH lightness adjustments for dark backgrounds
- Created useTeamSelection hook with selectTeam/clearTeam/selectedTeam and localStorage persistence
- Added restoreTeamTheme() pre-mount call to prevent flash of unthemed content

## Task Commits

Each task was committed atomically:

1. **Task 1: Dark base theme + 32 team accent color overrides** - `6a36e0e` (feat)
2. **Task 2: Team selection hook with localStorage persistence** - `78f10d1` (feat)

## Files Created/Modified
- `frontend/src/index.css` - Dark base theme + 32 [data-team] accent overrides, removed light :root block
- `frontend/src/hooks/useTeamSelection.ts` - Hook for team selection with localStorage persistence
- `frontend/src/main.tsx` - Pre-mount dark class + restoreTeamTheme() call

## Decisions Made
- Removed :root light mode block entirely — app is dark-only per design spec
- Used OKLCH color space with lightness >= 0.45 for 16 dark-bg problem teams
- Limited to 4 CSS variable overrides per team to keep CSS manageable
- CHI, HOU, NE use secondary (red) color since primary is near-black
- GB, PIT, NO, WAS use gold/secondary as primary accent (bright on dark)
- SEA uses Action Green secondary instead of dark navy primary

## Deviations from Plan
None - plan executed exactly as written

## Issues Encountered
- Pre-existing `tsc -b` error in `useEligibilityTable.ts` (type cast issue) — not introduced by this plan, `tsc --noEmit` passes clean

## Next Phase Readiness
- Dark theme foundation in place for all subsequent UI work
- Team color system ready — any component using shadcn variables automatically inherits team branding
- Next plan should build splash screen team picker or dashboard layout redesign

---
*Phase: 25-ux-audit-redesign*
*Completed: 2026-04-04*
