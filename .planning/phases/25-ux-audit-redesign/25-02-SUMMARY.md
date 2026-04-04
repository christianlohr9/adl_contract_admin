---
phase: 25-ux-audit-redesign
plan: 02
subsystem: ui
tags: [react, react-router, splash-screen, team-picker, sidebar, theming]

requires:
  - phase: 25-ux-audit-redesign
    provides: dark theme + 32 team accent color CSS overrides + useTeamSelection hook
provides:
  - Full-screen splash page team picker at /
  - Team-branded sidebar with switch team button
  - Route restructure: dashboard at /dashboard, splash at /
  - Team guard: redirect to splash if no team selected
affects: [25-ux-audit-redesign, 26-production-configuration]

tech-stack:
  added: []
  patterns: [splash-page-as-entry-point, synchronous-localstorage-init, team-guard-redirect]

key-files:
  created: [frontend/src/pages/SplashPage.tsx, frontend/src/lib/teams.ts]
  modified: [frontend/src/App.tsx, frontend/src/components/layout/AppSidebar.tsx, frontend/src/components/layout/AppLayout.tsx, frontend/src/hooks/useTeamSelection.ts]

key-decisions:
  - "NFL abbreviation mapping via static name→abbr lookup (teams use NFL names in DB)"
  - "Team colors duplicated as inline styles for splash badges (avoids setting global data-team)"
  - "Synchronous localStorage init to prevent redirect race condition"

patterns-established:
  - "Route guard: AppLayout redirects to splash if no team selected"
  - "Splash page: fetch teams from API, map to NFL abbreviations via static lookup"

issues-created: []

duration: 12min
completed: 2026-04-04
---

# Phase 25-02: Splash Screen Team Picker & Sidebar Branding Summary

**Full-screen 32-team splash picker as app entry point with team-branded sidebar and route restructure**

## Performance

- **Duration:** 12 min
- **Started:** 2026-04-04
- **Completed:** 2026-04-04
- **Tasks:** 2 (+ 1 checkpoint)
- **Files modified:** 7

## Accomplishments
- Full-screen dark splash page with ADL shield logo and 32 color-coded team badges
- Route restructure: splash at `/`, dashboard moved to `/dashboard`
- Sidebar shows selected team abbreviation with accent color and "Switch Team" button
- Team guard prevents accessing app without team selection
- Return visitors auto-redirect to dashboard via synchronous localStorage read

## Task Commits

Each task was committed atomically:

1. **Task 1: Create SplashPage with team picker grid** - `c0a1db0` (feat)
2. **Task 2: Update routing + sidebar team branding** - `fdbb203` (feat)
3. **Checkpoint fixes: redirect loop + lockfile** - `f23ce06` (fix)

## Files Created/Modified
- `frontend/src/lib/teams.ts` - NFL name→abbreviation and abbreviation→OKLCH color maps
- `frontend/src/pages/SplashPage.tsx` - Full-screen team picker with 32 badges
- `frontend/src/App.tsx` - Route restructure: splash at /, app routes under AppLayout
- `frontend/src/components/layout/AppSidebar.tsx` - Team abbreviation display + Switch Team button
- `frontend/src/components/layout/AppLayout.tsx` - Team guard redirect
- `frontend/src/hooks/useTeamSelection.ts` - Synchronous localStorage init (fixed race condition)
- `frontend/pnpm-lock.yaml` - Removed stale zustand reference

## Decisions Made
- Used static name→abbreviation mapping since backend stores NFL team names directly
- Duplicated OKLCH colors as inline styles for splash badges to avoid setting global data-team on hover
- Switched useTeamSelection from async useEffect init to synchronous localStorage read to prevent redirect loop

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Redirect loop on team selection**
- **Found during:** Checkpoint verification
- **Issue:** useTeamSelection initialized as null via useEffect, causing AppLayout to redirect to / before localStorage was read
- **Fix:** Changed to synchronous localStorage initialization, removed duplicate navigate call
- **Files modified:** frontend/src/hooks/useTeamSelection.ts, frontend/src/pages/SplashPage.tsx
- **Verification:** Team selection navigates correctly, no console errors
- **Committed in:** f23ce06

**2. [Rule 3 - Blocking] Stale pnpm-lock.yaml**
- **Found during:** Docker build
- **Issue:** Lockfile referenced removed zustand dependency, breaking frozen-lockfile install
- **Fix:** Ran pnpm install to regenerate lockfile
- **Files modified:** frontend/pnpm-lock.yaml
- **Verification:** Docker build succeeds
- **Committed in:** f23ce06

**3. [Rule 3 - Blocking] .gitignore too broad**
- **Found during:** Task 1 (git add)
- **Issue:** `lib/` pattern ignored frontend/src/lib/ directory
- **Fix:** Changed to `/lib/` to only match root-level lib
- **Files modified:** .gitignore
- **Committed in:** f23ce06

---

**Total deviations:** 3 auto-fixed (all blocking), 0 deferred
**Impact on plan:** All fixes necessary for correctness. No scope creep.

## Issues Encountered
None beyond the auto-fixed deviations above.

## Next Phase Readiness
- Splash screen and team branding complete
- Ready for Phase 25 Plan 03 (dashboard layout redesign or contract tool UX)
- All existing routes functional under new structure

---
*Phase: 25-ux-audit-redesign*
*Completed: 2026-04-04*
