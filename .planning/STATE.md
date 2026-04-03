# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-13)

**Core value:** Accurate, automated contract extension calculations (EPV-based) that eliminate manual rule interpretation and spreadsheet formulas
**Current focus:** v1.3 Data Integrity 2 — validate all contract tools against spreadsheet

## Current Position

Phase: 21 of 23 (5YO & PPE) — Plan 01 complete
Plan: 21-01 complete (5YO validation)
Status: Ready for 21-02 (PPE) or Phase 22 (B/R)
Last activity: 2026-04-03 — 21-01 complete (5 code fixes, 22/32 picks match, remaining are scoring data discrepancies)

Progress: ██████░░░░ 50% (v1.3)

## Performance Metrics

**Velocity:**
- Total plans completed: 51
- Average duration: ~10 min
- Total execution time: ~505 min

**By Milestone:**

| Milestone | Phases | Plans | Total Time | Avg/Plan |
|-----------|--------|-------|------------|----------|
| v1.0 MVP | 1-8 | 26 | 142 min | 5 min |
| v1.1 League Calendar | 9-13 | 9 | 81 min | 9 min |
| v1.2 Data Integrity | 14-17 | 8 | ~53 min | ~7 min |
| v1.3 Data Integrity 2 | 18-23 | 8 | ~205 min | ~26 min |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.

### Deferred Issues

- ISS-018-001: RESOLVED — weekly roster scans from 2016 now provide complete data; contracts table used as roster source; salary column widened for R/F auction values

### Key Discoveries

- ADL Cap Percentage: Tag salaries multiply positional averages by (current_cap / prev_cap). Discovered from spreadsheet formula, not in bylaws. May apply to 5YO/PPE too.
- EPV Performance Salary: Uses prior season (End25 Sal) salary rankings × 1.1 growth rate, NOT current season contract table directly. Published salary rankings are a projected snapshot.
- Accrued seasons: conference-scoped, 6-week minimum per season (NFL rule), summed across all conference teams. Weekly roster scans (MFL rosters?W=1..17) are the golden source — end-of-season snapshots miss mid-season drops.
- Spreadsheet eligibility flags are pre-action snapshots. App DB reflects current state (post-action), so already-tendered/extended players correctly show as ineligible.

### Blockers/Concerns

None.

### Roadmap Evolution

- v1.0 MVP shipped: 8 phases (1-8), foundation through functional UI, completed 2026-03-12
- v1.1 League Calendar & Contract Management shipped: 5 phases (9-13), completed 2026-03-13
- v1.2 Data Integrity & Eligibility Accuracy shipped: 4 phases (14-17), completed 2026-03-13
- v1.3 Data Integrity 2 created: cell-by-cell spreadsheet validation for all contract tools, 6 phases (18-23)

## Session Continuity

Last session: 2026-04-03
Stopped at: Phase 21 plan 01 complete — ready for 21-02 (PPE) or Phase 22
Resume file: None
