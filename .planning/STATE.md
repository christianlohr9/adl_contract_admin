# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-04)

**Core value:** Accurate, automated contract extension calculations (EPV-based) that eliminate manual rule interpretation and spreadsheet formulas
**Current focus:** v1.4 Polish & Deploy — repo cleanup, UX redesign, production deployment

## Current Position

Phase: 26 of 27 (Production Configuration) — COMPLETE
Plan: 01 complete (phase complete)
Status: Ready for Phase 27 (No-Cost Deployment)
Last activity: 2026-04-04 — Plan 26-01 executed (env config, CORS, production docker-compose)

Progress: ██████░░░░ 60% (v1.4)

## Performance Metrics

**Velocity:**
- Total plans completed: 60
- Average duration: ~10 min
- Total execution time: ~586 min

**By Milestone:**

| Milestone | Phases | Plans | Total Time | Avg/Plan |
|-----------|--------|-------|------------|----------|
| v1.0 MVP | 1-8 | 26 | 142 min | 5 min |
| v1.1 League Calendar | 9-13 | 9 | 81 min | 9 min |
| v1.2 Data Integrity | 14-17 | 8 | ~53 min | ~7 min |
| v1.3 Data Integrity 2 | 18-23 | 10 | ~273 min | ~25 min |

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.

### Deferred Issues

None.

### Key Discoveries

- ADL Cap Percentage: Tag salaries multiply positional averages by (current_cap / prev_cap). Discovered from spreadsheet formula, not in bylaws. Confirmed applies to 5YO modified TT (21-01) but NOT PPE (uses raw NFL tag prices).
- PPE price = raw NFL RFA tag price (SRFA or ORFA), NOT the tender MAX formula. Bylaws say "the SRFA/ORFA tag price."
- PPE below-floor exclusion: players ranked below PR Starter Floor get NO escalation. Bylaws say "above his PR Starter Floor."
- EPV Performance Salary: Uses prior season (End25 Sal) salary rankings × 1.1 growth rate, NOT current season contract table directly. Published salary rankings are a projected snapshot.
- Accrued seasons: conference-scoped, 6-week minimum per season (NFL rule), summed across all conference teams. Weekly roster scans (MFL rosters?W=1..17) are the golden source — end-of-season snapshots miss mid-season drops.
- Spreadsheet eligibility flags are pre-action snapshots. App DB reflects current state (post-action), so already-tendered/extended players correctly show as ineligible.

### Blockers/Concerns

None.

### Roadmap Evolution

- v1.0 MVP shipped: 8 phases (1-8), foundation through functional UI, completed 2026-03-12
- v1.1 League Calendar & Contract Management shipped: 5 phases (9-13), completed 2026-03-13
- v1.2 Data Integrity & Eligibility Accuracy shipped: 4 phases (14-17), completed 2026-03-13
- v1.3 Data Integrity 2 shipped: cell-by-cell spreadsheet validation for all contract tools, 6 phases (18-23), completed 2026-04-04. 99.7% match rate, spreadsheet declared redundant.
- v1.4 Polish & Deploy created: repo cleanup, UX redesign for commissioner buy-in, no-cost deployment, 4 phases (24-27)

## Session Continuity

Last session: 2026-04-04
Stopped at: Phase 26 complete, ready for Phase 27 (No-Cost Deployment)
Resume file: None
