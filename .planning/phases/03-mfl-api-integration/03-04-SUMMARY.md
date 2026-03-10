---
phase: 03-mfl-api-integration
plan: 04
subsystem: api, infra
tags: [apscheduler, fastapi, background-tasks, sync, scheduling]

# Dependency graph
requires:
  - phase: 03-03
    provides: roster_sync, contract_sync, score_sync services
  - phase: 03-02
    provides: team_sync, player_sync services
  - phase: 03-01
    provides: MFLClient HTTP client
provides:
  - Background sync scheduler (APScheduler 4.x)
  - Sync orchestrator with ordered execution
  - Manual sync trigger API endpoints
  - Sync status reporting endpoint
affects: [04-contract-engine, 06-api-layer]

# Tech tracking
tech-stack:
  added: [apscheduler 4.0.0a6]
  patterns: [APScheduler AsyncScheduler in FastAPI lifespan, BackgroundTasks for manual triggers, in-memory sync status tracking]

key-files:
  created:
    - src/app/services/sync_orchestrator.py
    - src/app/schemas/sync.py
    - src/app/api/sync.py
  modified:
    - pyproject.toml
    - uv.lock
    - src/app/core/config.py
    - src/app/main.py

key-decisions:
  - "APScheduler 4.x alpha (>=4.0.0a1) for AsyncScheduler support"
  - "BackgroundTasks for manual trigger (vs running inline)"
  - "Single transaction for full sync atomicity"
  - "No sync on startup — only on schedule or manual trigger"

patterns-established:
  - "Sync orchestrator pattern: ordered service execution in single transaction"
  - "API router pattern: prefix=/api/{resource}, tags=[resource]"

issues-created: []

# Metrics
duration: 4min
completed: 2026-03-10
---

# Phase 3 Plan 4: Scheduler & Sync Orchestration Summary

**APScheduler 4.x background sync with ordered orchestration (teams→players→rosters→scores) and manual trigger/status REST endpoints**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-10T15:25:00Z
- **Completed:** 2026-03-10T15:29:45Z
- **Tasks:** 2
- **Files modified:** 7

## Accomplishments
- Sync orchestrator with enforced execution order (teams → players → rosters → scores) in single atomic transaction
- APScheduler 4.x AsyncScheduler integrated into FastAPI lifespan with configurable interval (default 6h)
- REST API: GET /api/sync/status, POST /api/sync/trigger, POST /api/sync/historical
- In-memory sync status tracking (last_sync, in_progress, last_result, last_error)

## Task Commits

Each task was committed atomically:

1. **Task 1: Create sync orchestrator and APScheduler integration** - `40f8845` (feat)
2. **Task 2: Create sync API endpoint with status reporting** - `f52f1e6` (feat)

## Files Created/Modified
- `src/app/services/sync_orchestrator.py` - Orchestrates all sync services in order, manages status
- `src/app/schemas/sync.py` - Pydantic response models for sync endpoints
- `src/app/api/sync.py` - FastAPI router with status, trigger, and historical endpoints
- `pyproject.toml` - Added apscheduler>=4.0.0a1 dependency
- `uv.lock` - Updated lock file
- `src/app/core/config.py` - Added sync_interval_hours, sync_enabled, sync_historical_years settings
- `src/app/main.py` - APScheduler lifespan integration + sync router registration

## Decisions Made
- Used APScheduler 4.x alpha (>=4.0.0a1) to get AsyncScheduler — v3.x lacks async support
- BackgroundTasks for manual trigger (non-blocking 202 response) vs running sync inline
- Single transaction wrapping all four sync steps for atomicity
- No sync on startup — only on schedule interval or manual trigger

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] APScheduler version pinning**
- **Found during:** Task 1 (APScheduler installation)
- **Issue:** `uv add apscheduler` installed v3.x which lacks AsyncScheduler
- **Fix:** Specified `apscheduler>=4.0.0a1` to get 4.x alpha as intended by research
- **Verification:** Import of `from apscheduler import AsyncScheduler` succeeds

---

**Total deviations:** 1 auto-fixed (1 blocking), 0 deferred
**Impact on plan:** Minor version pinning fix, no scope change.

## Issues Encountered
None

## Next Phase Readiness
- Phase 3 complete — all 4 plans executed
- MFL data can be synced into all domain tables (teams, players, rosters, contracts, scores)
- Background sync runs automatically on configurable interval
- Manual sync available via REST API
- Ready for Phase 4 (Contract Engine) — models populated with real MFL data

---
*Phase: 03-mfl-api-integration*
*Completed: 2026-03-10*
