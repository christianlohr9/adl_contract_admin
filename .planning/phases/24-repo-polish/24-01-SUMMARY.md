---
phase: 24-repo-polish
plan: 01
subsystem: codebase
tags: [cleanup, comments, dependencies, dead-code]

# Dependency graph
requires:
  - phase: 23-cross-tool-validation
    provides: stable codebase with all features complete
provides:
  - Comment-free professional codebase (no separator slop)
  - Pruned dependency tree (openpyxl, pytest-httpx, zustand removed)
  - Clean imports (no unused imports)
affects: [24-repo-polish]

# Tech tracking
tech-stack:
  added: []
  patterns: []

key-files:
  created: []
  modified:
    - src/app/services/*.py (16 service files cleaned)
    - src/app/api/tools.py
    - src/app/schemas/tools.py
    - src/app/schemas/cap.py
    - frontend/src/api/types.ts
    - src/app/api/teams.py
    - pyproject.toml
    - frontend/package.json

key-decisions:
  - "Kept section header comments that provide organizational value, only removed separator lines"
  - "Left scripts/validate_*.py in place as historical artifacts even though openpyxl dep removed"

patterns-established: []

issues-created: []

# Metrics
duration: 3min
completed: 2026-04-04
---

# Phase 24-01: Code & Dependency Cleanup Summary

**Stripped separator comment slop from 18 files and removed 4 unused imports/dependencies**

## Performance

- **Duration:** 3 min
- **Started:** 2026-04-04
- **Completed:** 2026-04-04
- **Tasks:** 2
- **Files modified:** 23

## Accomplishments
- Removed all `# ---...---` separator comment lines from 18 Python/TypeScript files
- Removed redundant docstrings that merely restated function names
- Removed unused `Player` import from teams.py
- Purged 3 unused dependencies: openpyxl, pytest-httpx, zustand

## Task Commits

Each task was committed atomically:

1. **Task 1: Strip comment slop from Python and TypeScript files** - `063bf6e` (chore)
2. **Task 2: Remove unused imports and dependencies** - `1d52ea6` (chore)

## Files Created/Modified
- `src/app/services/*.py` (16 files) - Separator comments and redundant docstrings removed
- `src/app/api/tools.py` - Separator comments removed (found during scan)
- `src/app/schemas/tools.py` - Separator comments removed (found during scan)
- `src/app/schemas/cap.py` - Separator comments removed (found during scan)
- `frontend/src/api/types.ts` - Separator comments removed
- `src/app/api/teams.py` - Unused Player import removed
- `pyproject.toml` - openpyxl and pytest-httpx removed
- `uv.lock` - Regenerated after dep removal
- `frontend/package.json` - zustand removed
- `frontend/package-lock.json` - Regenerated after dep removal

## Decisions Made
- Kept section header comments (e.g., "# RFA tender calculations") when they provide genuine organizational value; only the dash-separator lines were removed
- Left scripts/validate_*.py in place as historical artifacts even though their openpyxl dependency was removed from pyproject.toml

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Auto-fix] Additional files with separator patterns**
- **Found during:** Task 1 (comment slop scan)
- **Issue:** 3 files not in the original list also had separator comment patterns: `src/app/api/tools.py`, `src/app/schemas/tools.py`, `src/app/schemas/cap.py`
- **Fix:** Cleaned them using the same rules
- **Files modified:** src/app/api/tools.py, src/app/schemas/tools.py, src/app/schemas/cap.py
- **Verification:** ruff check passes, no code changes
- **Committed in:** 063bf6e (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (additional files cleaned), 0 deferred
**Impact on plan:** Minor scope expansion to clean 3 additional files. No scope creep.

## Issues Encountered
None

## Next Phase Readiness
- Codebase is clean of comment slop and unused dependencies
- Ready for plan 24-02 (if exists) or Phase 25 UX Audit

---
*Phase: 24-repo-polish*
*Completed: 2026-04-04*
