---
phase: 02-foundation
plan: 03
subsystem: database
tags: [sqlalchemy, alembic, asyncpg, postgresql, models, migrations]

# Dependency graph
requires:
  - phase: 02-foundation (02-01, 02-02)
    provides: project scaffold, async DB module, Docker Compose, FastAPI app
  - phase: 01-rules-extraction
    provides: rule constants/formulas informing schema design
provides:
  - 7 SQLAlchemy domain models (Team, Player, Contract, RosterEntry, SalaryCapSnapshot, DraftPick, Transaction)
  - Alembic async migration infrastructure
  - Initial migration creating all tables
affects: [03-mfl-api-integration, 04-contract-engine, 05-salary-cap, 06-api-layer]

# Tech tracking
tech-stack:
  added: [alembic]
  patterns: [AsyncAttrs + DeclarativeBase, Mapped[] + mapped_column(), StrEnum for constrained fields, Numeric for money, JSONB for flexible details]

key-files:
  created:
    - src/app/models/base.py
    - src/app/models/team.py
    - src/app/models/player.py
    - src/app/models/contract.py
    - src/app/models/roster.py
    - src/app/models/salary_cap.py
    - src/app/models/draft_pick.py
    - src/app/models/transaction.py
    - src/app/models/__init__.py
    - alembic.ini
    - migrations/env.py
    - migrations/script.py.mako
    - migrations/versions/827bb2529fb6_initial_schema.py
  modified:
    - docker-compose.yml
    - Dockerfile
    - pyproject.toml

key-decisions:
  - "Used StrEnum (Python 3.11+) for ContractType, ContractStatus, RosterStatus, TransactionType"
  - "JSONB for transaction details — flexible storage for varied transaction types"
  - "Numeric(5,2) for salary, Numeric(6,2) for cap totals — avoids float rounding"
  - "Docker web command runs alembic upgrade head before uvicorn startup"

patterns-established:
  - "SQLAlchemy 2.0 style: Mapped[], mapped_column(), relationship() with type annotations"
  - "TimestampMixin for id/created_at/updated_at on all models"
  - "StrEnum for constrained string columns"
  - "Async Alembic env.py with settings-based URL"

issues-created: []

# Metrics
duration: 6 min
completed: 2026-03-10
---

# Phase 2 Plan 3: SQLAlchemy Models and Alembic Migrations Summary

**7 normalized domain models (Team, Player, Contract, RosterEntry, SalaryCapSnapshot, DraftPick, Transaction) with async Alembic migrations, StrEnum types, and Numeric money fields**

## Performance

- **Duration:** 6 min
- **Started:** 2026-03-10T13:51:58Z
- **Completed:** 2026-03-10T13:58:28Z
- **Tasks:** 2
- **Files modified:** 18

## Accomplishments
- Created 7 domain models with proper relationships, unique constraints, and indexes reflecting ADL bylaws structure
- Used SQLAlchemy 2.0 style throughout (Mapped[], mapped_column(), AsyncAttrs + DeclarativeBase)
- Initialized async Alembic with auto-migration, generated and applied initial schema creating all 7 tables
- Docker Compose now runs migrations automatically on startup

## Task Commits

Each task was committed atomically:

1. **Task 1: Create SQLAlchemy models** - `7fd929c` (feat)
2. **Task 2: Initialize Alembic and generate initial migration** - `2a188e7` (feat)

**Plan metadata:** `d173a81` (docs: complete plan)

## Files Created/Modified
- `src/app/models/base.py` - Base class with AsyncAttrs + DeclarativeBase, TimestampMixin
- `src/app/models/team.py` - Team model (franchise_id, name, conference, division)
- `src/app/models/player.py` - Player model (mfl_id, name, position, nfl_team, draft info)
- `src/app/models/contract.py` - Contract model with ContractType/ContractStatus enums, Numeric salary
- `src/app/models/roster.py` - RosterEntry model with RosterStatus enum
- `src/app/models/salary_cap.py` - SalaryCapSnapshot with Numeric money fields
- `src/app/models/draft_pick.py` - DraftPick with original/current team FKs
- `src/app/models/transaction.py` - Transaction with TransactionType enum, JSONB details
- `src/app/models/__init__.py` - Imports all models for Alembic discovery
- `alembic.ini` - Alembic config with prepend_sys_path=src
- `migrations/env.py` - Async env.py wired to app settings and Base metadata
- `migrations/script.py.mako` - Default Alembic template
- `migrations/versions/827bb2529fb6_initial_schema.py` - Initial migration (all 7 tables)
- `docker-compose.yml` - Web command now runs alembic upgrade head before uvicorn
- `Dockerfile` - Added COPY for migrations/ and alembic.ini
- `pyproject.toml` - Added ruff per-file-ignores for migration files

## Decisions Made
- Used Python 3.11+ StrEnum instead of (str, Enum) pattern — cleaner, ruff-compliant
- JSONB for transaction details field — flexible storage for varied transaction types
- Numeric(5,2) for salary, Numeric(6,2) for cap totals — avoids float rounding errors
- Docker web command runs `alembic upgrade head` before uvicorn — automatic schema sync on startup

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Ruff UP042: StrEnum required**
- **Found during:** Task 1 (Create SQLAlchemy models)
- **Issue:** Ruff requires `StrEnum` (Python 3.11+) instead of `(str, enum.Enum)` pattern
- **Fix:** Changed all enum classes to use `enum.StrEnum`
- **Files modified:** contract.py, roster.py, transaction.py
- **Verification:** ruff check passes clean
- **Committed in:** 7fd929c

**2. [Rule 3 - Blocking] Ruff TC003 vs SQLAlchemy runtime imports**
- **Found during:** Task 1
- **Issue:** datetime imports needed at runtime for SQLAlchemy but ruff wanted them in TYPE_CHECKING
- **Fix:** Added `# noqa: TC003` to keep as runtime imports
- **Files modified:** Multiple model files
- **Verification:** Models import and resolve correctly
- **Committed in:** 7fd929c

**3. [Rule 3 - Blocking] Ruff ignores for auto-generated migration files**
- **Found during:** Task 2 (Alembic init)
- **Issue:** Auto-generated Alembic migrations have line length and import style issues
- **Fix:** Added per-file-ignores for `migrations/versions/*.py` in pyproject.toml
- **Files modified:** pyproject.toml
- **Verification:** ruff check passes with ignores
- **Committed in:** 2a188e7

---

**Total deviations:** 3 auto-fixed (3 blocking), 0 deferred
**Impact on plan:** All auto-fixes were ruff compliance issues. No scope creep.

## Issues Encountered
None

## Next Phase Readiness
- Phase 2 complete — all 3 plans finished
- Database schema reflects ADL bylaws domain with proper normalization
- Async Alembic infrastructure ready for future schema changes
- Ready for Phase 3 (MFL API Integration) — models exist to sync data into

---
*Phase: 02-foundation*
*Completed: 2026-03-10*
