# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-10)

**Core value:** Accurate, automated contract extension calculations (EPV-based) that eliminate manual rule interpretation and spreadsheet formulas
**Current focus:** Phase 4 in progress — Contract Engine

## Current Position

Phase: 4 of 8 (Contract Engine)
Plan: 1 of 4 in current phase
Status: In progress
Last activity: 2026-03-11 — Completed 04-01-PLAN.md

Progress: ███████████░░░░░░░░░ 44%

## Performance Metrics

**Velocity:**
- Total plans completed: 11
- Average duration: 4 min
- Total execution time: 48 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1 - Rules Extraction | 3 | 16 min | 5 min |
| 2 - Foundation | 3 | 14 min | 5 min |
| 3 - MFL API Integration | 4 | 14 min | 4 min |
| 4 - Contract Engine | 1 | 4 min | 4 min |

**Recent Trend:**
- Last 5 plans: 03-01 (4 min), 03-02 (2 min), 03-03 (4 min), 03-04 (4 min), 04-01 (4 min)
- Trend: —

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

| Phase | Decision | Rationale |
|-------|----------|-----------|
| 01-01 | Used pay-in table value ($3,810) for prize pool | Bylaws has internal inconsistency; pay-in table is more reliable |
| 01-01 | Renamed neft_bid_error_penalty to bid_error_penalty | Applies to both NEFT and RFA auctions per bylaws |
| 01-02 | Bylaws wins over old EPV code on floor calculation | Old code uses 100% floor; bylaws specifies 75% — documented as epv_code_discrepancy |
| 01-03 | Created new contracts.yaml for contract formulas | Domain separation — rookie min, inflation, veteran min, UDFA budget don't fit existing formula files |
| 02-01 | Used uv (not pip/poetry) for package management | Per research phase — faster, modern, single tool for deps+venvs |
| 02-01 | src/app/ layout with core/models/schemas/api subpackages | Standard FastAPI convention, clean separation |
| 02-02 | expire_on_commit=False for async sessions | Prevents MissingGreenlet errors in async SQLAlchemy |
| 02-02 | Separate db-test service on port 5433 | Test isolation without affecting dev database |
| 02-03 | StrEnum for constrained columns (ContractType, etc.) | Python 3.11+ pattern, ruff-compliant, cleaner than (str, Enum) |
| 02-03 | JSONB for transaction details | Flexible storage for varied transaction types |
| 02-03 | Numeric(5,2)/Numeric(6,2) for money fields | Avoids float rounding errors |
| 02-03 | Alembic upgrade head in Docker startup | Automatic schema sync on container start |
| 03-01 | Added hatchling build-system to pyproject.toml | Required for package imports to work |
| 03-01 | Snake_case fields with Field(alias=...) for MFL camelCase | Satisfies ruff N815 while matching API response keys |
| 03-02 | SyncResult dataclass in team_sync.py, reused by player_sync | Standard return type for all sync services |
| 03-02 | Player sync fetches all existing into memory lookup | O(1) matching for batch upserts |
| 03-02 | Neither sync service commits transactions | Caller controls transaction boundaries |
| 03-03 | ContractType set to NG placeholder for all contracts | Phase 4 contract engine will classify NG/SD/FG properly |
| 03-03 | Salary parsed via float(Decimal(str)) | Preserve precision during conversion, match Contract model float field |
| 03-03 | sync_historical_scores uses async context manager factory | Year-scoped MFL clients need separate instances per season |
| 03-04 | APScheduler 4.x alpha (>=4.0.0a1) for AsyncScheduler | v3.x lacks async support needed for FastAPI integration |
| 03-04 | BackgroundTasks for manual sync trigger | Non-blocking 202 response, appropriate for one-off triggered tasks |
| 03-04 | Single transaction for full sync atomicity | All four sync steps committed together or rolled back together |
| 03-04 | No sync on startup | Only on schedule interval or manual trigger to avoid blocking app start |
| 04-01 | Salaries in millions throughout (0.01 = $10k) | Matches Contract model Numeric(5,2) |
| 04-01 | Year fallback for season lookups | Returns latest available if requested year missing |
| 04-01 | _sal_at_rank clamps to list bounds | Avoids index errors on out-of-range rank |

### Deferred Issues

None yet.

### Blockers/Concerns

None yet.

## Session Continuity

Last session: 2026-03-11
Stopped at: Completed 04-01-PLAN.md
Resume file: None
