# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-10)

**Core value:** Accurate, automated contract extension calculations (EPV-based) that eliminate manual rule interpretation and spreadsheet formulas
**Current focus:** Phase 3 in progress — MFL API Integration

## Current Position

Phase: 3 of 8 (MFL API Integration)
Plan: 1 of 3 in current phase
Status: In progress
Last activity: 2026-03-10 — Completed 03-01-PLAN.md

Progress: ███████░░░ 28%

## Performance Metrics

**Velocity:**
- Total plans completed: 7
- Average duration: 5 min
- Total execution time: 34 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1 - Rules Extraction | 3 | 16 min | 5 min |
| 2 - Foundation | 3 | 14 min | 5 min |
| 3 - MFL API Integration | 1 | 4 min | 4 min |

**Recent Trend:**
- Last 5 plans: 01-03 (4 min), 02-01 (3 min), 02-02 (5 min), 02-03 (6 min), 03-01 (4 min)
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

### Deferred Issues

None yet.

### Blockers/Concerns

None yet.

## Session Continuity

Last session: 2026-03-10
Stopped at: Completed 03-01-PLAN.md
Resume file: None — ready for 03-02-PLAN.md
