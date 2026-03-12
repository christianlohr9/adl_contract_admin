# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-03-10)

**Core value:** Accurate, automated contract extension calculations (EPV-based) that eliminate manual rule interpretation and spreadsheet formulas
**Current focus:** Milestone v1.1 — League Calendar & Contract Management

## Current Position

Phase: 12 of 13 (Contract Management Dashboard)
Plan: 2 of 2 in current phase
Status: Phase complete
Last activity: 2026-03-12 — Completed 12-02-PLAN.md

Progress: ███████████████████░░ 83% (33/40 plans)

## Performance Metrics

**Velocity:**
- Total plans completed: 33
- Average duration: 6 min
- Total execution time: 218 min

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1 - Rules Extraction | 3 | 16 min | 5 min |
| 2 - Foundation | 3 | 14 min | 5 min |
| 3 - MFL API Integration | 4 | 14 min | 4 min |
| 4 - Contract Engine | 4 | 14 min | 4 min |
| 5 - Salary Cap & Validation | 2+fix | 15 min | 5 min |
| 6 - API Layer | 3 | 9 min | 3 min |
| 7 - Frontend Placeholder | 2 | 13 min | 7 min |
| 8 - Frontend UI | 4 | 45 min | 11 min |

| 9 - League Calendar Data Model | 2 | 18 min | 9 min |
| 10 - Period Detection & Eligibility | 2 | 5 min | 3 min |
| 11 - Roster-Wide Eligibility API | 1 | 3 min | 3 min |

| 12 - Contract Management Dashboard | 2 | 50 min | 25 min |

**Recent Trend:**
- Last 5 plans: 10-01 (2 min), 10-02 (3 min), 11-01 (3 min), 12-01 (7 min), 12-02 (43 min)
- Trend: ↑ (12-02 included bug investigation for ISS-002)

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
| 04-02 | Combined eligibility + options in single file | Self-contained tool per CONTEXT.md |
| 04-02 | 5YO detection via '+' or '5YO' in designation | Contract designation string convention |
| 04-02 | Total value uses compound growth on smoothed salary | Consistent with salary smoothing formula |
| 04-03 | NFL RFA prices left as parameters with defaults | External values not yet in constants — non-blocking |
| 04-03 | Tag salary uses AVG(Top-N) vs 1.20x prev MAX | Cap% treated as positional average per bylaws guidance |
| 04-04 | All B/R, 5YO, PPE in single buyouts.py module | Complementary contract tools, self-contained |
| 04-04 | Modified TT reuses franchise_tags helpers with custom rank ranges | Avoids duplicating salary averaging logic |
| 04-04 | Starter percentile from PlayerScore YTD points | Consistent scoring source for percentile tiers |
| 05-01 | FG multi-year split uses ceil_10k/floor_10k (not 100k) | Bylaws example requires 10k precision matching cap_penalty_rounding |
| 05-01 | Draft round passed from Player model to classifier | More reliable than designation-only parsing |
| 05-01-FIX | Regex patterns use uppercase (OFF, IO) since classifier uppercases designation | Avoids case mismatch between stored designation and regex |
| 05-02 | Deferred imports in allotment checks to avoid circular deps | eligibility.py and allotments.py cross-reference each other |
| 05-02 | team_id resolved from player's contract record | Previous season for expired-contract actions, current for active |
| 05-02 | RFA and ERFA tenders share single "tender" allotment type | Matches bylaws limit of 2 shared tenders |
| 06-01 | Decimal for salary fields in Pydantic schemas | Match Numeric model columns, avoid float rounding |
| 06-01 | Search route before {player_id} in players router | Prevent FastAPI path parameter conflicts |
| 06-02 | Used /{player_id}/all for bundled endpoint | Avoids path conflicts with sub-routes like /extensions, /tags |
| 06-02 | Per-tool error isolation in bundled endpoint | Each service wrapped in try/except so one failure doesn't block others |
| 06-03 | PenaltyResultSchema mirrors actual dataclass fields | Plan said to check dataclass and mirror it — actual fields differ from plan's simplified version |
| 06-03 | Snapshot endpoint on teams router | Team-scoped endpoint, consistent with existing team routes |
| 07-01 | Tailwind v4 CSS-first config (no tailwind.config.js) | v4 uses @import "tailwindcss" and @theme in CSS |
| 07-01 | shadcn/ui v4 uses Base UI render prop (not Radix asChild) | New shadcn version changed component API |
| 07-01 | Force-tracked frontend/src/lib/ past root .gitignore lib/ rule | Python convention in root gitignore conflicts with frontend lib/ |
| 07-02 | 6 contract tool tabs: Extensions, Tags, Tenders, Buyout, 5YO, PPE | Matches bylaws sections X-A/B through X-E plus 5YO and PPE |
| 08-01 | BrowserRouter moved from App.tsx to main.tsx | QueryClientProvider needs to wrap everything |
| 08-01 | Query key convention: [entity, id, sub-resource] | Consistent cache invalidation pattern |
| 08-02 | Salary displayed in millions matching MFL platform ($40.93) | User feedback — platform convention, not expanded USD |
| 08-02 | CORS middleware restricted to localhost:5173 | Required for frontend-backend dev communication |
| 08-02 | nuqs with react-router/v7 adapter for URL state | Filter persistence across navigation |
| 08-04 | Fixed chart height 120px instead of aspect-video | Single stacked bar doesn't need 16:9 ratio |
| 08-04 | Free agents (years_remaining=0) as top-priority action items | Offseason: FA can be tagged/tendered, most urgent actions |
| 08-04 | Tags/tenders prioritized above buyouts in action items | Aligns with offseason workflow priority |
| 09-01 | All 27 date fields nullable — commissioner fills progressively | Dates set throughout offseason, not all at once |
| 09-01 | PUT uses setattr loop on non-None fields for partial updates | Partial updates without nulling others |
| 09-01 | 409 Conflict on duplicate season POST | Consistent with unique constraint |
| 09-02 | Native date inputs for admin calendar form | No date picker library needed for single-user admin |
| 09-02 | calendarExists priority over isNewSeason in save logic | Prevents 409 on existing seasons |
| 10-01 | PPE always-open, no deadline window | Performance-based, not a deadline action |
| 10-01 | Extension dual-window: oEXT deadline OR iEXT start/end | Either open = available |
| 10-01 | Deferred import of get_window_status in check_eligibility | Avoid circular dependencies |
| 10-02 | Window statuses in bundled endpoint only, not individual tools | Individual endpoints return calculations, not eligibility |
| 10-02 | Per-tool error isolation for window status fetch | Consistent with existing bundled endpoint pattern |

### Deferred Issues

- ISS-001: Extension window awareness — league calendar for offseason/in-season signing periods (**Resolved at backend level — Phase 10 complete**)
- ISS-002: Franchise tag / tender eligibility checks query wrong season for expired contracts (**Deferred to Phase 13**)

### Blockers/Concerns

None.

### Roadmap Evolution

- v1.0 MVP shipped: 8 phases (1-8), foundation through functional UI, completed 2026-03-12
- Milestone v1.1 created: League Calendar & Contract Management, 5 phases (Phase 9-13)

## Session Continuity

Last session: 2026-03-12
Stopped at: Completed 12-02-PLAN.md — Phase 12 complete
Resume file: None
