# Phase 14: Historical Data Imports - Research

**Researched:** 2026-03-13
**Domain:** MFL API historical data retrieval + FastAPI background sync patterns
**Confidence:** HIGH

<research_summary>
## Summary

Researched the MFL API for historical data endpoints and the existing codebase to identify exactly what historical data is needed, what infrastructure already exists, and what gaps must be filled.

**Key finding:** The codebase already has ~80% of the infrastructure needed. Historical score sync exists (`sync_historical_scores`) but only pulls YTD totals — it's missing weekly scores that `is_robust_season()` requires (counts numeric-week records >= 8). Historical roster/contract sync is completely missing — needed for consecutive tag counts, RFA eligibility, positional salary averages, and EPV calculations that reference prior-season contracts.

The MFL API supports historical data via year-scoped URLs (`/{year}/export`). The `rosters` endpoint returns full salary/contract data for that year's snapshot, and `playerScores` accepts a `W` parameter for individual weeks. The existing `MFLClient` pattern of creating year-scoped instances via a factory function is the correct approach.

**Primary recommendation:** Extend the existing sync infrastructure with two new capabilities: (1) weekly score fetching for historical seasons (loop weeks 1-17 per year), and (2) historical roster/contract sync using the existing `sync_rosters` function with year-scoped clients. Use FastAPI lifespan to trigger background backfill on startup with automatic gap detection.
</research_summary>

<standard_stack>
## Standard Stack

No new libraries needed — the existing stack handles everything:

### Core (Already in Project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| httpx | existing | MFL API HTTP client | Already used in MFLClient |
| tenacity | existing | Retry with exponential backoff | Already used in MFLClient |
| SQLAlchemy | existing | Async ORM for data persistence | Already used throughout |
| APScheduler | existing | Periodic sync scheduling | Already used in lifespan |
| asyncio | stdlib | Background task management | Used for startup backfill |

### Supporting (Already in Project)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pydantic | existing | MFL response parsing | Already have MFLRostersResponse, MFLPlayerScoresResponse |
| FastAPI BackgroundTasks | existing | One-shot async work | Already used in sync API |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| APScheduler for backfill | asyncio.create_task in lifespan | APScheduler is overkill for one-shot backfill; asyncio.create_task is simpler |
| Individual week fetches | Bulk week fetch if available | MFL doesn't support bulk; must iterate weeks 1-17 per season |

**Installation:**
No new packages needed.
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Approach: Extend Existing Sync Infrastructure

The codebase has a clean sync pattern: `MFLClient` → `sync_*` service → database upsert. Extend this, don't replace it.

```
src/app/services/
├── sync_orchestrator.py    # Add run_historical_backfill()
├── score_sync.py           # Extend sync_historical_scores() to fetch weekly
├── roster_sync.py          # Already works for any season — just needs year-scoped client
└── historical_sync.py      # NEW: gap detection + background coordinator
```

### Pattern 1: Year-Scoped Client Factory (Already Exists)
**What:** Create MFLClient instances per year since MFL API URLs are year-scoped.
**When to use:** Any historical data fetch.
**Already implemented in:** `sync_orchestrator.py:run_historical_sync()`
```python
def client_factory(year: int) -> MFLClient:
    return MFLClient(
        year=year,
        league_id=settings.mfl_league_id,
        base_url=settings.mfl_base_url,
        api_key=settings.mfl_api_key,
        ...
    )
```

### Pattern 2: Gap Detection via Database Queries
**What:** Query existing data to determine what's missing before fetching.
**When to use:** On startup to avoid redundant API calls.
```python
# Score gap detection: check which (season, week) combos are missing
async def detect_score_gaps(session, years):
    """Return dict of year -> list of missing weeks."""
    gaps = {}
    for year in years:
        existing_weeks = await session.execute(
            select(distinct(PlayerScore.week))
            .where(PlayerScore.season == year)
        )
        have = {row[0] for row in existing_weeks}
        need = {"YTD"} | {str(w) for w in range(1, 18)}
        missing = need - have
        if missing:
            gaps[year] = sorted(missing)
    return gaps

# Contract gap detection: check which seasons have contracts
async def detect_contract_gaps(session, years):
    """Return list of years with no contract records."""
    result = await session.execute(
        select(distinct(Contract.season))
        .where(Contract.season.in_(years))
    )
    have = {row[0] for row in result}
    return [y for y in years if y not in have]
```

### Pattern 3: Non-Blocking Startup Backfill
**What:** Launch backfill as background task during FastAPI lifespan startup.
**When to use:** On every app start — gap detection keeps it idempotent.
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    backfill_task = None
    if settings.sync_enabled:
        # ... existing scheduler setup ...
        # Launch backfill as non-blocking background task
        backfill_task = asyncio.create_task(
            run_historical_backfill(settings)
        )
    yield
    if backfill_task and not backfill_task.done():
        backfill_task.cancel()
    await engine.dispose()
```

### Pattern 4: Data Completeness Indicator
**What:** Track backfill status so eligibility checks can warn when data is incomplete.
**When to use:** Eligibility endpoints should indicate confidence level.
```python
@dataclass
class BackfillStatus:
    """Track historical data completeness."""
    in_progress: bool = False
    scores_complete: bool = False
    contracts_complete: bool = False
    missing_score_years: list[int] = field(default_factory=list)
    missing_contract_years: list[int] = field(default_factory=list)
```

### Anti-Patterns to Avoid
- **Fetching all data every startup:** Use gap detection — only fetch what's missing
- **Blocking startup on backfill:** App should serve requests immediately; backfill runs in background
- **Separate historical data tables:** Use existing PlayerScore and Contract tables with season column — they already support multi-season data
- **Ignoring rate limits between years:** MFL rate limits are per-IP; respect 1s delay even between year-scoped clients
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Roster/contract sync for past years | New historical roster parser | Existing `sync_rosters()` with year-scoped client | Same MFL response format, same parsing logic — just different year |
| Score sync for past years | New historical score service | Existing `sync_scores()` called per-week | Already handles upsert, batch flush, player lookup |
| MFL API client for past years | New client class | Existing `MFLClient` constructor with different `year` param | Year is already a constructor parameter |
| Retry/rate limiting | Custom retry loops | Existing tenacity decorators on `_export_with_retry` | Already handles 429, exponential backoff |
| Response parsing | Custom JSON parsers | Existing `MFLRostersResponse`, `MFLPlayerScoresResponse` Pydantic models | Same response shape regardless of year |

**Key insight:** The MFL API returns the same response structure regardless of year. The existing sync services (`sync_rosters`, `sync_scores`) work for any year — they just need to be called with year-scoped clients. The real work is orchestration (gap detection, background execution, status tracking), not new API integration.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Missing Weekly Scores for Robust Season Check
**What goes wrong:** `is_robust_season()` returns false for all historical seasons because only YTD records exist.
**Why it happens:** Current `sync_historical_scores` only fetches `week="YTD"`, but `is_robust_season` counts individual week records (1-17).
**How to avoid:** Historical score sync MUST fetch weeks 1-17 in addition to YTD for each season.
**Warning signs:** All players show "no Robust PRs in recent seasons" for extension eligibility.

### Pitfall 2: MFL Rate Limiting Across Year-Scoped Requests
**What goes wrong:** Getting 429 errors when switching between year-scoped endpoints rapidly.
**Why it happens:** MFL rate limits are per-IP, not per-year. Switching from `/{2024}/export` to `/{2023}/export` still counts against the same limit.
**How to avoid:** Maintain 1-second delay between ALL requests, even across different years. The existing `_request_delay` handles this within a single client, but when creating new client instances per year, the delay resets.
**Warning signs:** Intermittent 429 errors during historical sync.

### Pitfall 3: February 21 Rollover Boundary
**What goes wrong:** Querying "current year" data during Feb 1-20 returns previous season; querying after Feb 21 treats it as new season.
**Why it happens:** MFL rolls over to the new league year on February 21st.
**How to avoid:** Always use explicit year parameters, never rely on "current year" default. The `sync_historical_years` config explicitly lists years [2020-2025].
**Warning signs:** Missing data for the most recent completed season, or duplicate data across season boundary.

### Pitfall 4: Player Lookup Failures for Historical Seasons
**What goes wrong:** Historical rosters reference players who aren't in the current Player table (retired, removed from MFL).
**Why it happens:** Player sync only runs for current year — past-year rosters may contain players no longer active.
**How to avoid:** Run player sync from each historical year's data BEFORE syncing rosters, OR gracefully skip players not in lookup (current behavior in `sync_rosters`).
**Warning signs:** Many "player not found" warnings in historical roster sync logs.

### Pitfall 5: Contract Table Conflicts Across Seasons
**What goes wrong:** UniqueConstraint on `(team_id, player_id, season)` prevents re-importing.
**Why it happens:** Players can be traded mid-season, creating multiple contracts per season.
**How to avoid:** The existing upsert logic handles this — it updates existing records. But be aware that end-of-season roster snapshot may differ from mid-season state.
**Warning signs:** IntegrityError on contract inserts for traded players.

### Pitfall 6: Blocking App Startup with Backfill
**What goes wrong:** App doesn't respond to requests until multi-year backfill completes (~6 years × 18 requests × 1s delay = ~2 minutes minimum).
**Why it happens:** Running backfill synchronously in lifespan startup.
**How to avoid:** Use `asyncio.create_task()` to run backfill in background. App should be ready to serve immediately.
**Warning signs:** Health check fails/times out during startup.
</common_pitfalls>

<code_examples>
## Code Examples

### Existing Infrastructure to Reuse

**Year-scoped client factory (sync_orchestrator.py:116-125):**
```python
# Already exists — creates MFLClient per year
def client_factory(year: int) -> MFLClient:
    return MFLClient(
        year=year,
        league_id=settings.mfl_league_id,
        base_url=settings.mfl_base_url,
        api_key=settings.mfl_api_key,
        username=settings.mfl_username,
        password=settings.mfl_password,
        request_delay=settings.mfl_request_delay,
    )
```

**MFL playerScores endpoint (client.py:185-192):**
```python
# Already supports year parameter — just needs week iteration
async def player_scores(
    self, week: str = "YTD", year: int | None = None
) -> dict[str, Any]:
    params: dict[str, Any] = {"W": week}
    if year is not None:
        params["YEAR"] = year
    return await self._export("playerScores", **params)
```

**Existing score sync (score_sync.py:28-124):**
```python
# Already handles any (season, week) combo — reusable for historical weekly
async def sync_scores(client, session, season, week="YTD") -> SyncResult:
    # ... fetches, parses, upserts — works for any season/week
```

**Existing roster sync (roster_sync.py:55-220):**
```python
# Already handles any season — just pass year-scoped client
async def sync_rosters(client, session, season) -> SyncResult:
    # ... fetches rosters, upserts RosterEntry + Contract records
```

### Key Pattern: Weekly Score Fetch Loop
```python
# Fetch all weeks for a historical season
async def sync_season_weekly_scores(client, session, season):
    """Sync weekly scores (1-17) + YTD for a single season."""
    results = []
    for week in ["YTD"] + [str(w) for w in range(1, 18)]:
        result = await sync_scores(client, session, season=season, week=week)
        results.append(result)
    return results
```

### Key Pattern: Non-Blocking Lifespan Backfill
```python
# Source: FastAPI lifespan docs + asyncio.create_task pattern
@asynccontextmanager
async def lifespan(app: FastAPI):
    backfill_task = None
    if settings.sync_enabled:
        # Start scheduler (existing)
        async with AsyncScheduler() as scheduler:
            # ... existing scheduler setup ...

            # Launch one-shot backfill in background
            backfill_task = asyncio.create_task(
                run_historical_backfill(settings)
            )
            yield

            if backfill_task and not backfill_task.done():
                backfill_task.cancel()
    else:
        yield
    await engine.dispose()
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| FastAPI @app.on_event("startup") | Lifespan context manager | FastAPI 0.93+ | Already using lifespan — correct |
| APScheduler 3.x | APScheduler 4.x (AsyncScheduler) | 2024 | Already using v4 — correct |
| Synchronous DB operations | Async SQLAlchemy | 2023+ | Already using async — correct |

**New tools/patterns to consider:**
- `asyncio.TaskGroup` (Python 3.11+): Could replace manual task creation for parallel year fetches, but sequential is better here due to rate limits
- APScheduler `add_job` with `next_run_time=now`: Alternative to asyncio.create_task for one-shot backfill, integrates with existing scheduler

**Deprecated/outdated:**
- `@app.on_event("startup")` decorator: Use lifespan instead (already doing this)
- APScheduler 3.x `BackgroundScheduler`: Use 4.x `AsyncScheduler` (already doing this)
</sota_updates>

<open_questions>
## Open Questions

1. **MFL API availability for old years**
   - What we know: MFL states "most API functionality is only supported for the current year" and they "will not be adding features or fixing issues related to queries that refer to past seasons"
   - What's unclear: Whether the rosters endpoint for years 2020-2025 reliably returns salary/contract data, or whether some historical years may have incomplete data
   - Recommendation: Test each year's rosters endpoint manually before implementing. If a year returns no salary data, document it and skip. The app should gracefully handle missing historical contract data.

2. **Player table completeness for historical rosters**
   - What we know: Player sync only runs for current year. Historical rosters may reference players not in our Player table.
   - What's unclear: How many historical players are missing from current Player table
   - Recommendation: Either (a) run player sync per historical year (adds ~6 API calls), or (b) skip unknown players (existing behavior). Option (b) is simpler and may be acceptable since retired players are less likely to need eligibility checks.

3. **Weekly scores vs YTD for historical seasons**
   - What we know: `is_robust_season()` needs weekly records; `get_position_rank()` needs YTD
   - What's unclear: Whether MFL reliably returns week-level scores for old seasons (e.g., 2020)
   - Recommendation: Attempt to fetch all weeks; if a year returns empty/error for weekly scores, fall back to YTD-only and log a warning. The robust season check would be unable to run for that year, but EPV position rank would still work.
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- Codebase analysis: `src/app/services/score_sync.py`, `src/app/services/roster_sync.py`, `src/app/services/sync_orchestrator.py` — existing sync infrastructure
- Codebase analysis: `src/app/services/epv.py` — historical data consumers (`get_position_rank`, `is_robust_season`)
- Codebase analysis: `src/app/services/franchise_tags.py` — `get_consecutive_tag_count` needs historical contracts
- Codebase analysis: `src/app/services/tenders.py` — RFA/ERFA eligibility checks need contract history
- MFL API docs: `https://api.myfantasyleague.com/2025/api_info?STATE=details` — endpoint parameters and historical limitations

### Secondary (MEDIUM confidence)
- FastAPI lifespan docs — confirmed asyncio.create_task pattern for non-blocking startup tasks
- ffscrapr R package — confirmed year-scoped URL pattern and week-level score fetching work for historical years
- MFL API info page — confirmed rate limiting (1 req/sec, 429 on throttle)

### Tertiary (LOW confidence - needs validation)
- MFL historical data reliability — MFL officially states past-year support is limited, but community tools (ffscrapr) successfully use it. Needs testing for our specific league (60206) and year range (2020-2025).
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: MFL API historical endpoints (rosters, playerScores)
- Ecosystem: FastAPI lifespan, asyncio background tasks, APScheduler
- Patterns: Gap detection, year-scoped client factory, non-blocking backfill
- Pitfalls: Rate limits, missing weekly scores, player lookup failures, rollover boundary

**Confidence breakdown:**
- Standard stack: HIGH — no new libraries needed, all existing
- Architecture: HIGH — extends proven patterns already in codebase
- Pitfalls: HIGH — identified from direct codebase analysis (weekly scores gap is verified)
- Code examples: HIGH — all from existing codebase with minimal modifications

**Research date:** 2026-03-13
**Valid until:** 2026-04-13 (30 days — MFL API is stable, codebase patterns established)
</metadata>

---

*Phase: 14-historical-data-imports*
*Research completed: 2026-03-13*
*Ready for planning: yes*
