# Phase 3: MFL API Integration - Research

**Researched:** 2026-03-10
**Domain:** MyFantasyLeague (MFL) REST API integration with Python/FastAPI
**Confidence:** HIGH

<research_summary>
## Summary

Researched the MFL API ecosystem, all available Python wrappers, and data sync architecture patterns for FastAPI. The MFL API is a straightforward REST API with JSON support — all data retrieval is via GET requests to `https://api.myfantasyleague.com/{year}/export?TYPE={type}&L={league_id}&JSON=1`.

**Key finding:** All existing Python MFL wrappers (pymfl, python-mfl, python-myfantasyleague, mfl-pyapi) are unmaintained, have minimal stars/activity, and none support async. The MFL API is simple enough that a thin custom client using httpx (async) is the clear best approach — it gives full control over endpoints, auth, error handling, and fits naturally into the async FastAPI stack.

**Primary recommendation:** Build a thin async MFL client using httpx with cookie-based authentication. Use APScheduler (v4) integrated via FastAPI lifespan for scheduled background sync. Keep the client layer separate from the sync/service layer.
</research_summary>

<standard_stack>
## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| httpx | 0.28+ | Async HTTP client | Native async/sync support, cookie management, HTTP/2, pairs naturally with FastAPI |
| apscheduler | 4.x | Scheduled background sync | Native async support, FastAPI lifespan integration, SQLAlchemy data store |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pytest-httpx | 0.35+ | Mock HTTP in tests | Testing MFL client without hitting real API |
| tenacity | 9.x | Retry logic | Rate limit handling with exponential backoff |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| httpx | aiohttp | aiohttp slightly faster for pure async, but httpx has simpler API, sync+async support, better cookie handling, and is the de facto standard with FastAPI |
| httpx | pymfl (joeyagreco) | pymfl is unmaintained (last release Aug 2022, 10 stars), no async, incomplete endpoint coverage |
| httpx | python-mfl (mikeplis) | 5 stars, 24 commits, no releases, essentially abandoned |
| httpx | python-myfantasyleague (mraspberry) | 0 stars, 6 commits, created 2016, abandoned |
| APScheduler | Celery | Celery requires Redis/RabbitMQ broker — overkill for single-server periodic sync |
| APScheduler | FastAPI BackgroundTasks | BackgroundTasks runs after a request — not suitable for scheduled/periodic jobs |

**Installation:**
```bash
uv add httpx apscheduler tenacity
uv add --dev pytest-httpx
```
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Project Structure
```
src/app/
├── mfl/
│   ├── __init__.py
│   ├── client.py          # Thin async MFL API client (httpx wrapper)
│   ├── auth.py            # Cookie-based authentication
│   ├── models.py          # Pydantic models for MFL API responses
│   ├── sync.py            # Sync orchestration (which endpoints to call, in what order)
│   └── exceptions.py      # MFL-specific exceptions (rate limit, auth failure)
├── services/
│   └── sync_service.py    # Business logic: transform MFL data → DB models
└── scheduler/
    └── jobs.py            # APScheduler job definitions
```

### Pattern 1: Thin Async MFL Client
**What:** Wrap httpx.AsyncClient with MFL-specific methods. One method per endpoint type. All return parsed JSON dicts.
**When to use:** All MFL API access goes through this client.
**Example:**
```python
import httpx
from typing import Any

class MFLClient:
    """Thin async wrapper around the MFL REST API."""

    BASE_URL = "https://api.myfantasyleague.com"

    def __init__(self, year: int, league_id: int):
        self.year = year
        self.league_id = league_id
        self._client: httpx.AsyncClient | None = None
        self._cookie: str | None = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            base_url=f"{self.BASE_URL}/{self.year}",
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        return self

    async def __aexit__(self, *exc):
        if self._client:
            await self._client.aclose()

    async def login(self, username: str, password: str) -> None:
        """Authenticate and store session cookie."""
        resp = await self._client.get(
            "/login",
            params={"USERNAME": username, "PASSWORD": password, "XML": 1},
        )
        resp.raise_for_status()
        # Parse cookie from XML response
        # Store as self._cookie for subsequent requests

    async def export(self, type_: str, **params: Any) -> dict:
        """Generic export endpoint."""
        params = {"TYPE": type_, "L": self.league_id, "JSON": 1, **params}
        headers = {}
        if self._cookie:
            headers["Cookie"] = f"MFL_USER_ID={self._cookie}"
        resp = await self._client.get("/export", params=params, headers=headers)
        resp.raise_for_status()
        return resp.json()

    # Convenience methods
    async def league(self) -> dict:
        return await self.export("league")

    async def rosters(self) -> dict:
        return await self.export("rosters")

    async def players(self) -> dict:
        return await self.export("players")

    async def player_scores(self, week: str = "YTD", year: int | None = None) -> dict:
        params = {"W": week}
        if year:
            params["YEAR"] = year
        return await self.export("playerScores", **params)

    async def free_agents(self) -> dict:
        return await self.export("freeAgents")

    async def salaries(self) -> dict:
        return await self.export("salaries")

    async def transactions(self, **params) -> dict:
        return await self.export("transactions", **params)
```

### Pattern 2: FastAPI Lifespan + APScheduler
**What:** Start/stop the scheduler in FastAPI's lifespan context manager. Scheduler runs background sync jobs on a configurable interval.
**When to use:** For periodic MFL data sync.
**Example:**
```python
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from fastapi import FastAPI
from apscheduler import AsyncScheduler, ConflictPolicy
from apscheduler.triggers.interval import IntervalTrigger

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    scheduler = AsyncScheduler()
    async with scheduler:
        await scheduler.add_schedule(
            sync_mfl_data,
            IntervalTrigger(hours=6),
            id="mfl_sync",
            conflict_policy=ConflictPolicy.replace,
        )
        await scheduler.start_in_background()
        yield

app = FastAPI(lifespan=lifespan)
```

### Pattern 3: Year-Scoped API Access for Historical Data
**What:** MFL API URLs are year-scoped (`/2025/export`, `/2024/export`). Create separate client instances per year when syncing historical data.
**When to use:** Syncing past seasons for EPV calculations.
**Example:**
```python
async def sync_historical_scores(league_id: int, years: list[int]):
    for year in years:
        async with MFLClient(year=year, league_id=league_id) as client:
            scores = await client.player_scores(week="YTD")
            await save_scores(scores, year)
```

### Anti-Patterns to Avoid
- **Using an unmaintained wrapper library:** All Python MFL wrappers are abandoned. Building a thin client takes 1-2 hours and gives full control.
- **Hitting the API without rate limiting:** MFL throttles at ~1 req/sec for unregistered clients. Always add delays between requests.
- **Not caching the player database:** MFL docs state player DB only changes once/day. Cache it, don't re-fetch every sync.
- **Synchronous HTTP calls in async FastAPI:** Use httpx.AsyncClient, not requests. Blocking calls will starve the event loop.
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| HTTP client with retry | Custom retry loops | httpx + tenacity | Exponential backoff, jitter, rate limit awareness |
| Scheduled background jobs | asyncio.create_task with sleep loops | APScheduler | Persistent schedules, error handling, configurable triggers |
| Cookie-based auth session | Manual cookie string parsing | httpx cookie jar | Handles encoding, expiry, domain scoping |
| API response validation | Manual dict key checks | Pydantic models | Type safety, clear error messages, documentation |
| Rate limiting | sleep(1) between requests | tenacity with wait_exponential | Handles 429 responses, backs off intelligently |

**Key insight:** The MFL API is simple (GET requests with query params returning JSON), but the operational concerns — rate limiting, auth cookie management, error handling, retries, scheduling — are where complexity lives. Use established libraries for those operational concerns and keep the MFL-specific code thin.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Rate Limiting / HTTP 429
**What goes wrong:** MFL returns 429 Too Many Requests, sync fails mid-batch
**Why it happens:** Unregistered clients have low rate limits; burst requests during bulk sync trigger throttling
**How to avoid:** Space requests 1+ second apart. Use tenacity with `retry_if_exception_type(httpx.HTTPStatusError)` and `wait_exponential`. Register as a developer client for ~2.5x higher limits.
**Warning signs:** Intermittent 429 errors, especially during initial historical data load

### Pitfall 2: Year-Scoped URLs
**What goes wrong:** Getting empty/wrong data because the year in the URL doesn't match the data expected
**Why it happens:** MFL API format is `/{year}/export?...` — the year is part of the URL path, not a parameter. Historical data requires different year in the URL.
**How to avoid:** Make year an explicit parameter in the client. When syncing seasons 2020-2025, create a client per year.
**Warning signs:** Empty rosters for past years, missing historical scores

### Pitfall 3: Cookie Auth Encoding
**What goes wrong:** Authentication fails or silently returns public-only data
**Why it happens:** MFL cookie values are Base64 that may contain `+`, `/`, `=` — these need URL encoding in some contexts
**How to avoid:** Let httpx handle cookie encoding via its cookie jar. If manually passing cookies, URL-encode the value.
**Warning signs:** Getting 401s, or getting limited data that looks like "public only" results

### Pitfall 4: Stale Player Database Cache
**What goes wrong:** Player IDs don't resolve to names, or positions are wrong
**Why it happens:** The `players` endpoint returns ALL players in MFL's database (thousands). Fetching it every sync is wasteful and slow.
**How to avoid:** MFL docs state player DB changes once/day. Cache it and refresh daily, separate from the main roster/scores sync.
**Warning signs:** Slow sync times, unnecessary API calls

### Pitfall 5: Custom Scoring/Roster Not Reflected
**What goes wrong:** Scores don't match what GMs see in MFL, roster slots look wrong
**Why it happens:** ADL has custom scoring rules and custom roster positions in MFL. The `league` endpoint returns these configs — must use them when interpreting data.
**How to avoid:** Fetch `league` endpoint first, parse `rosterSize`, `starters`, and scoring rules. Store config in DB for reference.
**Warning signs:** Score mismatches, wrong number of roster slots, "unknown position" errors
</common_pitfalls>

<code_examples>
## Code Examples

### MFL API Request Format
```python
# Source: MFL API documentation (api.myfantasyleague.com/2025/api_info)
# All export requests follow this pattern:
# GET https://api.myfantasyleague.com/{year}/export?TYPE={type}&L={league_id}&JSON=1

# Public endpoints (no auth needed):
# - players (full player database)
# - playerScores (when not franchise-specific)
# - injuries, nflSchedule, nflByeWeeks

# Auth-required endpoints:
# - rosters, freeAgents, salaries, transactions
# - Any franchise-specific data
```

### Authentication Flow
```python
# Source: MFL API documentation
# Step 1: Login to get cookie
import httpx

async def mfl_login(username: str, password: str) -> str:
    """Returns the MFL_USER_ID cookie value."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://api.myfantasyleague.com/2025/login",
            params={"USERNAME": username, "PASSWORD": password, "XML": 1},
        )
        resp.raise_for_status()
        # Response XML: <status cookie_name="MFL_USER_ID" cookie_value="..."/>
        # Parse cookie_value from XML
        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.text)
        return root.attrib["cookie_value"]

# Step 2: Use cookie in subsequent requests
async def mfl_export(cookie: str, year: int, league_id: int, type_: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://api.myfantasyleague.com/{year}/export",
            params={"TYPE": type_, "L": league_id, "JSON": 1},
            headers={"Cookie": f"MFL_USER_ID={cookie}"},
        )
        resp.raise_for_status()
        return resp.json()
```

### Rate-Limited Requests with Tenacity
```python
# Source: tenacity docs + MFL rate limit guidance
from tenacity import retry, wait_exponential, retry_if_exception_type, stop_after_attempt
import httpx

@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
)
async def fetch_with_retry(client: httpx.AsyncClient, url: str, **params) -> dict:
    resp = await client.get(url, params=params)
    resp.raise_for_status()
    return resp.json()
```

### APScheduler + FastAPI Lifespan
```python
# Source: APScheduler 4.x docs (Context7)
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from fastapi import FastAPI
from apscheduler import AsyncScheduler, ConflictPolicy
from apscheduler.triggers.interval import IntervalTrigger

async def sync_mfl_data():
    """Periodic sync job."""
    # ... sync logic here

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    scheduler = AsyncScheduler()
    async with scheduler:
        await scheduler.add_schedule(
            sync_mfl_data,
            IntervalTrigger(hours=6),
            id="mfl_sync",
            conflict_policy=ConflictPolicy.replace,
        )
        await scheduler.start_in_background()
        yield

app = FastAPI(lifespan=lifespan)
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| R/ffscrapr via rpy2 | Direct Python httpx calls | Now (this project) | Eliminates R dependency, native async, simpler stack |
| requests (sync) | httpx (async) | 2023+ | httpx is the standard async HTTP client for FastAPI projects |
| APScheduler 3.x (BackgroundScheduler) | APScheduler 4.x (AsyncScheduler) | 2024 | Native async, FastAPI lifespan integration, SQLAlchemy data store |
| pymfl/python-mfl wrappers | Custom thin client | Now | All wrappers abandoned; thin client is more maintainable |

**New tools/patterns to consider:**
- **APScheduler 4.x:** Full async rewrite with FastAPI lifespan integration pattern (via Context7 docs)
- **httpx custom Auth class:** Can implement MFL cookie auth as a reusable httpx.Auth subclass
- **pytest-httpx:** Mock MFL API responses in tests without real network calls

**Deprecated/outdated:**
- **ffscrapr via rpy2:** Adds R dependency to a Python project; fragile bridge
- **pymfl (v1.0.2, Aug 2022):** Unmaintained, no async, references 2022 API docs
- **requests library:** Sync-only; blocks FastAPI's event loop
</sota_updates>

<mfl_api_reference>
## MFL API Endpoint Reference

Complete list of export TYPE values relevant to ADL:

### Must-Have Endpoints
| TYPE | Purpose | Auth Required | Notes |
|------|---------|---------------|-------|
| `league` | League config, franchise list, roster settings, scoring rules | No | Fetch first — contains custom scoring/roster config |
| `rosters` | Current rosters with player IDs, salary, contract info | Yes | Includes salary and contract year data |
| `players` | Full player database (ID, name, position, team) | No | Changes once/day — cache aggressively |
| `playerScores` | Scores by week/season | No | Use `W=YTD` for season totals, `YEAR=XXXX` for past |
| `salaries` | Player salaries and contract details | Yes | May overlap with rosters — verify |
| `freeAgents` | Available free agents | Yes | Needed for contract tools context |
| `transactions` | Trades, adds, drops, IR moves | Yes | Filter by `DAYS`, `W` (week), `TYPE` |
| `leagueStandings` | Current standings | No | Useful for dashboard views |

### Nice-to-Have Endpoints
| TYPE | Purpose | Auth Required | Notes |
|------|---------|---------------|-------|
| `futureDraftPicks` | Draft pick ownership | Yes | Relevant for trade context |
| `draftResults` | Historical draft results | No | For draft history views |
| `rules` | Scoring rules detail | No | Machine-readable scoring config |
| `projectedScores` | Projected fantasy points | No | Nice for extension decision support |
| `injuries` | NFL injury report | No | Useful for roster context |

### Request Parameters
| Param | Purpose | Example |
|-------|---------|---------|
| `L` | League ID | `60206` |
| `JSON` | Response format (1=JSON) | `1` |
| `W` | Week number or YTD | `1`, `YTD` |
| `YEAR` | Season year | `2025` |
| `FRANCHISE_ID` | Filter to franchise | `0001` |
| `DAYS` | Transaction lookback | `7` |
| `APIKEY` | API key (alternative to cookie) | User-specific value |
</mfl_api_reference>

<open_questions>
## Open Questions

1. **API Key vs Cookie Auth — which is simpler for our use case?**
   - What we know: API key is tied to user/franchise/league combo, simpler to pass (query param). Cookie requires login flow.
   - What's unclear: Whether API key alone can access all endpoints we need (rosters, salaries, transactions). Docs say APIKEY works for "export" requests that "require both a league id and are access-restricted."
   - Recommendation: Try API key first during implementation. Fall back to cookie auth if any endpoint requires it.

2. **MFL Developer Registration — is it needed?**
   - What we know: Registered clients get ~2.5x higher rate limits. Registration requires phone validation and User-Agent header.
   - What's unclear: Whether unregistered limits are sufficient for a single-league sync (we only need ~10 requests per sync cycle).
   - Recommendation: Start without registration. If rate-limited during development, register.

3. **Historical data depth — how far back?**
   - What we know: EPV calculations need historical scores. Old app synced 2020-2024.
   - What's unclear: Exact years needed for EPV formula. Whether MFL keeps data for all past years.
   - Recommendation: Sync 2020-current during implementation. Confirm required range from EPV formula in Phase 4.

4. **Custom roster positions — what does ADL use?**
   - What we know: ADL has custom roster positions configured in MFL. The `league` endpoint returns this config.
   - What's unclear: Exact position names and slot counts until we fetch the data.
   - Recommendation: Fetch and log `league` endpoint early in development to understand the schema.
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- MFL API Documentation (api.myfantasyleague.com/2025/api_info) — Complete endpoint list, auth methods, rate limiting, request format
- Context7 /encode/httpx — Async client, cookies, custom auth, retry transport patterns
- Context7 /agronholm/apscheduler — FastAPI lifespan integration, AsyncScheduler, IntervalTrigger

### Secondary (MEDIUM confidence)
- Context7 /colin-b/pytest_httpx — Testing HTTP mocks (verified library exists with high benchmark score)
- ffscrapr docs (ffscrapr.ffverse.com) — Confirmed MFL auth uses cookie + API key approach, verified against MFL docs
- GitHub pymfl, python-mfl, python-myfantasyleague — Evaluated all Python wrappers; confirmed all are low-activity/unmaintained

### Tertiary (LOW confidence - needs validation)
- APScheduler 4.x SQLAlchemy data store — Example uses asyncpg; need to verify it works with our existing async SQLAlchemy setup
- tenacity retry for 429 handling — Pattern is standard, but need to test that MFL returns proper 429 status (vs silent throttle)
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: MFL REST API (direct HTTP)
- Ecosystem: httpx, APScheduler 4.x, tenacity, pytest-httpx
- Patterns: Thin async client, FastAPI lifespan scheduler, year-scoped API access
- Pitfalls: Rate limiting, cookie encoding, year-scoped URLs, stale caches, custom configs

**Confidence breakdown:**
- Standard stack: HIGH — httpx is established, APScheduler verified via Context7, all alternatives evaluated
- Architecture: HIGH — Patterns from official docs and established FastAPI conventions
- Pitfalls: HIGH — Rate limiting and auth documented in MFL API docs, custom config confirmed by user context
- Code examples: HIGH — From Context7 (httpx, APScheduler) and MFL API documentation
- MFL endpoint list: HIGH — Directly from MFL API reference page

**Research date:** 2026-03-10
**Valid until:** 2026-04-10 (30 days — MFL API is stable/slow-moving)
</metadata>

---

*Phase: 03-mfl-api-integration*
*Research completed: 2026-03-10*
*Ready for planning: yes*
