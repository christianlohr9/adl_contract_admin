# Phase 16: NFL Kickoff Rule - Research

**Researched:** 2026-03-13
**Domain:** NFL schedule data integration for kickoff-based eligibility gating
**Confidence:** HIGH

<research_summary>
## Summary

Researched NFL schedule data sources and architecture patterns for integrating a kickoff-based eligibility gate into the existing contract extension system. The phase is a surgical addition — not a complex integration.

**Key finding:** The existing `regular_season_start` field on `SeasonCalendar` already captures the NFL kickoff date. No new schema, no external API integration, and no new libraries are needed. The implementation is a single eligibility check added to `check_extension_eligibility()` plus frontend messaging.

For admin convenience, the ESPN public API provides a free, unauthenticated endpoint that returns Week 1 dates — useful for optional "prefill" but explicitly out of scope per CONTEXT.md.

**Primary recommendation:** Use the existing `regular_season_start` field in SeasonCalendar as the kickoff gate. Add one check to extensions.py for rookie/UDFA contracts in their final year. Surface clear messaging in the UI showing the unlock date.
</research_summary>

<standard_stack>
## Standard Stack

### Core (Already in Place)

| Component | Location | Purpose | Status |
|-----------|----------|---------|--------|
| SeasonCalendar model | `src/app/models/season_calendar.py` | Stores `regular_season_start` per season | Exists, field populated |
| Extension eligibility | `src/app/services/extensions.py:130-217` | `check_extension_eligibility()` — where kickoff check goes | Exists, needs one more check |
| Window status service | `src/app/services/window_status.py` | Date-based gating patterns | Exists, reusable pattern |
| Eligibility dispatcher | `src/app/services/eligibility.py` | Unified eligibility API | Exists, surfaces reasons to frontend |

### No New Libraries Needed

This phase requires zero new dependencies. The implementation uses:
- Existing SQLAlchemy model fields
- Existing date comparison patterns from `window_status.py`
- Existing eligibility result structure (`EligibilityResult`)

### External Data Sources (For Reference / Future Use)

| Source | Auth Required | Cost | Data Quality | Notes |
|--------|--------------|------|--------------|-------|
| ESPN Scoreboard API | No | Free | HIGH | `site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?seasontype=2&week=1` — returns exact kickoff dates per game |
| NFL Operations | No | Free | HIGH | `operations.nfl.com/gameday/nfl-schedule/` — official dates page, not machine-readable |
| nflreadpy (Python) | No | Free | HIGH | `nflreadpy.load_schedules(season)` — returns Polars DataFrame with game dates. PyPI: `nflreadpy` |
| nfl_data_py (Python) | No | Free | HIGH | `nfl_data_py.import_schedules([season])` — similar, older package. PyPI: `nfl-data-py` |
| SportsDataIO | API Key | Paid | HIGH | Professional-grade, overkill for one date per year |

**Recommended if auto-prefill ever needed:** ESPN Scoreboard API — free, no auth, returns structured JSON with Week 1 game dates. A single GET request gives the Thursday kickoff date.
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Pattern 1: Add Kickoff Check to Existing Eligibility Flow

**What:** Insert one additional eligibility rule into `check_extension_eligibility()` — after the rookie/UDFA contract detection (line ~174) and before the EXT cooldown check (line ~188).

**When to use:** When the contract is a rookie or UDFA contract AND the player is in the final year (years_remaining <= 1).

**Logic:**
```python
# After existing rookie/UDFA detection (is_rookie_contract, is_udfa_contract)
if is_rookie_contract or is_udfa_contract:
    # Existing max-years check...

    # NEW: Kickoff gating for final-year rookie/UDFA contracts
    # "Players on Drafted Rookie or UDFA contracts are ineligible
    #  for EXTs until NFL games kick off in the final year of their contract."
    if contract.years_remaining <= 1:  # final year
        calendar = await _get_season_calendar(session, season)
        if calendar and calendar.regular_season_start:
            today = date.today()
            if today < calendar.regular_season_start:
                return (
                    False,
                    f"Rookie/UDFA extension unavailable until NFL kickoff "
                    f"({calendar.regular_season_start})"
                )
        elif calendar is None or calendar.regular_season_start is None:
            # No kickoff date configured — block conservatively
            return (
                False,
                "Rookie/UDFA extension requires NFL season start date "
                "(not yet configured in league calendar)"
            )
```

### Pattern 2: Reuse Window Status Pattern for Messaging

**What:** The frontend already handles `WindowStatus` with status/reason/closes fields. The kickoff block should follow the same pattern for consistent UX.

**Key insight:** The kickoff rule is an *eligibility* check (player-specific), not a *window* check (league-wide). It belongs in `check_extension_eligibility()`, not in `window_status.py`. But the **messaging pattern** should mirror window status for consistency.

### Pattern 3: Frontend Greyed-Out Treatment

**What:** When eligibility returns `(False, reason)` with a kickoff-related reason, the frontend should:
1. Grey out the extension action
2. Display the reason with the specific unlock date
3. Distinguish from permanently-ineligible (greyed + lock icon) vs. time-gated (greyed + clock icon + date)

### Anti-Patterns to Avoid

- **Separate kickoff_date field:** Don't add a new `nfl_kickoff_date` column — `regular_season_start` already represents this. One source of truth.
- **Window status for per-player checks:** Don't put player-specific eligibility logic in the window status service. Windows are league-wide; eligibility is per-player.
- **Fetching NFL schedules at runtime:** Don't hit external APIs during eligibility checks. The date is set once per year by an admin.
- **Per-team kickoff dates:** The bylaws mention "that player's game" for the iEXT window (Week 1-16), but the general kickoff rule just says "NFL games kick off." Use a single league-wide date. Game-level granularity is explicitly out of scope per CONTEXT.md.
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| NFL kickoff date storage | New model/table | `SeasonCalendar.regular_season_start` | Field already exists and is populated |
| Date-based gating logic | New gating framework | Pattern from `window_status.py` `_check_deadline()` | Proven pattern, consistent behavior |
| Eligibility reason messaging | New messaging system | Existing `(bool, str)` return from `check_extension_eligibility()` | Already flows through to frontend |
| NFL schedule data fetching | Custom scraper/API client | Admin manual entry (or ESPN API if ever needed) | One date per year doesn't justify an integration |

**Key insight:** This is a ~20-line code change to an existing function plus frontend messaging. The temptation is to over-engineer with NFL API integration, per-team schedules, or a separate kickoff management system. Resist — the bylaws specify one simple rule, and the existing infrastructure already supports it.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Blocking Too Many Actions
**What goes wrong:** Applying the kickoff gate to ALL extensions instead of only rookie/UDFA final-year contracts.
**Why it happens:** Misreading "Players on Drafted Rookie or UDFA contracts" as "all players."
**How to avoid:** The check must be nested inside the `if is_rookie_contract or is_udfa_contract:` block and only when `years_remaining <= 1` (final year).
**Warning signs:** Veteran players on standard contracts getting blocked from offseason extensions.

### Pitfall 2: Confusing "Final Year" Definition
**What goes wrong:** Not counting the contract year correctly. A player with `years_remaining=0` is expired (no contract). A player with `years_remaining=1` is in the final year.
**Why it happens:** Off-by-one in interpreting "final year of their contract."
**How to avoid:** The rule says "until NFL games kick off in the final year." A player in the final year (`years_remaining=1`) who hasn't yet reached kickoff is blocked. A player with `years_remaining=0` has an expired contract — different rule applies.
**Warning signs:** Players with expired contracts being incorrectly blocked or final-year players not being blocked.

### Pitfall 3: Missing the Calendar Data
**What goes wrong:** Silently allowing extensions when `regular_season_start` is NULL.
**Why it happens:** Not handling the unconfigured case.
**How to avoid:** If no kickoff date is set, block conservatively with a clear message ("kickoff date not configured"). Admin should always set this.
**Warning signs:** Extensions going through in offseason for rookie/UDFA players when they shouldn't.

### Pitfall 4: Timezone Issues with Kickoff Date
**What goes wrong:** The Thursday night kickoff is actually late evening — a `date` comparison might allow extensions on kickoff day before the game.
**Why it happens:** Using `date` type loses time-of-day precision.
**How to avoid:** Per the bylaws and CONTEXT.md, we only need date-level granularity. The kickoff date is the day games start. If today >= kickoff_date, the season has started. This is consistent with how all other calendar checks work in the system.
**Warning signs:** None expected — date granularity is fine for this use case.

### Pitfall 5: Per-Player vs. League-Wide Kickoff
**What goes wrong:** Trying to implement per-team kickoff dates (some teams play Thursday, most play Sunday).
**Why it happens:** The bylaws say "of that player's game" in the iEXT window rule.
**How to avoid:** That wording applies to the iEXT window boundary (Week 1-16), NOT the general kickoff eligibility gate. The general rule says "until NFL games kick off" (plural, league-wide). CONTEXT.md explicitly excludes game-level granularity. Use a single date.
**Warning signs:** Requirement creep toward per-team schedules.
</common_pitfalls>

<code_examples>
## Code Examples

### Current Extension Eligibility Flow (Where Kickoff Check Goes)
```python
# Source: src/app/services/extensions.py:165-186
# Existing rookie/UDFA detection — kickoff check goes here

desig = contract.designation or ""
is_rookie_contract = any(
    f"{contract.signed_season} {pick}" in desig
    for pick in [f"{r}." for r in range(1, 6)]
)
is_udfa_contract = "UDFA" in desig

if is_rookie_contract or is_udfa_contract:
    # Existing max-years check...

    # >>> INSERT KICKOFF CHECK HERE <<<
    # Check if in final year AND before NFL kickoff
```

### Existing Calendar Lookup Pattern (Reuse)
```python
# Source: src/app/services/window_status.py:215-217
result = await session.execute(
    select(SeasonCalendar).where(SeasonCalendar.season == season)
)
calendar = result.scalar_one_or_none()
```

### Existing SeasonCalendar Field (Already Present)
```python
# Source: src/app/models/season_calendar.py:94-96
regular_season_start: Mapped[date | None] = mapped_column(
    Date, comment="NFL regular season start",
)
```

### ESPN API for Admin Reference (Optional / Future)
```python
# Free, no auth, returns Week 1 kickoff date
# GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?seasontype=2&week=1&dates=2025
#
# Response includes events[0].date = "2025-09-05T00:20Z" (Thursday kickoff)
# Min(event.date for event in events) = NFL kickoff date for the season
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| NFL kickoff always Thursday after Labor Day | 2026 season starts on Wednesday Sep 9 | 2026 | Admin must check the actual date each year — can't hardcode "first Thursday in September" |
| Regular season = 16 games | Regular season = 18 weeks / 17 games | 2021 | No impact on kickoff date, but relevant to iEXT window (Week 16 endpoint) |

**NFL Schedule Data Landscape (2025-2026):**
- **ESPN public API** remains the best free option — no auth, structured JSON, reliable
- **nflreadpy** (Python, via nflverse) supersedes `nfl_data_py` for Python NFL data access
- **NFL official site** publishes dates at `operations.nfl.com` but is not machine-readable
- **2026 NFL schedule release** expected May 13-15, 2026 — kickoff date known by then

**Key dates for reference:**
- 2025 NFL regular season: September 4, 2025 – January 4, 2026
- 2026 NFL regular season: September 9, 2026 (Wednesday opener, first time since 2012)
</sota_updates>

<open_questions>
## Open Questions

1. **iEXT window "of that player's game" wording**
   - What we know: The bylaws say the iEXT window runs "between Week 1 kickoff and Week 16 kickoff *of that player's game*"
   - What's unclear: Does this mean the iEXT window end should be per-team (each player's Week 16 game date)?
   - Recommendation: Out of scope per CONTEXT.md. The existing `iext_window_end` field handles the window boundary. The per-player granularity could be a future enhancement if GMs request it.

2. **Conservative blocking when calendar unconfigured**
   - What we know: If `regular_season_start` is NULL, we should block to prevent false allows
   - What's unclear: Will admins always have this populated before it matters?
   - Recommendation: Block with clear messaging. Admin can resolve by setting the date. Add a note in the season calendar admin UI.
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- `rules/docs/contract_tools.md` lines 19, 23 — Bylaws text defining kickoff eligibility rule
- `src/app/models/season_calendar.py` lines 94-96 — Existing `regular_season_start` field
- `src/app/services/extensions.py` lines 130-217 — Existing eligibility check to modify
- `src/app/services/window_status.py` — Existing date-gating patterns to follow
- `.planning/phases/16-nfl-kickoff-rule/16-CONTEXT.md` — Phase scope and constraints

### Secondary (MEDIUM confidence)
- ESPN Scoreboard API (`site.api.espn.com`) — Verified returns Week 1 game dates, no auth required
- NFL Operations Important Dates (`operations.nfl.com`) — 2025-2026 season dates confirmed
- Wikipedia NFL Kickoff article — Historical kickoff date patterns verified

### Tertiary (LOW confidence - needs validation)
- 2026 Wednesday opener date (Sep 9) — reported by multiple sources but NFL hasn't published final 2026 schedule yet
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: Existing Python/SQLAlchemy codebase — no new tech
- Ecosystem: NFL schedule data sources (ESPN API, nflreadpy, nfl_data_py)
- Patterns: Eligibility gating, date comparison, frontend messaging
- Pitfalls: Over-scoping, wrong action mapping, missing calendar data

**Confidence breakdown:**
- Standard stack: HIGH — all components already exist in codebase
- Architecture: HIGH — clear insertion point, established patterns
- Pitfalls: HIGH — derived from bylaws analysis and codebase review
- Code examples: HIGH — from actual codebase files

**Research date:** 2026-03-13
**Valid until:** 2026-04-13 (30 days — simple date-based logic, nothing fast-moving)
</metadata>

---

*Phase: 16-nfl-kickoff-rule*
*Research completed: 2026-03-13*
*Ready for planning: yes*
