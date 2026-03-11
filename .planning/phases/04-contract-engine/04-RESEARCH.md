# Phase 4: Contract Engine - Research

**Researched:** 2026-03-11
**Domain:** Python calculation engine — porting EPV logic + building contract tools
**Confidence:** HIGH

<research_summary>
## Summary

Researched the existing EPV calculation codebase (old Taipy app), the Phase 1 rules extraction (constants/formulas/docs), and the current Phase 2-3 service architecture to understand how to build the contract engine.

The old EPV code in `archive/` uses Polars DataFrames with complex window functions and salary interpolation. The new implementation should use the existing SQLAlchemy models + async session pattern, loading rules from `rules/constants/` and `rules/formulas/` YAML/JSON files. Each contract tool becomes a standalone async service function following the established pattern.

Key finding: The old code conflates data aggregation (building a "contracts" table from player scores) with EPV calculation. In the new architecture, player scores and contracts are already synced (Phase 3). The engine only needs to calculate — not aggregate raw data.

**Primary recommendation:** Build each contract tool as a self-contained async service that reads rules from YAML/JSON, queries existing models, and returns structured result dataclasses. No new libraries needed — this is pure business logic using existing stack.
</research_summary>

<standard_stack>
## Standard Stack

No new libraries required. Phase 4 uses the existing stack:

### Core (Already Installed)
| Library | Purpose | Why |
|---------|---------|-----|
| SQLAlchemy 2.x (async) | Query players, contracts, scores | Already in use for sync services |
| Pydantic v2 | Input validation + result schemas | Already in use for MFL models |
| Python Decimal | Salary calculations | Avoid float rounding — contracts.yaml specifies ROUND_TO_10K |
| PyYAML | Load rules/formulas | Already a dependency for rules loading |

### Supporting (Already Installed)
| Library | Purpose | When to Use |
|---------|---------|-------------|
| structlog | Calculation audit logging | Trace EPV calculations for debugging |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Decimal for money | `py-moneyed` or `money` lib | Overkill — we only need precision rounding, not currency conversion |
| Raw YAML loading | Pydantic Settings from YAML | Could add type safety to rules, but adds coupling — rules already validated in Phase 1 |
| DataFrames (Polars/Pandas) | SQLAlchemy queries | Old code uses Polars; new code should use ORM queries since data is already in DB |

**Installation:** None needed — all dependencies already present.
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Service Structure
```
src/app/services/
├── team_sync.py              # (existing)
├── player_sync.py            # (existing)
├── roster_sync.py            # (existing)
├── score_sync.py             # (existing)
├── sync_orchestrator.py      # (existing)
├── rules.py                  # NEW: Load rules from YAML/JSON
├── epv.py                    # NEW: EPV calculation core
├── extensions.py             # NEW: Extension tools (X-A/B)
├── franchise_tags.py         # NEW: EFT/NEFT/TT (X-C)
├── tenders.py                # NEW: ERFA/RFA tenders (X-D)
└── buyouts.py                # NEW: Buyouts/restructures (X-E)
```

### Pattern 1: Rules Loader Service
**What:** Single module that loads and caches rules from YAML/JSON files at startup
**When to use:** Any calculation that needs constants or formulas
**Why:** Avoids re-reading files on every request; single source of truth

```python
# rules.py
from functools import lru_cache
from pathlib import Path
import json, yaml

RULES_DIR = Path(__file__).parent.parent.parent.parent / "rules"

@lru_cache
def load_constants(name: str) -> dict:
    """Load rules/constants/{name}.json"""
    with open(RULES_DIR / "constants" / f"{name}.json") as f:
        return json.load(f)

@lru_cache
def load_formulas(name: str) -> dict:
    """Load rules/formulas/{name}.yaml"""
    with open(RULES_DIR / "formulas" / f"{name}.yaml") as f:
        return yaml.safe_load(f)

def get_salary_cap(season: int) -> dict:
    cap = load_constants("salary_cap")
    return cap["seasons"].get(str(season), cap["seasons"]["2025"])

def get_contract_rules() -> dict:
    return load_constants("contracts")

def get_epv_formulas() -> dict:
    return load_formulas("contracts")
```

### Pattern 2: Calculation Service with Result Dataclass
**What:** Each tool returns a typed result dataclass, not raw dicts
**When to use:** All contract tools
**Why:** Matches SyncResult pattern from Phase 3; type-safe, serializable

```python
# Example: extensions.py
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class ExtensionOption:
    extension_years: int
    eys: Decimal            # Extension Year Salary
    smoothed_salary: Decimal # Final annual salary
    total_value: Decimal     # Total contract value
    contract_type: str       # SD or FG

@dataclass
class ExtensionResult:
    player_id: int
    player_name: str
    current_salary: Decimal
    current_years: int
    options: list[ExtensionOption]
    epv_details: dict        # EPV breakdown for transparency
    errors: list[str]

async def calculate_extensions(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> ExtensionResult:
    """Calculate all valid extension options for a player."""
    ...
```

### Pattern 3: EPV as Internal Helper, Not Standalone Service
**What:** EPV calculation is called BY contract tools, not exposed separately
**When to use:** Extensions, 5YO, PPE calculations all need EPV
**Why:** Per CONTEXT.md — "EPV logic is embedded within the tools that need it, not a separate abstraction layer"

```python
# epv.py — internal helpers, called by extensions.py/franchise_tags.py/etc.
async def calculate_epv(
    session: AsyncSession,
    player_id: int,
    season: int,
) -> EPVResult:
    """Core EPV calculation used by multiple tools."""
    # 1. Get player's position rank history
    # 2. Interpolate salary from position salary table
    # 3. Return EPV values (curr, new, old)
    ...
```

### Pattern 4: Salary Rounding Helpers
**What:** Centralized rounding functions matching bylaws specifications
**When to use:** Every salary calculation
**Why:** Bylaws specify different rounding rules (ROUND_TO_10K, ROUND_TO_100K, CEIL_100K, FLOOR_100K)

```python
from decimal import Decimal, ROUND_HALF_UP

def round_to_10k(amount: Decimal) -> Decimal:
    return (amount / 10_000).quantize(Decimal("1"), ROUND_HALF_UP) * 10_000

def round_to_100k(amount: Decimal) -> Decimal:
    return (amount / 100_000).quantize(Decimal("1"), ROUND_HALF_UP) * 100_000

def ceil_100k(amount: Decimal) -> Decimal:
    return -(-amount // 100_000) * 100_000

def floor_100k(amount: Decimal) -> Decimal:
    return (amount // 100_000) * 100_000
```

### Anti-Patterns to Avoid
- **Using Polars/Pandas for calculations:** Old code used DataFrames because data wasn't in a DB. Now it is — use SQLAlchemy queries.
- **Building a generic "calculation engine":** Each tool has unique logic. Don't abstract prematurely — build each tool separately and extract common helpers only when duplication is clear.
- **Hardcoding constants:** All values come from rules/ files. No magic numbers in service code.
- **Committing transactions in services:** Follow Phase 3 pattern — services flush, callers commit.
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Money arithmetic | Float math | Python `Decimal` | Bylaws specify precision rounding (ROUND_TO_10K); floats introduce errors |
| Salary interpolation | Custom interpolation | Direct DB query with ORDER BY + LIMIT | Old code built salary lookup tables in memory; DB can do this natively |
| Position rank calculations | In-memory ranking | SQL window functions (rank/dense_rank) | SQLAlchemy supports window functions; more efficient than Python sorting |
| Rules loading | Inline constants | `rules/` YAML/JSON files | Phase 1 extracted these specifically to be machine-readable |
| Contract eligibility checks | Ad-hoc conditionals | Structured eligibility rules from bylaws docs | Complex eligibility has many edge cases — systematize, don't scatter |

**Key insight:** The old code hand-rolled everything because it used Polars DataFrames in memory. The new architecture has a proper DB with indexed queries and window functions — lean on the database instead of reimplementing in Python.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Float Rounding Errors in Salary Calculations
**What goes wrong:** $1,861,333.33... rounds to $1.86m or $1.87m depending on float representation
**Why it happens:** Using Python floats for money arithmetic
**How to avoid:** Use `Decimal` for all salary math. Convert from float only at input boundaries (DB reads). The Contract model currently uses `float` for salary — calculations should convert to Decimal immediately.
**Warning signs:** Salaries off by $10k from expected values

### Pitfall 2: Confusing Old Code Logic with Bylaws Rules
**What goes wrong:** Porting a bug from old code thinking it's a feature
**Why it happens:** Old code has known discrepancies with bylaws (documented in Phase 1: e.g., EPV floor was 100% in old code, bylaws says 75%)
**How to avoid:** Use `rules/formulas/` as source of truth, NOT old `epv_calculations.py`. Old code is implementation reference only — bylaws win on any discrepancy.
**Warning signs:** Phase 1 decision log lists specific discrepancies (01-02: "Old code uses 100% floor; bylaws specifies 75%")

### Pitfall 3: Missing the "Robust" Season Filter
**What goes wrong:** EPV calculated from seasons with too few games, giving unreliable rankings
**Why it happens:** Not filtering for `is_robust` (≥5 games) before calculating position ranks
**How to avoid:** Always filter scores for minimum game threshold before ranking. The old code explicitly checks `is_robust == 1`.
**Warning signs:** Position ranks that seem off for players who missed significant time

### Pitfall 4: Incorrect Growth Rate Compounding
**What goes wrong:** Salary smoothing produces wrong values
**Why it happens:** Applying 10% growth additively instead of multiplicatively, or starting from wrong year index
**How to avoid:** Growth is compound: `salary × 1.1^year_index`. Previous years start at index 0. Extension years continue the index sequence.
**Warning signs:** Smoothed salary doesn't match manual spreadsheet verification

### Pitfall 5: Extension Year Salary Multiplier Off-by-One
**What goes wrong:** EYS multiplier is wrong for players with 5YO
**Why it happens:** The 5YO adjustment subtracts 1 from effective extension years: `1.15 - 0.05 × (ext_yrs - YO5)`. Missing the YO5 flag shifts all multipliers.
**How to avoid:** Track 5YO status from contract designation. Bylaws specifies: "Effective EXT years for multiplier = (total_ext_years - 1)" when 5YO is exercised.
**Warning signs:** 5YO + extension salaries don't match expected tiers

### Pitfall 6: Salary Cap Percentage Constants
**What goes wrong:** Franchise tag calculations use wrong salary averages
**Why it happens:** EFT uses top 5 salaries, TT uses top 10 — easy to mix up
**How to avoid:** Load from `rules/formulas/free_agency.yaml` which specifies exactly which top-N salaries each tag type uses.
**Warning signs:** Tag prices significantly higher/lower than expected
</common_pitfalls>

<code_examples>
## Code Examples

### EPV Calculation Core (ported from old code, adapted for SQLAlchemy)
```python
# Source: archive/app/services/epv_calculations.py, adapted for new architecture
from decimal import Decimal
from sqlalchemy import select, func
from src.app.models.player_score import PlayerScore
from src.app.models.contract import Contract

GROWTH_RATE = Decimal("1.10")

async def get_position_salary_at_rank(
    session: AsyncSession,
    position: str,
    rank: int,
    season: int,
) -> Decimal:
    """Get the salary at a given position rank for EPV interpolation.

    EPV formula: ROUND_TO_10K(AVERAGE(SAL(2×PR-3), SAL(2×PR-2)))
    where SAL(n) = nth highest ADL salary at position.
    """
    # Query salaries ranked by position for the season
    stmt = (
        select(Contract.salary)
        .where(Contract.season == season)
        .join(Contract.player)
        .where(Player.position == position)
        .order_by(Contract.salary.desc())
        .offset(rank - 1)
        .limit(1)
    )
    result = await session.execute(stmt)
    salary = result.scalar_one_or_none()
    return Decimal(str(salary)) if salary else Decimal("0")


async def calculate_epv_at_rank(
    session: AsyncSession,
    position: str,
    pr: int,  # position rank
    season: int,
) -> Decimal:
    """Calculate EPV for a position rank.

    Formula: ROUND_TO_10K(AVERAGE(SAL(2×PR-3), SAL(2×PR-2)))
    """
    rank_a = 2 * pr - 3
    rank_b = 2 * pr - 2
    sal_a = await get_position_salary_at_rank(session, position, rank_a, season)
    sal_b = await get_position_salary_at_rank(session, position, rank_b, season)
    avg = (sal_a + sal_b) / 2
    return round_to_10k(avg)
```

### Extension Year Salary (EYS) Calculation
```python
# Source: rules/formulas/contracts.yaml + archive EPV code
def calculate_eys(
    epv_curr: Decimal | None,
    epv_new: Decimal,
    epv_old: Decimal | None,
    floor: Decimal,
    extension_years: int,
    has_5yo: bool = False,
) -> Decimal:
    """Calculate Extension Year Salary.

    EYS = MAX(EPV_curr, EPV_new, EPV_old, floor) × (1.15 - 0.05 × EXT_years)

    Floor = 75% of previous salary (82.5% if contract expired)
    5YO adjustment: effective ext_years = ext_years - 1
    """
    candidates = [epv_new, floor]
    if epv_curr is not None:
        candidates.append(epv_curr)
    if epv_old is not None:
        candidates.append(epv_old)

    max_epv = max(candidates)

    effective_years = extension_years - (1 if has_5yo else 0)
    multiplier = Decimal("1.15") - Decimal("0.05") * effective_years

    return round_to_10k(max_epv * multiplier)
```

### Salary Smoothing (New Salary Calculation)
```python
# Source: archive/app/services/epv_calculations.py calculate_new_salary()
def calculate_smoothed_salary(
    current_salary: Decimal,
    eys: Decimal,
    prev_years: int,
    ext_years: int,
) -> Decimal:
    """Smooth salary across combined contract period.

    Combines past salary (with growth) and future EYS (with growth),
    then divides by total growth factor to get level annual salary.
    """
    growth = Decimal("1.10")
    total_years = prev_years + ext_years

    # Past years: current salary compounded
    past_total = sum(current_salary * growth ** i for i in range(prev_years))

    # Future years: EYS compounded (continuing from prev_years index)
    future_total = sum(eys * growth ** i for i in range(prev_years, total_years))

    # Denominator: sum of all growth factors
    denominator = sum(growth ** i for i in range(total_years))

    combined = past_total + future_total
    return round_to_10k(combined / denominator)
```

### Franchise Tag Price Calculation
```python
# Source: rules/formulas/free_agency.yaml
async def calculate_franchise_tag_price(
    session: AsyncSession,
    position: str,
    tag_type: str,  # "EFT", "NEFT", "TT"
    prev_salary: Decimal,
    season: int,
) -> Decimal:
    """Calculate franchise tag salary.

    EFT/NEFT: MAX(ADL_Cap% × AVG(Top5_Salaries_EOY), 120% × prev_salary)
    TT: MAX(ADL_Cap% × AVG(Top10_Salaries_EOY), 120% × prev_salary)
    """
    top_n = 5 if tag_type in ("EFT", "NEFT") else 10

    stmt = (
        select(func.avg(Contract.salary))
        .where(Contract.season == season - 1)  # End of prior year
        .join(Contract.player)
        .where(Player.position == position)
        .order_by(Contract.salary.desc())
        .limit(top_n)
    )
    # Note: This is simplified — actual implementation needs subquery for top-N avg

    avg_top = Decimal(str(await session.execute(stmt).scalar() or 0))
    cap_pct_value = avg_top  # Simplified — actual formula uses ADL cap percentage
    floor_value = Decimal("1.20") * prev_salary

    return round_to_10k(max(cap_pct_value, floor_value))
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach (Archive Code) | Current Approach (New Architecture) | Impact |
|----------------------------|-------------------------------------|--------|
| Polars DataFrames in memory | SQLAlchemy async ORM queries | Data already in DB; no need to load everything into memory |
| R/ffscrapr via rpy2 | Direct MFL HTTP API (Phase 3) | Eliminates R dependency entirely |
| Taipy GUI state management | FastAPI service layer + result dataclasses | Clean separation; no UI coupling in calculations |
| Float arithmetic for salaries | Decimal for all money math | Eliminates rounding errors the old code likely had |
| Hardcoded constants | rules/ YAML/JSON files | Phase 1 extracted everything; calculations reference structured files |
| Manual "contracts" table aggregation | PlayerScore + Contract models already synced | Phase 3 sync handles data ingestion; engine only calculates |

**Key architectural shift:** The old code did data pipeline + calculation in one pass. The new architecture separates:
1. **Data sync** (Phase 3, done) — MFL → DB
2. **Calculation** (Phase 4, this phase) — DB → results
3. **API** (Phase 6, future) — results → REST

This separation means the contract engine is purely computational — it reads from DB and returns results. No data fetching, no state management.
</sota_updates>

<open_questions>
## Open Questions

1. **Position rank calculation: use synced scores or compute on-the-fly?**
   - What we know: Old code computed ranks from playerscores table via aggregation. New architecture has PlayerScore model with per-week scores.
   - What's unclear: Should position ranks be pre-computed during sync (new column on Player or Contract) or calculated on-demand by the engine?
   - Recommendation: Calculate on-demand using SQL window functions. Avoid adding computed columns to sync — keeps sync simple and ranks always fresh.

2. **Salary table for EPV interpolation: which salaries?**
   - What we know: Old code uses "roster" table salaries ranked by position. New architecture has Contract model with salary field.
   - What's unclear: Should EPV use current active contract salaries, or end-of-year snapshots? The formula references "ADL salary at position" which implies current contracts.
   - Recommendation: Use active Contract records for the relevant season. This matches what Phase 3 syncs from MFL rosters.

3. **Contract model salary: float vs Decimal**
   - What we know: Contract.salary is currently defined as `float` (Numeric(6,2) in DB). Calculations need Decimal precision.
   - What's unclear: Should we change the model to Decimal, or convert at calculation boundaries?
   - Recommendation: Convert to Decimal at calculation input boundaries. Changing the model type is a Phase 2 concern and may require migration. The DB stores Numeric which is precise — only the Python representation differs.

4. **"Robust" season flag: where does it come from?**
   - What we know: Old code checks `is_robust == 1` (player played ≥5 games). This was computed in `calculate_and_save_contracts()` which aggregated scores.
   - What's unclear: The new PlayerScore model has individual game scores but no aggregated "is_robust" flag.
   - Recommendation: Compute robustness on-the-fly: `COUNT(PlayerScore WHERE season=X AND player_id=Y) >= 5`. SQL is fast enough for this.
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- `archive/app/services/epv_calculations.py` — Full old EPV code (on archive/old-taipy-app branch)
- `archive/app/services/data_processing.py` — Old data loading utilities
- `archive/app/services/database_service.py` — Old contract aggregation logic
- `rules/formulas/contracts.yaml` — EPV and extension formulas (Phase 1 extraction)
- `rules/formulas/free_agency.yaml` — Franchise tag, RFA, buyout formulas
- `rules/formulas/salary_cap.yaml` — Cap calculation formulas
- `rules/constants/contracts.json` — Contract type definitions, year limits, designations
- `rules/constants/salary_cap.json` — Cap values, SD minimum, rollover limits
- `rules/constants/league.json` — League structure, weeks, positions
- `rules/docs/contracts.md` — Contract eligibility rules, signing windows, edge cases
- `rules/docs/free_agency.md` — Franchise tag, RFA, buyout detailed rules
- `src/app/models/` — Current SQLAlchemy models (all 8 models)
- `src/app/services/` — Current sync service patterns

### Secondary (MEDIUM confidence)
- Old code's growth rate (1.10), EYS formula (1.15 - 0.05×years), salary interpolation — verified against rules/formulas/contracts.yaml

### Tertiary (LOW confidence - needs validation)
- Position rank salary interpolation details — old code uses `SAL(2×PR-3), SAL(2×PR-2)` which needs verification against bylaws during implementation
- "Robust" threshold of 5 games — from old code, not explicitly in extracted rules
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: Python async services with SQLAlchemy
- Ecosystem: No new libraries needed — existing stack sufficient
- Patterns: Service-per-tool, result dataclasses, rules loader, Decimal arithmetic
- Pitfalls: Float rounding, old code bugs, growth rate compounding, 5YO edge cases

**Confidence breakdown:**
- Standard stack: HIGH — no new dependencies, all existing
- Architecture: HIGH — follows established patterns from Phase 3
- Pitfalls: HIGH — documented discrepancies from Phase 1, clear rounding concerns
- Code examples: MEDIUM — adapted from old code + rules, needs validation during implementation

**Research date:** 2026-03-11
**Valid until:** 2026-04-11 (30 days — internal domain, stable)
</metadata>

---

*Phase: 04-contract-engine*
*Research completed: 2026-03-11*
*Ready for planning: yes*
