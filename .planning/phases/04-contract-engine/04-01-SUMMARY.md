---
phase: 04-contract-engine
plan: 01
type: summary
status: complete
---

# 04-01 Summary: Rules Loader & EPV Calculation Core

## Tasks Completed

### Task 1: Rules loader service and salary rounding helpers
- **Commit:** `a06c205` — `feat(04-01): create rules loader service and salary rounding helpers`
- **File:** `src/app/services/rules.py`
- Cached JSON/YAML loaders with `@lru_cache`
- Convenience wrappers for all rules files
- Decimal-based rounding: `round_to_10k`, `round_to_100k`, `floor_100k`, `ceil_100k`, `round_to_nearest_4`
- Season-specific getters with year fallback: `get_veteran_minimum`, `get_rookie_minimum`, `get_sd_minimum`, `get_salary_growth_rate`

### Task 2: EPV calculation core
- **Commit:** `96fe1cc` — `feat(04-01): create EPV calculation core`
- **File:** `src/app/services/epv.py`
- `EPVResult` dataclass with all EPV fields
- `get_position_rank()` — dense rank by YTD total points
- `is_robust_season()` — checks >= 8 active weeks
- `calculate_performance_salary()` — SAL-based formula with PR=1 linear extrapolation
- `calculate_epv()` — orchestrator with 75%/82.5% floor logic per bylaws

## Files Created
- `src/app/services/rules.py` (155 lines)
- `src/app/services/epv.py` (277 lines)

## Deviations
- None

## Issues
- None
