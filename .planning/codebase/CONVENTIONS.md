# Coding Conventions

**Analysis Date:** 2026-03-10

## Naming Patterns

**Files:**
- `snake_case.py` for all Python modules
- `__init__.py` for package markers
- Descriptive names: `database_service.py`, `epv_calculations.py`

**Functions:**
- `snake_case` consistently: `load_contracts()`, `filter_table()`, `calculate_epvs()`
- Verb prefixes: `get_*()` for retrievals, `load_*()` for data loading, `calculate_*()` for computations, `create_*()` for resource creation, `filter_*()` for data filtering, `delete_*()` for removals

**Variables:**
- `snake_case` for all variables
- DataFrame suffix: `contracts_df`, `filtered_df`, `salaries_df`
- Config dicts: `db_config`
- Taipy state: `state.selected_team`, `state.filtered_df`

**Constants:**
- `UPPER_SNAKE_CASE`: `START_YEAR`, `DEFAULT_SEASON`, `LEAGUE_ID`, `DEFAULT_TEAM`
- Located in `app/config/config.py`

**No classes used** - Entire codebase is functional/procedural

## Code Style

**Formatting:**
- No formatter configured (no Black, autopep8, or yapf)
- 4-space indentation (Python standard)
- Mixed quote styles (single and double used inconsistently)
- No enforced line length limit

**Linting:**
- No linter configured (no flake8, pylint, ruff)
- No pre-commit hooks

## Import Organization

**Order:**
- Not strictly followed
- Standard library, third-party, and local imports mixed

**Patterns:**
- Relative imports: `from services.database_service import load_contracts`
- Wildcard import present: `from services.ffscrapr import *` in `app/services/database_service.py`
- PYTHONPATH set to `/adl_contract_admin/app` in Docker for module resolution

## Error Handling

**Patterns:**
- Generic `except Exception as e:` with `logging.error()`
- No custom exception classes
- No transaction rollback on database errors
- User notifications via Taipy `notify(state, "success/error", message)`

**Gaps:**
- No input validation at boundaries
- No connection pool management
- Missing error recovery strategies

## Logging

**Framework:**
- Python `logging` module in `app/services/update_data.py`
- `print()` statements in other files
- No structured logging

**Patterns:**
- `logging.info()` for operations in batch scripts
- `logging.error()` for exceptions
- No log levels configured consistently

## Comments

**Language:**
- Bilingual: German and English comments mixed throughout
- German: `# Verbindung zur DB herstellen` (Connect to DB)
- English: `# Process and save franchise data`

**Docstrings:**
- Inconsistent: Some functions have Google-style docstrings (German), most have none
- Example with docstring: `load_table_from_db()` in `app/services/database_service.py`
- Example without: `filter_table()` in `app/services/data_processing.py`

**TODO Comments:**
- Not used (no TODO/FIXME found, but issues exist)

## Function Design

**Size:**
- Varies significantly: `calculate_epvs()` is ~187 lines
- No enforced limit

**Parameters:**
- No consistent pattern for parameter count
- Type hints partially applied: some functions annotated, some not
- Mutable default arguments present (list defaults)

**Return Values:**
- DataFrames as primary return type
- Some functions return None (side effects only)
- No Result/Optional patterns

## Module Design

**Exports:**
- No `__all__` declarations
- Wildcard imports used in some places
- No barrel file pattern

---

*Convention analysis: 2026-03-10*
*Update when patterns change*
