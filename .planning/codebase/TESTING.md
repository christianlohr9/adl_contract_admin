# Testing Patterns

**Analysis Date:** 2026-03-10

## Test Framework

**Status: NO TESTING INFRASTRUCTURE EXISTS**

- No test framework installed (pytest, unittest not in dependencies)
- No test files found (`test_*.py`, `*_test.py`)
- No test directories (`tests/`, `__tests__/`)
- No test configuration (`pytest.ini`, `conftest.py`, `tox.ini`)
- No CI/CD pipeline to run tests

## Test File Organization

Not applicable - no tests exist.

The `.gitignore` includes standard Python testing entries (`.pytest_cache/`, `.coverage`, `htmlcov/`) suggesting testing was considered but never implemented.

## Test Structure

Not applicable.

## Mocking

Not applicable.

## Fixtures and Factories

Not applicable.

## Coverage

**Requirements:**
- No coverage targets
- No coverage tooling configured

## Test Types

**Unit Tests:** None
**Integration Tests:** None
**E2E Tests:** None

## Critical Functions Needing Tests

**High Priority:**
- `calculate_new_salary()` in `app/services/epv_calculations.py` - Complex financial calculation with growth rates
- `calculate_epvs()` in `app/services/epv_calculations.py` - 187-line transformation with salary formulas
- `calculate_smoothed_salary()` in `app/services/epv_calculations.py` - Salary smoothing algorithm

**Medium Priority:**
- `load_table_from_db()` in `app/services/database_service.py` - Core data loading
- `filter_table()` in `app/services/data_processing.py` - Data filtering logic
- `calculate_and_save_contracts()` in `app/services/database_service.py` - Contract processing pipeline

**Lower Priority:**
- `get_unique_teams()`, `get_seasons()` in `app/services/data_processing.py` - Simple data extraction
- `ff_connect()` in `app/services/ffscrapr.py` - R integration wrapper

---

*Testing analysis: 2026-03-10*
*Update when test patterns change*
