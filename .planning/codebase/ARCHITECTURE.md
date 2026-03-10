# Architecture

**Analysis Date:** 2026-03-10

## Pattern Overview

**Overall:** Layered Monolithic Web Application

**Key Characteristics:**
- Single deployment unit (Docker container on Fly.io)
- Clear vertical slicing: UI -> Business Logic -> Data Access
- Synchronous request handling
- Functional/procedural code style (no classes)
- Taipy GUI framework for web interface

## Layers

**Presentation Layer (Pages):**
- Purpose: Define UI layouts using Taipy DSL markup
- Contains: Page template strings with Taipy components
- Location: `app/pages/home.py`, `app/pages/extension.py`, `app/pages/evp.py`
- Depends on: Nothing (pure markup strings)
- Used by: `app/main.py` (route registration)

**Application Layer (Main):**
- Purpose: Route handling, state management, user interaction orchestration
- Contains: Navigation logic, event handlers, state transitions
- Location: `app/main.py`
- Depends on: Pages, Services
- Used by: Taipy GUI framework

**Business Logic Layer (Services):**
- Purpose: Data transformation, calculations, orchestration
- Contains: Filtering, EPV calculations, data pipeline
- Location: `app/services/data_processing.py`, `app/services/epv_calculations.py`
- Depends on: Data Access layer, Config
- Used by: Application layer

**Data Access Layer:**
- Purpose: Database operations, external API access
- Contains: PostgreSQL queries, R package wrappers
- Location: `app/services/database_service.py`, `app/services/ffscrapr.py`
- Depends on: Config, psycopg2, rpy2
- Used by: Business Logic layer

**Configuration Layer:**
- Purpose: Centralized configuration and constants
- Contains: Database credentials, league constants, defaults
- Location: `app/config/config.py`
- Depends on: Environment variables (os.getenv)
- Used by: All layers

## Data Flow

**User Interaction Flow:**

1. User opens app -> `app/main.py` initializes Taipy GUI
2. Initial data loads: `get_unique_teams()` -> `load_contracts()` -> PostgreSQL
3. Home page renders team/season selection dropdowns
4. User clicks "Filtern" -> `filter_and_navigate()` called
5. `filter_table(team, season)` filters contracts DataFrame
6. Navigate to "extension" page with filtered data
7. User edits contract years, clicks "EPVs berechnen"
8. `calculate_epvs(state)` runs complex salary calculations
9. Navigate to "epv" page with calculated results

**Data Pipeline Flow (batch update via `app/services/update_data.py`):**

1. MFL API (via ffscrapr R package) -> Raw data
2. R DataFrame -> pandas2ri conversion -> Polars DataFrame
3. Polars DataFrame -> PostgreSQL (Supabase)

**State Management:**
- Taipy `state` object carries all UI state between pages
- Key state vars: `filtered_df`, `selected_team`, `selected_season`, `selected_weeks`
- No persistent client-side state

## Key Abstractions

**Service Functions:**
- Purpose: Encapsulate business operations as standalone functions
- Examples: `load_contracts()`, `filter_table()`, `calculate_epvs()`
- Pattern: Pure functional (no classes, module-level functions)

**DataFrames as Primary Data Structure:**
- Purpose: Tabular data representation throughout the app
- Examples: `contracts_df`, `filtered_df`, `salaries_df`
- Pattern: Polars for processing, Pandas for R interop and Taipy display

**Taipy Pages:**
- Purpose: UI layout definitions
- Examples: `home_page`, `extension_page`, `evp_page`
- Pattern: String-based DSL with variable bindings

## Entry Points

**Primary - Web Application:**
- Location: `app/main.py`
- Triggers: `python app/main.py` or Docker CMD
- Responsibilities: Initialize Taipy GUI, register routes ("/", "home", "extension", "epv"), start server on 0.0.0.0:8080

**Secondary - Data Update Script:**
- Location: `app/services/update_data.py`
- Triggers: Manual execution for batch data refresh
- Responsibilities: Load franchises, rosters, contracts, player scores from MFL API into database

## Error Handling

**Strategy:** Generic try/except with logging, re-raise

**Patterns:**
- `except Exception as e:` with `logging.error()` in database functions
- No transaction rollback on failure
- No custom error types
- User notifications via Taipy `notify()` function

## Cross-Cutting Concerns

**Logging:**
- Python `logging` module in `app/services/update_data.py`
- `print()` statements elsewhere (no structured logging)

**Validation:**
- Minimal: basic null checks on table names in `database_service.py`
- No input validation at UI boundary

**Authentication:**
- None (application has no user auth)

---

*Architecture analysis: 2026-03-10*
*Update when major patterns change*
