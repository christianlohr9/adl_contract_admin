# Codebase Concerns

**Analysis Date:** 2026-03-10

## Tech Debt

**Hardcoded Database Credentials:**
- Issue: Production database password exposed in source code as default values
- Files: `app/config/config.py` (lines 6-12), `docker-compose.yaml` (lines 9-15)
- Why: Rapid prototyping, `.env` integration deferred
- Impact: Credential compromise if repository is public or shared
- Fix approach: Move all secrets to `.env`, create `.env.example`, load via `python-dotenv`

**SQL Injection Vulnerabilities:**
- Issue: Table names directly interpolated in SQL strings via f-strings
- Files: `app/services/database_service.py` (lines 48, 116), `app/services/d.py` (line 29)
- Code: `query = f"SELECT * FROM {table}"`, `query = f"DROP TABLE IF EXISTS {table_name}"`
- Impact: Potential SQL injection if table names come from user input
- Fix approach: Use `psycopg2.sql.SQL()` for identifier quoting

**Wildcard Imports:**
- Issue: `from services.ffscrapr import *` makes dependencies unclear
- Files: `app/services/database_service.py` (line 11), `app/services/d.py` (line 12)
- Impact: Debugging difficulty, namespace pollution
- Fix approach: Use explicit imports

**Mixed DataFrame Libraries:**
- Issue: Polars and Pandas used interchangeably with frequent conversions
- Files: `app/services/database_service.py`, `app/services/data_processing.py`, `app/services/epv_calculations.py`
- Why: Polars for performance, Pandas for R interop and Taipy compatibility
- Impact: Conversion overhead, confusing data flow
- Fix approach: Standardize on one library where possible

**Repeated Database Connection Pattern:**
- Issue: Similar connection create/close pattern repeated in every database function
- Files: `app/services/database_service.py` (lines 96-130, 145-222, 237-283, 298-350)
- Impact: Code duplication, resource leak risk
- Fix approach: Create context manager for database connections

## Known Bugs

**Missing psycopg2.sql Import:**
- Symptoms: NameError when roster loading executes
- Files: `app/services/database_service.py` (line 314)
- Code: `cursor.execute(sql.SQL(...))` but `sql` not imported
- Workaround: None - function crashes at runtime
- Root cause: Missing `from psycopg2 import sql` import

**Type Mismatch in load_table_from_db:**
- Symptoms: AttributeError when function is called
- Files: `app/services/database_service.py` (line 44)
- Code: `conn = db_config` where `db_config` is a dict, then `conn.cursor()` called
- Workaround: None - function crashes at runtime
- Root cause: Parameter should be connection object, not config dict

**Parameter Mismatches in update_data.py:**
- Symptoms: Functions called with wrong argument types
- Files: `app/services/update_data.py` (lines 20-32)
- Trigger: Running batch data update script
- Root cause: `conn` passed where `db_config` expected

**Profanity in User Notification:**
- Symptoms: Inappropriate message shown to users
- Files: `app/services/epv_calculations.py` (line 187)
- Code: `notify(state, "success", f'Fuck this.')`
- Fix: Replace with professional notification message

**.gitignore Merge Conflict:**
- Symptoms: Unresolved git merge conflict markers in `.gitignore`
- Files: `.gitignore` (line 4)
- Fix: Resolve merge conflict markers

## Security Considerations

**Exposed Database Credentials:**
- Risk: Full database access if credentials are compromised
- Files: `app/config/config.py` - password in plaintext as default value
- Current mitigation: None
- Recommendations: Use `.env` file, remove defaults from source code, rotate credentials

**No Application Authentication:**
- Risk: Anyone with the URL can access and modify contract data
- Current mitigation: None (app is publicly accessible on Fly.io)
- Recommendations: Add authentication layer (Taipy supports auth)

**No Input Validation:**
- Risk: Invalid data could corrupt database
- Files: `app/services/database_service.py` - minimal validation
- Current mitigation: Basic null check on table names
- Recommendations: Validate all user inputs at UI boundary

## Performance Bottlenecks

**Row-wise Operations in EPV Calculations:**
- Problem: Using `.to_dicts()` and list comprehension instead of vectorized operations
- Files: `app/services/epv_calculations.py` (lines 164-168)
- Cause: Complex per-row logic converted to Python iteration
- Improvement path: Rewrite as Polars vectorized operations

**Nested Loop in Salary Grouping:**
- Problem: Manual iteration over grouped data with list appends
- Files: `app/services/epv_calculations.py` (lines 102-141)
- Cause: Complex grouping logic not expressed as DataFrame operations
- Improvement path: Use Polars native group_by with aggregation expressions

**No Database Connection Pooling:**
- Problem: Each function creates and closes a new database connection
- Files: `app/services/database_service.py` (all functions)
- Cause: Simple implementation without pool management
- Improvement path: Use psycopg2 connection pool or SQLAlchemy engine

## Fragile Areas

**EPV Calculations Module:**
- Files: `app/services/epv_calculations.py`
- Why fragile: 187-line function with complex financial formulas, magic numbers, no tests
- Common failures: Calculation errors silently produce wrong results
- Safe modification: Add tests first, then refactor
- Test coverage: None

**Database Service:**
- Files: `app/services/database_service.py`
- Why fragile: No transaction management, generic exception handling, no rollback
- Common failures: Partial writes on error, connection leaks
- Safe modification: Add context manager, transaction support
- Test coverage: None

## Missing Critical Features

**No Test Suite:**
- Problem: Zero automated tests for any functionality
- Current workaround: Manual testing only
- Blocks: Safe refactoring, regression detection, CI/CD
- Priority: High - especially for financial calculation logic

**No .env.example File:**
- Problem: New developers don't know which environment variables are needed
- Current workaround: Read `app/config/config.py` for defaults
- Fix: Create `.env.example` with all required variables (no secret values)

**Empty README:**
- Problem: No setup instructions, no documentation
- Files: `README.md` (20 bytes)
- Blocks: Onboarding, understanding project purpose

## Test Coverage Gaps

**All Code is Untested:**
- What's not tested: Everything
- Risk: Financial calculations could produce incorrect results, database operations could corrupt data
- Priority: High for `epv_calculations.py`, Medium for `database_service.py`
- Difficulty to test: EPV calculations are pure functions (easy to test), database ops need mocking

## Dependencies at Risk

**Taipy 3.1.0:**
- Risk: Relatively young framework, API may change significantly between versions
- Impact: UI and routing would need updates on upgrade
- Migration plan: Monitor Taipy releases, test upgrades in Docker first

**Scratch Files:**
- Files: `app/services/d.py`, `app/services/d.ipynb`
- Risk: Duplicate connection logic, unclear purpose, may confuse future development
- Fix: Remove or rename with clear purpose

---

*Concerns audit: 2026-03-10*
*Update as issues are fixed or new ones discovered*
