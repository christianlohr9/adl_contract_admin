# Project Milestones: ADL Contract Admin

## v1.2 Data Integrity & Eligibility Accuracy (Shipped: 2026-03-13)

**Delivered:** Historical data imports, systematic eligibility audit fixing 12 discrepancies, NFL kickoff rule, and full regression validation with 0 anomalies across 879 players

**Phases completed:** 14-17 (8 plans total)

**Key accomplishments:**

- Historical data import pipeline with gap detection for multi-season player scores and contracts
- Non-blocking startup backfill with adaptive rate limiting and status tracking API
- Comprehensive bylaw-to-code eligibility audit identifying 12 discrepancies and 3 missing implementations
- Fixed all eligibility discrepancies: UDFA max-years, PK/PN grouping, NFL RFA prices, re-signed player checks, PR Starter Floor
- NFL kickoff eligibility gate for rookie/UDFA final-year extensions (resolving UAT-001)
- Full regression validation: 879 players, 6,153 checks, 0 anomalies across all 32 teams

**Stats:**

- 39 files created/modified
- ~16,404 lines of code (9,722 Python + 6,682 TypeScript)
- 4 phases, 8 plans, ~53 min execution time
- 1 day (2026-03-13)

**Git range:** `feat(14-01)` → `feat(17-01)`

**What's next:** TBD — discuss next milestone goals

---

## v1.1 League Calendar & Contract Management (Shipped: 2026-03-13)

**Delivered:** Date-aware contract management with roster-wide eligibility visibility, deadline countdowns, and admin-configurable league calendar

**Phases completed:** 9-13 (9 plans total)

**Key accomplishments:**
- SeasonCalendar data model with admin UI for 27 configurable league date fields
- Window status service gating all 7 contract actions by configured deadline dates
- Roster-wide eligibility API aggregating contract action options across entire team roster
- Contract Management Dashboard with dynamic eligibility table and eligible-only toggle
- Urgency-colored deadline countdown cards with days-remaining awareness
- Fixed ISS-002: tag/tender eligibility season queries for expired contracts

**Stats:**
- 54 files created/modified
- ~14,964 lines of code (8,282 Python + 6,682 TypeScript)
- 5 phases, 9 plans, 81 min execution time
- 2 days from v1.0 to v1.1 ship

**Git range:** `feat(09-01)` → `feat(13-02)`

**What's next:** TBD — discuss next milestone goals

---

## v1.0 MVP (Shipped: 2026-03-12)

**Delivered:** Full-stack contract administration app replacing the Google Sheet — EPV extensions, franchise tags, tenders, buyouts, salary cap calculations, and functional React UI for all 32 GMs

**Phases completed:** 1-8 (26 plans total)

**Key accomplishments:**
- Structured ADL Bylaws into machine-readable rules (MD docs, JSON constants, YAML formulas)
- FastAPI backend with PostgreSQL, SQLAlchemy models, and Alembic migrations
- MFL API integration with data sync for franchises, rosters, contracts, and player scores
- Complete contract engine: EPV extensions, franchise/transition tags, ERFA tenders, buyouts, 5YO, PPE
- Salary cap penalty calculations and contract eligibility validation
- React/TypeScript frontend with roster browsing, player detail pages, contract tools, and dashboard

**Stats:**
- 211 files created/modified
- ~14,964 lines of code at ship (8,282 Python + 6,682 TypeScript)
- 8 phases, 26 plans, 142 min execution time
- 3 days from start to ship (2026-03-10 → 2026-03-12)

**Git range:** `feat(01-01)` → `feat(08-04)`

**What's next:** v1.1 League Calendar & Contract Management

---
