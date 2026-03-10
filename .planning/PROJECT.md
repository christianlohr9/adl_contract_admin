# ADL Contract Admin

## What This Is

A web application that automates contract management for the Analytics Dynasty League (ADL), a 32-team fantasy football dynasty league. It replaces the existing Google Sheet-based "Contract Admin" workflow with a proper backend API and database, eliminating manual calculations for contract extensions, salary cap penalties, franchise tags, and other contract tools. All 32 GMs will have access.

## Core Value

Accurate, automated contract extension calculations (EPV-based) that eliminate manual rule interpretation and spreadsheet formulas — the single most time-consuming commissioner task.

## Requirements

### Validated

- ✓ MFL API data sync (franchises, rosters, player scores) — existing via old Taipy app
- ✓ EPV calculation logic — existing in `app/services/epv_calculations.py`
- ✓ PostgreSQL data storage (contracts, rosters, franchises, playerscores) — existing via Supabase

### Active

- [ ] Parse and structure the ADL Bylaws into separate markdown docs (`rules/docs/`)
- [ ] Extract rule constants into JSON (`rules/constants/`)
- [ ] Extract rule formulas into YAML (`rules/formulas/`)
- [ ] New project structure separating docs, rules, backend, frontend
- [ ] FastAPI backend with SQLAlchemy models and Alembic migrations
- [ ] PostgreSQL schema for: Team, Player, Contract, SalaryCap, Extension, FranchiseTag, Transaction
- [ ] MFL API integration module (`backend/app/mfl/`) — research whether direct HTTP or ffscrapr
- [ ] Port EPV calculation logic from old `epv_calculations.py` to new service layer
- [ ] Contract Tools engine: Extensions (X-A/B), Franchise/Transition Tags (X-C), ERFA Tenders (X-D), Buyouts/Restructures (X-E)
- [ ] Contract eligibility validation against bylaws rules
- [ ] Salary cap penalty calculations (NG/SD/FG contract types)
- [ ] FastAPI endpoints: players, teams, contracts, extensions, salary cap
- [ ] React frontend placeholder pages (no functional UI in v1)

### Out of Scope

- Functional React UI — v1 is API-only, frontend is placeholder only
- Scheduled/automatic MFL sync — manual trigger only in v1
- Trade logic and draft pick management — deferred to v2
- User authentication — no auth in v1, open access
- Deployment configuration — develop locally first, deployment target TBD

## Context

- **Existing codebase**: Old Taipy-based app in `app/` directory (to be archived to `archive/`)
- **Bylaws source of truth**: `Analytics Dynasty League Bylaws 2025.md` — all rules MUST come from this document; never invent rules; mark unclear rules with TODO
- **Contract Admin Sheet**: `2026 ADL Contract Admin.xlsx` — the Google Sheet being replaced; EXT tab contains the core EPV extension calculator
- **League**: 32 teams, 2 conferences, MFL league ID 60206
- **EPV logic**: Port from existing `app/services/epv_calculations.py` — use old code as implementation reference
- **MFL API**: Currently accessed via R/rpy2/ffscrapr; needs research whether direct HTTP calls are feasible as replacement
- **Database**: Fresh PostgreSQL start (new schema), old Supabase DB is abandoned
- **Users**: All 32 GMs will access the tool; no auth required in v1

## Constraints

- **Bylaws authority**: All contract rules, formulas, and constants MUST be derived from the bylaws document. If a rule is unclear, mark TODO — never guess.
- **Tech stack**: Python/FastAPI/PostgreSQL/SQLAlchemy/Alembic backend; React/TypeScript frontend; MD/JSON/YAML for rule data
- **Old code as reference**: The EPV calculation logic in `epv_calculations.py` should be ported, not rewritten from scratch
- **MFL API research needed**: Must determine if direct HTTP replaces R/ffscrapr before implementing

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Replace Taipy with FastAPI | Taipy is limited for multi-user API; FastAPI is standard for Python REST APIs | — Pending |
| Fresh DB schema | Old schema has issues (type mismatches, no proper relations); clean start is faster than migration | — Pending |
| Bylaws as single source of truth | Prevents rule drift between code and official rules | — Pending |
| Rules in MD/JSON/YAML | Separates human-readable docs from machine-readable config; enables non-code rule updates | — Pending |
| No auth in v1 | 32 GMs need access; auth adds complexity without blocking core value | — Pending |
| Archive old app/ | Keep as reference for porting EPV logic; remove later | — Pending |
| MFL API approach | TBD — needs research phase to determine direct HTTP vs ffscrapr | — Pending |

---
*Last updated: 2026-03-10 after initialization*
