# ADL Contract Admin

## What This Is

A full-stack web application that automates contract management for the Analytics Dynasty League (ADL), a 32-team fantasy football dynasty league. Replaces the Google Sheet-based "Contract Admin" workflow with a FastAPI/PostgreSQL backend and React/TypeScript frontend. Features EPV-based contract extensions, franchise/transition tags, ERFA/RFA tenders, buyouts, salary cap calculations, date-aware eligibility gating via admin-configurable league calendar, and a contract management dashboard with roster-wide eligibility visibility. All 32 GMs have access.

## Core Value

Accurate, automated contract extension calculations (EPV-based) that eliminate manual rule interpretation and spreadsheet formulas — the single most time-consuming commissioner task.

## Requirements

### Validated

- ✓ Parse and structure ADL Bylaws into markdown docs, JSON constants, YAML formulas — v1.0
- ✓ New project structure separating docs, rules, backend, frontend — v1.0
- ✓ FastAPI backend with SQLAlchemy models and Alembic migrations — v1.0
- ✓ PostgreSQL schema (Team, Player, Contract, SalaryCap, SeasonCalendar) — v1.0/v1.1
- ✓ MFL API integration via direct HTTP (replaced ffscrapr/R approach) — v1.0
- ✓ EPV calculation logic ported to new service layer — v1.0
- ✓ Contract Tools engine: Extensions, Tags, Tenders, Buyouts, 5YO, PPE — v1.0
- ✓ Contract eligibility validation against bylaws rules — v1.0
- ✓ Salary cap penalty calculations (NG/SD/FG) — v1.0
- ✓ FastAPI endpoints: players, teams, contracts, extensions, salary cap, calendar, eligibility — v1.0/v1.1
- ✓ React/TypeScript frontend with roster browsing, player detail, contract tools, dashboard — v1.0
- ✓ Admin-configurable league calendar with 27 date fields — v1.1
- ✓ Date-aware eligibility gating via window status service — v1.1
- ✓ Roster-wide eligibility API aggregating all contract actions — v1.1
- ✓ Contract Management Dashboard with dynamic eligibility table — v1.1
- ✓ Deadline countdown cards with urgency awareness — v1.1

### Active

- [ ] Historical player score import pipeline (multi-season)
- [ ] Multi-season contract history import
- [ ] NFL kickoff eligibility rule for Drafted Rookie/UDFA contracts
- [ ] Calendar timeline visualization (visual period rendering)

### Out of Scope

- Trade logic and draft pick management — deferred to future milestone
- User authentication — open access, 32 GMs trust-based
- Deployment configuration — developing locally, deployment target TBD
- Offline mode — real-time data is core value

## Context

- **Shipped v1.0 + v1.1**: Full-stack app with 14,964 LOC (8,282 Python + 6,682 TypeScript)
- **Tech stack**: Python/FastAPI/PostgreSQL/SQLAlchemy/Alembic backend; React/TypeScript/Vite/shadcn-ui frontend; MD/JSON/YAML for rule data
- **Bylaws source of truth**: `Analytics Dynasty League Bylaws 2025.md` — all rules derived from this document
- **League**: 32 teams, 2 conferences, MFL league ID 60206
- **MFL API**: Direct HTTP via httpx (replaced R/ffscrapr approach)
- **Database**: PostgreSQL with Docker Compose (dev on port 5432, test on 5433)
- **Old codebase**: Archived to `archive/` — EPV logic fully ported
- **Known data gaps**: Historical player scores and multi-season contract data not yet imported

## Constraints

- **Bylaws authority**: All contract rules, formulas, and constants MUST be derived from the bylaws document. If a rule is unclear, mark TODO — never guess.
- **Tech stack**: Python/FastAPI/PostgreSQL/SQLAlchemy/Alembic backend; React/TypeScript frontend; MD/JSON/YAML for rule data
- **Phase numbering**: Next phase starts at 14 (continuing from v1.1)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Replace Taipy with FastAPI | Taipy is limited for multi-user API; FastAPI is standard for Python REST APIs | ✓ Good |
| Fresh DB schema | Old schema has issues (type mismatches, no proper relations); clean start is faster than migration | ✓ Good |
| Bylaws as single source of truth | Prevents rule drift between code and official rules | ✓ Good |
| Rules in MD/JSON/YAML | Separates human-readable docs from machine-readable config; enables non-code rule updates | ✓ Good |
| No auth in v1 | 32 GMs need access; auth adds complexity without blocking core value | ✓ Good |
| Direct HTTP for MFL API | Eliminated R/rpy2/ffscrapr dependency; httpx provides async HTTP with redirect handling | ✓ Good |
| uv for package management | Faster, modern, single tool for deps+venvs | ✓ Good |
| APScheduler 4.x for async scheduling | v3.x lacks async support needed for FastAPI integration | ✓ Good |
| Tailwind v4 CSS-first config | Modern approach, no config file needed | ✓ Good |
| shadcn/ui with Base UI render props | New shadcn v4 API pattern | ✓ Good |
| Tool-centric window gating (no abstract period layer) | Simpler, each action checks its own calendar fields | ✓ Good |
| SeasonCalendar with 27 nullable date fields | Commissioner fills progressively; all fields optional | ✓ Good |
| Dynamic eligibility columns by window status | Only shows relevant contract actions, reduces noise | ✓ Good |

---
*Last updated: 2026-03-13 after v1.1 milestone*
