# Roadmap: ADL Contract Admin

## Overview

Transform the ADL's Google Sheet-based contract administration into a proper web application. Starting with structured rule extraction from the bylaws, building a FastAPI backend with PostgreSQL, integrating MFL league data, implementing the contract calculation engine (EPV extensions, tags, tenders, buyouts), exposing REST endpoints, and delivering a functional React UI for all 32 GMs.

## Domain Expertise

None

## Milestones

- ✅ **v1.0 MVP** - Phases 1-8 (shipped 2026-03-12)
- 🚧 **v1.1 League Calendar & Contract Management** - Phases 9-13 (in progress)

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

<details>
<summary>✅ v1.0 MVP (Phases 1-8) - SHIPPED 2026-03-12</summary>

### Phase 1: Rules Extraction
**Goal**: Parse the ADL Bylaws into structured, machine-readable formats — markdown docs in `rules/docs/`, JSON constants in `rules/constants/`, YAML formulas in `rules/formulas/`
**Depends on**: Nothing (first phase)
**Research**: Unlikely (internal document parsing, established patterns)
**Plans**: 3 plans

Plans:
- [x] 01-01: Verify rule constants against bylaws
- [x] 01-02: Verify rule formulas against bylaws
- [x] 01-03: Extract rule formulas into YAML files

### Phase 2: Foundation
**Goal**: Set up the new project structure — archive old Taipy app, scaffold FastAPI backend, define PostgreSQL schema with SQLAlchemy models and Alembic migrations
**Depends on**: Phase 1 (need rule constants/formulas to inform schema design)
**Research**: Unlikely (standard project setup, established tech stack)
**Plans**: 3 plans

Plans:
- [x] 02-01: Archive old app, create new project structure
- [x] 02-02: FastAPI app, async DB, Docker Compose
- [x] 02-03: SQLAlchemy models and Alembic migrations

### Phase 3: MFL API Integration
**Goal**: Research MFL API access approach (direct HTTP vs ffscrapr) and implement data sync module for franchises, rosters, and player scores
**Depends on**: Phase 2 (need DB models to sync into)
**Research**: Likely (external API, technology choice)
**Research topics**: MFL API documentation, direct HTTP vs ffscrapr/R approach, authentication method, available endpoints for franchises/rosters/scores
**Plans**: 4 plans

Plans:
- [x] 03-01: MFL API client and response models
- [x] 03-02: Implement MFL data sync module
- [x] 03-03: Sync rosters, contracts, and player scores
- [x] 03-04: Background sync scheduler and manual trigger endpoint

### Phase 4: Contract Engine
**Goal**: Port EPV calculation logic from old codebase and build the full contract tools engine — extensions (X-A/B), franchise/transition tags (X-C), ERFA tenders (X-D), buyouts/restructures (X-E)
**Depends on**: Phase 2 (need models), Phase 1 (need rule formulas/constants)
**Research**: Unlikely (porting existing logic from old codebase)
**Plans**: 4 plans

Plans:
- [x] 04-01: Port EPV calculation logic to new service layer
- [x] 04-02: Extensions engine (X-A/B)
- [x] 04-03: Franchise/transition tags (X-C) and ERFA tenders (X-D)
- [x] 04-04: Buyouts and restructures (X-E)

### Phase 5: Salary Cap & Validation
**Goal**: Implement salary cap penalty calculations (NG/SD/FG contract types) and contract eligibility validation against bylaws rules
**Depends on**: Phase 4 (need contract engine), Phase 1 (need rule constants)
**Research**: Unlikely (rules derived from bylaws, internal logic)
**Plans**: 2 plans

Plans:
- [x] 05-01: Salary cap penalty calculations (NG/SD/FG)
- [x] 05-02: Contract eligibility validation

### Phase 6: API Layer
**Goal**: Build FastAPI REST endpoints for players, teams, contracts, extensions, and salary cap
**Depends on**: Phase 4, Phase 5 (need contract engine and cap logic)
**Research**: Unlikely (standard FastAPI patterns)
**Plans**: 3 plans

Plans:
- [x] 06-01: Player and team endpoints
- [x] 06-02: Contract and extension endpoints
- [x] 06-03: Salary cap endpoints

### Phase 7: Frontend Placeholder
**Goal**: Scaffold React/TypeScript frontend with routing, layout shell, and placeholder pages for all major views
**Depends on**: Phase 6 (need API to define page structure)
**Research**: Unlikely (standard React scaffold)
**Plans**: 2 plans

Plans:
- [x] 07-01: React scaffold with routing and layout
- [x] 07-02: Placeholder pages for all views

### Phase 8: Frontend UI
**Goal**: Build functional React UI — contract tools forms, salary cap views, team dashboards, player search, and data tables for all 32 GMs
**Depends on**: Phase 6 (need working API), Phase 7 (need scaffold)
**Research**: Likely (UI/UX decisions, component library choice)
**Research topics**: React component library selection, data table library, dashboard layout patterns, form handling for contract tools
**Plans**: 4 plans

Plans:
- [x] 08-01: Component library setup and shared components
- [x] 08-02: Roster browsing and player search
- [x] 08-03: Player detail page with contract tools
- [x] 08-04: Salary cap and dashboard views

</details>

## Phase Details

### 🚧 v1.1 League Calendar & Contract Management (In Progress)

**Milestone Goal:** Add date-aware contract management with roster-wide visibility into all available actions, resolving ISS-001 (extension window awareness)

#### Phase 9: League Calendar Data Model
**Goal**: Admin-configured season dates per year — oEXT deadline, tag/tender deadlines, B/R deadline, 5YO deadline, iEXT window, all auction dates. Stored as manual config with CRUD endpoints.
**Depends on**: v1.0 complete
**Research**: Unlikely (internal data model, standard CRUD)
**Plans**: 2 plans

Plans:
- [x] 09-01: SeasonCalendar model, migration, and CRUD endpoints
- [x] 09-02: Calendar admin UI with date entry form

#### Phase 10: Period Detection & Date-Aware Eligibility
**Goal**: Backend logic to determine current league period from configured dates, gate contract tools by window, enforce date constraints in eligibility checks.
**Depends on**: Phase 9
**Research**: Unlikely (internal business logic, extends existing eligibility services)
**Plans**: 2 plans

Plans:
- [x] 10-01: Window status service and eligibility integration
- [x] 10-02: API response enhancement with window status

#### Phase 11: Roster-Wide Eligibility API
**Goal**: New endpoint returning contract action eligibility summary for all players on a team — tag candidates, tender candidates, extension-eligible, B/R candidates, 5YO/PPE eligible — with calculated values.
**Depends on**: Phase 10
**Research**: Unlikely (aggregates existing per-player endpoints, internal patterns)
**Plans**: 1 plan

Plans:
- [x] 11-01: Roster eligibility service, schemas, and endpoint

#### Phase 12: Contract Management Dashboard
**Goal**: Frontend roster view grouped by action type (tags, tenders, extensions, B/R, 5YO/PPE) with inline calculations. Compare candidates side-by-side without clicking into individual players.
**Depends on**: Phase 11
**Research**: Unlikely (follows existing frontend patterns from Phase 8)
**Plans**: TBD

Plans:
- [ ] 12-01: TBD (run /gsd:plan-phase 12 to break down)

#### Phase 13: Calendar/Timeline & Deadline Countdowns
**Goal**: Dedicated calendar page showing all league dates/periods/auction sequence on a visual timeline, plus dashboard countdown widgets and period indicators.
**Depends on**: Phase 9
**Research**: Likely (timeline/calendar visualization library selection)
**Research topics**: React timeline/calendar components, visual period rendering approaches
**Plans**: TBD

Plans:
- [ ] 13-01: TBD (run /gsd:plan-phase 13 to break down)

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 1. Rules Extraction | v1.0 | 3/3 | Complete | 2026-03-10 |
| 2. Foundation | v1.0 | 3/3 | Complete | 2026-03-10 |
| 3. MFL API Integration | v1.0 | 4/4 | Complete | 2026-03-10 |
| 4. Contract Engine | v1.0 | 4/4 | Complete | 2026-03-11 |
| 5. Salary Cap & Validation | v1.0 | 2/2 | Complete | 2026-03-11 |
| 6. API Layer | v1.0 | 3/3 | Complete | 2026-03-11 |
| 7. Frontend Placeholder | v1.0 | 2/2 | Complete | 2026-03-11 |
| 8. Frontend UI | v1.0 | 4/4 | Complete | 2026-03-12 |
| 9. League Calendar Data Model | v1.1 | 2/2 | Complete | 2026-03-12 |
| 10. Period Detection & Date-Aware Eligibility | v1.1 | 2/2 | Complete | 2026-03-12 |
| 11. Roster-Wide Eligibility API | v1.1 | 1/1 | Complete | 2026-03-12 |
| 12. Contract Management Dashboard | v1.1 | 0/? | Not started | - |
| 13. Calendar/Timeline & Deadline Countdowns | v1.1 | 0/? | Not started | - |
