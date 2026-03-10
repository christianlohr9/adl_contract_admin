# Roadmap: ADL Contract Admin

## Overview

Transform the ADL's Google Sheet-based contract administration into a proper web application. Starting with structured rule extraction from the bylaws, building a FastAPI backend with PostgreSQL, integrating MFL league data, implementing the contract calculation engine (EPV extensions, tags, tenders, buyouts), exposing REST endpoints, and delivering a functional React UI for all 32 GMs.

## Domain Expertise

None

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Rules Extraction** - Parse bylaws into structured docs, constants, and formulas
- [x] **Phase 2: Foundation** — Complete
- [ ] **Phase 3: MFL API Integration** - In progress (3/4 plans)
- [ ] **Phase 4: Contract Engine** - Port EPV logic, build all contract tools
- [ ] **Phase 5: Salary Cap & Validation** - Cap penalties and eligibility checks
- [ ] **Phase 6: API Layer** - FastAPI REST endpoints for all resources
- [ ] **Phase 7: Frontend Placeholder** - React/TypeScript scaffold with routing
- [ ] **Phase 8: Frontend UI** - Functional UI for contract tools and dashboards

## Phase Details

### Phase 1: Rules Extraction
**Goal**: Parse the ADL Bylaws into structured, machine-readable formats — markdown docs in `rules/docs/`, JSON constants in `rules/constants/`, YAML formulas in `rules/formulas/`
**Depends on**: Nothing (first phase)
**Research**: Unlikely (internal document parsing, established patterns)
**Plans**: TBD

Plans:
- [x] 01-01: Verify rule constants against bylaws
- [x] 01-02: Verify rule formulas against bylaws
- [x] 01-03: Extract rule formulas into YAML files

### Phase 2: Foundation
**Goal**: Set up the new project structure — archive old Taipy app, scaffold FastAPI backend, define PostgreSQL schema with SQLAlchemy models and Alembic migrations
**Depends on**: Phase 1 (need rule constants/formulas to inform schema design)
**Research**: Unlikely (standard project setup, established tech stack)
**Plans**: TBD

Plans:
- [x] 02-01: Archive old app, create new project structure
- [x] 02-02: FastAPI app, async DB, Docker Compose
- [x] 02-03: SQLAlchemy models and Alembic migrations

### Phase 3: MFL API Integration
**Goal**: Research MFL API access approach (direct HTTP vs ffscrapr) and implement data sync module for franchises, rosters, and player scores
**Depends on**: Phase 2 (need DB models to sync into)
**Research**: Likely (external API, technology choice)
**Research topics**: MFL API documentation, direct HTTP vs ffscrapr/R approach, authentication method, available endpoints for franchises/rosters/scores
**Plans**: TBD

Plans:
- [x] 03-01: MFL API client and response models
- [x] 03-02: Implement MFL data sync module
- [x] 03-03: Sync rosters, contracts, and player scores
- [ ] 03-04: Background sync scheduler and manual trigger endpoint

### Phase 4: Contract Engine
**Goal**: Port EPV calculation logic from old codebase and build the full contract tools engine — extensions (X-A/B), franchise/transition tags (X-C), ERFA tenders (X-D), buyouts/restructures (X-E)
**Depends on**: Phase 2 (need models), Phase 1 (need rule formulas/constants)
**Research**: Unlikely (porting existing logic from old codebase)
**Plans**: TBD

Plans:
- [ ] 04-01: Port EPV calculation logic to new service layer
- [ ] 04-02: Extensions engine (X-A/B)
- [ ] 04-03: Franchise/transition tags (X-C) and ERFA tenders (X-D)
- [ ] 04-04: Buyouts and restructures (X-E)

### Phase 5: Salary Cap & Validation
**Goal**: Implement salary cap penalty calculations (NG/SD/FG contract types) and contract eligibility validation against bylaws rules
**Depends on**: Phase 4 (need contract engine), Phase 1 (need rule constants)
**Research**: Unlikely (rules derived from bylaws, internal logic)
**Plans**: TBD

Plans:
- [ ] 05-01: Salary cap penalty calculations (NG/SD/FG)
- [ ] 05-02: Contract eligibility validation

### Phase 6: API Layer
**Goal**: Build FastAPI REST endpoints for players, teams, contracts, extensions, and salary cap
**Depends on**: Phase 4, Phase 5 (need contract engine and cap logic)
**Research**: Unlikely (standard FastAPI patterns)
**Plans**: TBD

Plans:
- [ ] 06-01: Player and team endpoints
- [ ] 06-02: Contract and extension endpoints
- [ ] 06-03: Salary cap endpoints

### Phase 7: Frontend Placeholder
**Goal**: Scaffold React/TypeScript frontend with routing, layout shell, and placeholder pages for all major views
**Depends on**: Phase 6 (need API to define page structure)
**Research**: Unlikely (standard React scaffold)
**Plans**: TBD

Plans:
- [ ] 07-01: React scaffold with routing and layout
- [ ] 07-02: Placeholder pages for all views

### Phase 8: Frontend UI
**Goal**: Build functional React UI — contract tools forms, salary cap views, team dashboards, player search, and data tables for all 32 GMs
**Depends on**: Phase 6 (need working API), Phase 7 (need scaffold)
**Research**: Likely (UI/UX decisions, component library choice)
**Research topics**: React component library selection, data table library, dashboard layout patterns, form handling for contract tools
**Plans**: TBD

Plans:
- [ ] 08-01: Component library setup and shared components
- [ ] 08-02: Contract tools UI (extensions, tags, tenders, buyouts)
- [ ] 08-03: Salary cap and team dashboard views
- [ ] 08-04: Player search and data tables

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Rules Extraction | 3/3 | Complete | 2026-03-10 |
| 2. Foundation | 3/3 | Complete | 2026-03-10 |
| 3. MFL API Integration | 3/4 | In progress | - |
| 4. Contract Engine | 0/4 | Not started | - |
| 5. Salary Cap & Validation | 0/2 | Not started | - |
| 6. API Layer | 0/3 | Not started | - |
| 7. Frontend Placeholder | 0/2 | Not started | - |
| 8. Frontend UI | 0/4 | Not started | - |
