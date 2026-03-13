# Roadmap: ADL Contract Admin

## Overview

Transform the ADL's Google Sheet-based contract administration into a proper web application. Starting with structured rule extraction from the bylaws, building a FastAPI backend with PostgreSQL, integrating MFL league data, implementing the contract calculation engine (EPV extensions, tags, tenders, buyouts), exposing REST endpoints, and delivering a functional React UI for all 32 GMs.

## Domain Expertise

None

## Milestones

- ✅ **v1.0 MVP** - Phases 1-8 (shipped 2026-03-12)
- ✅ [v1.1 League Calendar & Contract Management](milestones/v1.1-ROADMAP.md) (Phases 9-13) — SHIPPED 2026-03-13
- 🚧 **v1.2 Data Integrity & Eligibility Accuracy** - Phases 14-17 (in progress)

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

<details>
<summary>✅ v1.0 MVP (Phases 1-8) — SHIPPED 2026-03-12</summary>

- [x] Phase 1: Rules Extraction (3/3 plans) — completed 2026-03-10
- [x] Phase 2: Foundation (3/3 plans) — completed 2026-03-10
- [x] Phase 3: MFL API Integration (4/4 plans) — completed 2026-03-10
- [x] Phase 4: Contract Engine (4/4 plans) — completed 2026-03-11
- [x] Phase 5: Salary Cap & Validation (2/2 plans) — completed 2026-03-11
- [x] Phase 6: API Layer (3/3 plans) — completed 2026-03-11
- [x] Phase 7: Frontend Placeholder (2/2 plans) — completed 2026-03-11
- [x] Phase 8: Frontend UI (4/4 plans) — completed 2026-03-12

</details>

<details>
<summary>✅ v1.1 League Calendar & Contract Management (Phases 9-13) — SHIPPED 2026-03-13</summary>

- [x] Phase 9: League Calendar Data Model (2/2 plans) — completed 2026-03-12
- [x] Phase 10: Period Detection & Date-Aware Eligibility (2/2 plans) — completed 2026-03-12
- [x] Phase 11: Roster-Wide Eligibility API (1/1 plan) — completed 2026-03-12
- [x] Phase 12: Contract Management Dashboard (2/2 plans) — completed 2026-03-12
- [x] Phase 13: Calendar/Timeline & Deadline Countdowns (2/2 plans) — completed 2026-03-13

</details>

### 🚧 v1.2 Data Integrity & Eligibility Accuracy (In Progress)

**Milestone Goal:** Import missing historical data from MFL and systematically audit/fix all eligibility logic against bylaws to eliminate false positives and false negatives.

#### Phase 14: Historical Data Imports

**Goal**: Pull past-season player scores and contract history from MFL API to enable accurate eligibility checks
**Depends on**: Previous milestone complete
**Research**: Likely (MFL API historical data endpoints)
**Research topics**: MFL API endpoints for historical scores and prior-season contract data
**Plans**: TBD

Plans:
- [ ] 14-01: TBD (run /gsd:plan-phase 14 to break down)

#### Phase 15: Eligibility Audit & Fixes

**Goal**: Systematically audit and fix all 7 contract action eligibility checks against bylaws and real MFL data
**Depends on**: Phase 14
**Research**: Unlikely (internal logic audit)
**Plans**: TBD

Plans:
- [ ] 15-01: TBD

#### Phase 16: NFL Kickoff Rule

**Goal**: Integrate external NFL schedule data for kickoff-based eligibility gating (UAT-001)
**Depends on**: Phase 15
**Research**: Likely (external NFL schedule API/data source)
**Research topics**: NFL schedule data sources, API options for kickoff dates
**Plans**: TBD

Plans:
- [ ] 16-01: TBD

#### Phase 17: Regression Testing & Validation

**Goal**: End-to-end eligibility verification across all 7 contract actions with real roster data
**Depends on**: Phase 16
**Research**: Unlikely (internal testing patterns)
**Plans**: TBD

Plans:
- [ ] 17-01: TBD

## Progress

**Execution Order:**
Phases execute in numeric order. Next phase continues from 14.

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
| 12. Contract Management Dashboard | v1.1 | 2/2 | Complete | 2026-03-12 |
| 13. Calendar/Timeline & Deadline Countdowns | v1.1 | 2/2 | Complete | 2026-03-13 |
| 14. Historical Data Imports | v1.2 | 0/? | Not started | - |
| 15. Eligibility Audit & Fixes | v1.2 | 0/? | Not started | - |
| 16. NFL Kickoff Rule | v1.2 | 0/? | Not started | - |
| 17. Regression Testing & Validation | v1.2 | 0/? | Not started | - |
