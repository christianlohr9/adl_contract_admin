# Roadmap: ADL Contract Admin

## Overview

Transform the ADL's Google Sheet-based contract administration into a proper web application. Starting with structured rule extraction from the bylaws, building a FastAPI backend with PostgreSQL, integrating MFL league data, implementing the contract calculation engine (EPV extensions, tags, tenders, buyouts), exposing REST endpoints, and delivering a functional React UI for all 32 GMs.

## Domain Expertise

None

## Milestones

- ✅ **v1.0 MVP** - Phases 1-8 (shipped 2026-03-12)
- ✅ [v1.1 League Calendar & Contract Management](milestones/v1.1-ROADMAP.md) (Phases 9-13) — SHIPPED 2026-03-13
- ✅ [v1.2 Data Integrity & Eligibility Accuracy](milestones/v1.2-ROADMAP.md) (Phases 14-17) — SHIPPED 2026-03-13
- ✅ [v1.3 Data Integrity 2](milestones/v1.3-ROADMAP.md) (Phases 18-23) — SHIPPED 2026-04-04
- 🚧 **v1.4 Polish & Deploy** - Phases 24-27 (in progress)

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

<details>
<summary>✅ v1.2 Data Integrity & Eligibility Accuracy (Phases 14-17) — SHIPPED 2026-03-13</summary>

- [x] Phase 14: Historical Data Imports (4/4 plans) — completed 2026-03-13
- [x] Phase 15: Eligibility Audit & Fixes (2/2 plans) — completed 2026-03-13
- [x] Phase 16: NFL Kickoff Rule (1/1 plan) — completed 2026-03-13
- [x] Phase 17: Regression Testing & Validation (1/1 plan) — completed 2026-03-13

</details>

<details>
<summary>✅ v1.3 Data Integrity 2 (Phases 18-23) — SHIPPED 2026-04-04</summary>

- [x] Phase 18: Franchise Tags (2/2 plans) — completed 2026-04-01
- [x] Phase 19: Extensions (2/2 plans) — completed 2026-04-02
- [x] Phase 20: Tenders (2/2 plans) — completed 2026-04-03
- [x] Phase 21: 5YO & PPE (2/2 plans) — completed 2026-04-04
- [x] Phase 22: Buyout/Restructure (1/1 plan) — completed 2026-04-04
- [x] Phase 23: Cross-Tool Validation (1/1 plan) — completed 2026-04-04

</details>

### 🚧 v1.4 Polish & Deploy (In Progress)

**Milestone Goal:** Clean up the repo, redesign UX to sell commissioners on replacing the spreadsheet, and deploy to free-tier hosting

#### Phase 24: Repo Polish

**Goal**: Remove AI slop (unnecessary comments, dead code, unused files/deps), add favicon, clean repo for public readiness
**Depends on**: Previous milestone complete
**Research**: Unlikely (internal cleanup)
**Plans**: TBD

Plans:
- [ ] 24-01: TBD (run /gsd:plan-phase 24 to break down)

#### Phase 25: UX Audit & Redesign

**Goal**: Full UX review as a designer — layout, flows, visual hierarchy, mobile responsiveness, polish. Make commissioners say "yes, this replaces the spreadsheet"
**Depends on**: Phase 24
**Research**: Unlikely (internal UI patterns)
**Plans**: TBD

Plans:
- [ ] 25-01: TBD

#### Phase 26: Production Configuration

**Goal**: Environment management, CORS for production domain, production-ready settings, error handling
**Depends on**: Phase 25
**Research**: Unlikely (established patterns)
**Plans**: TBD

Plans:
- [ ] 26-01: TBD

#### Phase 27: No-Cost Deployment

**Goal**: Deploy to a free-tier platform (Fly.io / Render / Railway), CI/CD pipeline, DNS
**Depends on**: Phase 26
**Research**: Likely (platform evaluation for FastAPI + PostgreSQL + React SPA)
**Research topics**: Evaluate free-tier hosting options that support FastAPI + PostgreSQL + static React
**Plans**: TBD

Plans:
- [ ] 27-01: TBD

## Progress

**Execution Order:**
Phases execute in numeric order. Next phase continues from 24.

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
| 14. Historical Data Imports | v1.2 | 4/4 | Complete | 2026-03-13 |
| 15. Eligibility Audit & Fixes | v1.2 | 2/2 | Complete | 2026-03-13 |
| 16. NFL Kickoff Rule | v1.2 | 1/1 | Complete | 2026-03-13 |
| 17. Regression Testing & Validation | v1.2 | 1/1 | Complete | 2026-03-13 |
| 18. Franchise Tags | v1.3 | 2/2 | Complete | 2026-04-01 |
| 19. Extensions | v1.3 | 2/2 | Complete | 2026-04-02 |
| 20. Tenders | v1.3 | 2/2 | Complete | 2026-04-03 |
| 21. 5YO & PPE | v1.3 | 2/2 | Complete | 2026-04-04 |
| 22. Buyout/Restructure | v1.3 | 1/1 | Complete | 2026-04-04 |
| 23. Cross-Tool Validation | v1.3 | 1/1 | Complete | 2026-04-04 |
| 24. Repo Polish | v1.4 | 0/? | Not started | - |
| 25. UX Audit & Redesign | v1.4 | 0/? | Not started | - |
| 26. Production Configuration | v1.4 | 0/? | Not started | - |
| 27. No-Cost Deployment | v1.4 | 0/? | Not started | - |
