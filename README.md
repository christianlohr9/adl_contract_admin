# ADL Contract Admin

A full-stack web application that automates contract management for the **Analytics Dynasty League (ADL)**, a 32-team fantasy football dynasty league. It replaces a sprawling Google Sheet with a FastAPI/PostgreSQL backend and a React/TypeScript frontend so commissioners and GMs can run extensions, tags, tenders, buyouts and cap math without spreadsheet formulas.

All rules are derived from the ADL Bylaws — the bylaws are the single source of truth, with rule data stored as Markdown, JSON and YAML next to the code.

---

## Features

**Contract tools** (validated cell-by-cell against the legacy spreadsheet, 99.7% match rate):
- **Extensions** — EPV-based with prior-season salary × 1.1 growth methodology
- **Franchise / Transition Tags** — positional averages adjusted by ADL Cap Percentage
- **ERFA / RFA Tenders** — accrued-season-based eligibility with conference scoping
- **5-Year Option** — modified Tag Tag formula with cap percentage adjustment
- **Performance Pay Escalator (PPE)** — raw NFL tag pricing with PR Starter Floor exclusion
- **Buyouts / Releases** — NG / SD / FG cap penalty calculations

**League management:**
- Admin-configurable **League Calendar** with 27 date fields gating eligibility windows
- **Contract Management Dashboard** with dynamic eligibility table per team
- Deadline countdown cards with urgency awareness
- Roster-wide eligibility API aggregating all 7 contract actions
- Salary cap rollups, dead cap, penalty tracking

**Data pipeline:**
- Direct MFL API integration via `httpx` (no R/ffscrapr dependency)
- Multi-season historical import (2020+) with gap detection
- Weekly roster scans (2016+) for accurate conference-scoped accrued seasons (6-week NFL threshold)
- Player score history (weeks 1–17 + YTD) used as the activity golden source

---

## Tech Stack

| Layer | Stack |
|---|---|
| Backend | Python 3.13 · FastAPI · SQLAlchemy 2 (async) · Alembic · Pydantic v2 |
| Database | PostgreSQL 16 |
| Frontend | React 19 · TypeScript · Vite · TanStack Query/Table · shadcn/ui · Tailwind v4 |
| Tooling | `uv` (Python) · `pnpm` (Node) · Docker Compose (local DB) |
| Deployment | Render (web service + static site) · Neon (serverless Postgres) · GitHub Actions (cron sync) |

---

## Local Development

### Prerequisites

- [uv](https://docs.astral.sh/uv/) for Python dependency management
- [pnpm](https://pnpm.io/) for the frontend
- Docker (for local Postgres) or a local Postgres 16 instance
- An MFL account with API access for league `60206`

### 1. Clone and configure

```bash
git clone https://github.com/christianlohr9/adl_contract_admin.git
cd adl_contract_admin
cp .env.example .env
# fill in MFL_API_KEY, MFL_USERNAME, MFL_PASSWORD
```

### 2. Start the database

```bash
docker compose up -d db
```

This brings up Postgres on `localhost:5432` (user `adl`, password `adl_dev`, db `adl`). A second test database runs on port 5433.

### 3. Backend

```bash
uv sync
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --port 8000
```

API: http://localhost:8000 · Swagger: http://localhost:8000/docs

### 4. Frontend

```bash
cd frontend
pnpm install
pnpm dev
```

App: http://localhost:5173

### 5. Initial data load

The first time you start the backend it will backfill historical seasons (2020–2025) and the current season's roster, contracts, scores and weekly roster scans from MFL. This takes a few minutes — watch the logs.

### Tests

```bash
uv run pytest                     # backend
cd frontend && pnpm lint          # frontend lint
```

---

## Project Structure

```
adl_contract_admin/
├── src/app/                  # FastAPI application
│   ├── api/                  # HTTP endpoints
│   ├── services/             # Business logic (contract tools, EPV, eligibility)
│   ├── models/               # SQLAlchemy ORM models
│   ├── core/                 # Config, db session, MFL client
│   └── tasks/                # Sync jobs and backfills
├── migrations/               # Alembic migrations
├── rules/                    # Bylaws-derived rule data (md/json/yaml)
├── frontend/                 # React + Vite + TypeScript app
├── tests/                    # pytest suite
├── docker-compose.yml        # Local Postgres + optional web container
├── Dockerfile                # Production backend image (used by Render)
├── render.yaml               # Render Blueprint (backend + static frontend)
├── start.sh                  # Production entrypoint: alembic upgrade + uvicorn
└── .github/workflows/        # CI + scheduled MFL sync
```

---

## Deployment

The app is designed to run on a **fully free tier**: Render free web service (backend), Render free static site (frontend), and Neon free Postgres. Scheduled MFL syncs run via GitHub Actions instead of an in-app scheduler so the backend can spin down when idle.

### Architecture

```
┌────────────────────┐         ┌────────────────────┐
│  Render Static     │ ──────▶ │  Render Web        │
│  (React frontend)  │  HTTPS  │  (FastAPI backend) │
└────────────────────┘         └─────────┬──────────┘
                                         │
                                         ▼
                                ┌────────────────────┐
                                │  Neon Postgres     │
                                │  (serverless)      │
                                └────────────────────┘
                                         ▲
                                         │
                              ┌──────────┴──────────┐
                              │  GitHub Actions     │
                              │  (cron MFL sync)    │
                              └─────────────────────┘
```

### 1. Create the Neon database

1. Sign up at [neon.tech](https://neon.tech) (GitHub login works)
2. Create a project named `adl-contract-admin` in a US region
3. Copy the connection string from the dashboard
4. Replace the scheme: `postgresql://` → `postgresql+asyncpg://`
5. Replace the SSL param: `?sslmode=require` → `?ssl=require` (asyncpg syntax)

Final form:
```
postgresql+asyncpg://user:pass@ep-xxx.us-east-2.aws.neon.tech/dbname?ssl=require
```

### 2. Create the Render services

1. Sign up at [render.com](https://render.com) and connect this repo
2. Create a **Blueprint Instance** pointing at the repo — Render reads `render.yaml` and creates both services automatically:
   - `adl-api` (Docker web service, free plan)
   - `adl-frontend` (static site, free plan)

### 3. Configure environment variables

Both services have placeholders in `render.yaml` (`sync: false`) that you fill in via the dashboard: open each service → **Environment** in the left sidebar → fill values → **Save Changes**.

**`adl-api` (backend):**

| Variable | Value |
|---|---|
| `DATABASE_URL` | Neon connection string from step 1 |
| `CORS_ORIGINS` | `https://adl-frontend-xxx.onrender.com` (frontend URL — comma-separated for multiple) |
| `MFL_LEAGUE_ID` | `60206` (already set) |
| `MFL_YEAR` | `2026` (already set) |
| `MFL_API_KEY` | Your MFL API key |
| `MFL_USERNAME` | Your MFL username |
| `MFL_PASSWORD` | Your MFL password |
| `SYNC_ENABLED` | `false` (production uses GitHub Actions, not the in-app scheduler) |

**`adl-frontend` (static site):**

| Variable | Value |
|---|---|
| `VITE_API_URL` | `https://adl-api-xxx.onrender.com` (backend URL) |

> ⚠️ `VITE_API_URL` is **build-time** — after changing it you must trigger a manual redeploy of the frontend so Vite rebakes the bundle.

#### Two-pass deploy (chicken-and-egg)

The frontend needs the backend URL and vice versa, but neither URL exists until Render assigns it. Workaround:

1. Deploy the blueprint with placeholder values (e.g. `https://placeholder.onrender.com`) for `CORS_ORIGINS` and `VITE_API_URL`
2. Wait for both services to come up — copy the real URLs from the Render dashboard
3. Update both env vars and **manually redeploy both services**

### 4. Configure the GitHub Actions sync

Production data sync runs via `.github/workflows/mfl-sync.yml` on a cron schedule, triggering a backend endpoint with an authenticated request.

1. GitHub repo → **Settings → Secrets and variables → Actions**
2. Add secret: `API_URL` = `https://adl-api-xxx.onrender.com`
3. Trigger the first run manually: **Actions → MFL Data Sync → Run workflow**

The first sync takes 2–5 minutes (full backfill + current season). Subsequent runs are incremental.

### 5. Verify

- **Backend health:** `https://adl-api-xxx.onrender.com/docs` shows the FastAPI Swagger UI
- **Frontend:** `https://adl-frontend-xxx.onrender.com` shows the splash screen with team picker
- Pick a team → dashboard shows roster, cap, contract tools
- Click any player → Extensions tab shows EPV calculations

> 💤 **Cold starts:** Render free web services spin down after 15 minutes of inactivity. The first request after idle takes 5–10 seconds (Render spin-up + Neon wake). Subsequent requests are fast.

### Deployment troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `error parsing value for field "cors_origins"` | pydantic-settings tried to JSON-decode the env var | Already handled — accepts comma-separated values via a `NoDecode` annotation |
| `connect() got an unexpected keyword argument 'sslmode'` | asyncpg uses `ssl=`, not `sslmode=` | Change `?sslmode=require` → `?ssl=require` in `DATABASE_URL` |
| `KeyError` on a migration revision | A migration file is missing from the deployed branch | Make sure all `migrations/versions/*.py` files are committed and pushed |
| Frontend shows CORS errors | `CORS_ORIGINS` doesn't match the actual frontend URL | Update `CORS_ORIGINS` on `adl-api`, save, redeploy |
| Frontend hits the wrong API | `VITE_API_URL` was set after the build | Manually redeploy the frontend so Vite rebuilds with the correct value |

---

## Configuration Reference

See `.env.example` for the full list. The most important variables:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Async SQLAlchemy URL (`postgresql+asyncpg://...`) |
| `CORS_ORIGINS` | Comma-separated allowed origins (or JSON array) |
| `MFL_LEAGUE_ID` / `MFL_YEAR` | Target league and season |
| `MFL_API_KEY` / `MFL_USERNAME` / `MFL_PASSWORD` | MFL credentials |
| `SYNC_ENABLED` | `true` for in-app APScheduler (local dev), `false` for GitHub Actions cron (production) |
| `SYNC_HISTORICAL_YEARS` | Comma-separated years to backfill on first start |

---

## League Context

- **League:** Analytics Dynasty League — 32 teams, 2 conferences, MFL ID `60206`
- **Bylaws:** `Analytics Dynasty League Bylaws 2025.md` — all rules and constants are derived from this document
- **Users:** All 32 GMs have open access (no auth — trust-based)
- **History:** Player scores 2020–2025, contracts 2020–2025, weekly roster scans 2016–2025

---

## Status

Shipped through **v1.4** (Polish & Deploy). Currently deployed on Render + Neon with GitHub Actions sync. See `.planning/PROJECT.md` and `.planning/STATE.md` for milestone history and accumulated context.
