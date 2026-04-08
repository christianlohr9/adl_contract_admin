# Phase 27: No-Cost Deployment - Research

**Researched:** 2026-04-08
**Domain:** Free-tier hosting for FastAPI + PostgreSQL + React SPA
**Confidence:** HIGH

<research_summary>
## Summary

Researched the free-tier hosting landscape for deploying a full-stack FastAPI/PostgreSQL/React application at zero cost. Evaluated Fly.io, Render, Railway, and Koyeb as compute platforms, plus Neon, Supabase, and Render Postgres as database options.

**Key finding:** Fly.io eliminated its free tier for new customers in 2024. Railway has no free plan ($5/mo minimum). Render's free PostgreSQL expires after 30 days. The winning combination is **Render (free web service + free static site) + Neon (free PostgreSQL)** — this is the only truly zero-cost, no-expiration stack that supports the full FastAPI + React SPA + PostgreSQL architecture. GitHub Actions provides free cron scheduling for the periodic MFL sync.

**Primary recommendation:** Deploy backend on Render free web service, frontend on Render free static site, database on Neon free PostgreSQL. Use GitHub Actions for periodic MFL sync cron job. Push-to-deploy via Render's GitHub integration.
</research_summary>

<standard_stack>
## Standard Stack

### Core
| Service | Provider | Tier | Purpose | Why Standard |
|---------|----------|------|---------|--------------|
| Backend (FastAPI) | Render | Free web service | API server | Free, auto-deploy from GitHub, Docker support |
| Frontend (React SPA) | Render | Free static site | Vite build served as static files | Free, CDN, auto-deploy, unlimited bandwidth not metered |
| Database (PostgreSQL) | Neon | Free plan | Serverless Postgres | Free forever, 0.5 GB storage, no expiration, SSL |
| Periodic Sync | GitHub Actions | Free (public repo) | Cron-triggered MFL sync | Unlimited minutes on public repos, cron syntax |

### Supporting
| Service | Provider | Purpose | When to Use |
|---------|----------|---------|-------------|
| DNS/Domain | Render-provided | `*.onrender.com` URLs | Default — no custom domain needed per requirements |
| Migrations | Alembic (in Docker CMD) | Schema management | Run `alembic upgrade head` on each deploy |
| Secrets | Render env vars | DATABASE_URL, MFL_API_KEY | Platform-native secret management |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Render (compute) | Koyeb | Koyeb has free tier with Postgres included, but less mature, smaller community, and only 50 active compute hours/month |
| Neon (database) | Supabase | Supabase pauses after 1 week inactivity, only 500 MB; Neon suspends compute but keeps data accessible |
| Neon (database) | Render Postgres | Render free Postgres expires after 30 days — unacceptable |
| GitHub Actions (cron) | Render cron jobs | Render cron jobs cost ~$1/mo minimum — not free |
| GitHub Actions (cron) | cron-job.org | External service hitting a public endpoint; GH Actions more integrated |

### Why NOT the other platforms

**Fly.io:** Removed free allowances for new customers in 2024. New signups get a free trial (2 VM hours or 7 days, whichever first), then require paid plan. Managed Postgres starts at $38/mo. **Eliminated.**

**Railway:** No free plan. $5/mo Hobby plan with $5 usage credit. Even idle apps incur the $5 subscription fee. **Eliminated (not zero-cost).**

**Render Postgres (free):** Expires after 30 days (reduced from 90 days in May 2024). Must recreate and restore every 30 days. **Eliminated (operational burden, data loss risk).**
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Deployment Architecture
```
GitHub (main branch)
  ├── push triggers → Render Web Service (FastAPI backend)
  │                    ├── Connects to Neon PostgreSQL via DATABASE_URL
  │                    ├── Runs alembic upgrade head on startup
  │                    └── Serves API at api-xxx.onrender.com
  │
  ├── push triggers → Render Static Site (React frontend)
  │                    ├── Builds with: cd frontend && pnpm install && pnpm build
  │                    ├── Publishes: frontend/dist/
  │                    └── Serves SPA at app-xxx.onrender.com
  │
  └── cron schedule → GitHub Actions workflow
                       ├── Calls POST /api/sync on the Render backend URL
                       └── Runs daily/weekly per cron expression
```

### Pattern 1: Render Web Service from Existing Dockerfile
**What:** Render auto-detects the Dockerfile, builds and deploys the container
**When to use:** Our project — we already have a multi-stage Dockerfile
**How it works:**
- Render detects `Dockerfile` in repo root
- Builds the image using our existing multi-stage build
- Runs the CMD: `uvicorn app.main:app --host 0.0.0.0 --port 8000`
- Environment variables set in Render dashboard (DATABASE_URL, etc.)

### Pattern 2: Render Static Site for React SPA
**What:** Render builds the frontend and serves the static output from CDN
**When to use:** Any Vite/React SPA
**Configuration:**
- Root directory: `frontend`
- Build command: `pnpm install && pnpm build`
- Publish directory: `dist`
- Environment variable: `VITE_API_URL=https://api-xxx.onrender.com`

### Pattern 3: Neon Serverless PostgreSQL with Connection Pooling
**What:** Neon provides a standard PostgreSQL connection string with SSL
**Connection string format:**
```
postgresql://user:pass@ep-xxx.us-east-2.aws.neon.tech/dbname?sslmode=require
```
**Key details:**
- Compute auto-suspends after 5 minutes of inactivity (free plan)
- Wakes on first connection (~1-2 seconds cold start)
- 0.5 GB storage per project, 100 CU-hours/month
- No expiration — free forever

### Pattern 4: GitHub Actions Cron for Periodic MFL Sync
**What:** GitHub Actions workflow triggered on a cron schedule calls the sync API endpoint
**When to use:** Periodic data refresh without paying for Render cron jobs
**Example:**
```yaml
# .github/workflows/mfl-sync.yml
name: MFL Sync
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:       # Manual trigger option

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger MFL sync
        run: |
          curl -X POST "${{ secrets.API_URL }}/api/sync" \
            -H "Content-Type: application/json" \
            --fail --silent --show-error
```

### Pattern 5: Startup Data Seeding via Alembic + Sync Endpoint
**What:** First deploy runs migrations, then manual/automated sync populates data
**How:**
1. Dockerfile CMD already runs `alembic upgrade head` (from docker-compose pattern)
2. Modify Dockerfile CMD or add a start script: `alembic upgrade head && uvicorn ...`
3. After first deploy, trigger `/api/sync` manually or via GitHub Actions
4. Subsequent deploys: migrations run automatically, data persists in Neon

### Anti-Patterns to Avoid
- **Using Render's free PostgreSQL:** Expires after 30 days. Will lose all data.
- **Running frontend as a web service:** Wastes the 750 free instance hours. Use static site instead (doesn't count against hours).
- **Keeping the backend awake with ping services:** Unnecessary for this use case — cold starts are acceptable per user requirements.
- **Storing data in the filesystem:** Render's free tier has ephemeral storage — files lost on redeploy.
- **Bundling frontend and backend in one service:** Wastes compute serving static files. Split them.
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| SSL/TLS certificates | Manual cert management | Render managed TLS | Automatic, free, handles renewal |
| Container orchestration | Docker Compose in production | Render service definitions | Platform handles health checks, restarts |
| Static file serving | FastAPI StaticFiles mount | Render Static Site | Free CDN, no compute cost, better caching |
| Cron job infrastructure | In-app scheduler (APScheduler) | GitHub Actions cron | Free, no compute while idle, survives restarts |
| Database connection pooling | Manual pool tuning | Neon's built-in pooler | Handles serverless wake/sleep transparently |
| Health checks | Custom endpoint | Render's built-in health checks | Platform-native, triggers restarts |
| Log aggregation | Custom logging | Render log streams | Built-in, free, searchable |

**Key insight:** The entire deployment should use platform-native features. The only custom code needed is: (1) a startup script combining alembic + uvicorn, (2) the GitHub Actions workflow YAML for cron sync. Everything else is configuration.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Render Free PostgreSQL Expiration
**What goes wrong:** Database and all data deleted after 30 days
**Why it happens:** Render reduced free Postgres from 90 to 30 days in May 2024
**How to avoid:** Use Neon free tier instead — no expiration, 0.5 GB storage
**Warning signs:** Email from Render about upcoming database deletion

### Pitfall 2: Frontend Eating Instance Hours
**What goes wrong:** Running React dev server as a Render web service consumes the 750 free hours
**Why it happens:** Not knowing Render offers free static site hosting separately
**How to avoid:** Deploy frontend as a Static Site (build command + publish directory), not a web service
**Warning signs:** Running out of instance hours mid-month

### Pitfall 3: CORS Misconfiguration Between Services
**What goes wrong:** Frontend can't call backend API — browser blocks cross-origin requests
**Why it happens:** Frontend (app-xxx.onrender.com) and backend (api-xxx.onrender.com) are different origins
**How to avoid:** Configure CORS in FastAPI with the exact frontend origin URL. Phase 26 already added CORS configuration — just set the environment variable correctly.
**Warning signs:** "CORS policy" errors in browser console

### Pitfall 4: Neon Compute Suspend on First Request
**What goes wrong:** First API call after idle period takes 3-5 seconds (database waking up)
**Why it happens:** Neon free tier suspends compute after 5 minutes of inactivity
**How to avoid:** Accept it — user requirements say cold starts are fine. Optionally, the GitHub Actions cron can keep the database warm.
**Warning signs:** Slow first response after periods of no traffic

### Pitfall 5: Missing `sslmode=require` in DATABASE_URL
**What goes wrong:** Connection to Neon fails
**Why it happens:** Neon requires SSL; local dev typically doesn't
**How to avoid:** Neon's connection string includes `?sslmode=require` by default — just use the full string from Neon dashboard
**Warning signs:** "SSL required" connection errors in logs

### Pitfall 6: Alembic Migrations Running Before Database is Ready
**What goes wrong:** Migration fails on cold start because Neon is still waking
**Why it happens:** Startup script runs `alembic upgrade head` immediately, Neon needs 1-2s
**How to avoid:** Add a brief retry/wait in the start script, or ensure the database connection has a reasonable connect timeout
**Warning signs:** Intermittent migration failures on deploy

### Pitfall 7: GitHub Actions Cron Disabled After 60 Days
**What goes wrong:** Scheduled MFL sync stops running silently
**Why it happens:** GitHub disables scheduled workflows on repos with no activity for 60 days
**How to avoid:** Push any commit (even a README tweak) periodically, or set up a workflow_dispatch to manually re-enable
**Warning signs:** MFL data becomes stale, no workflow runs in Actions tab

### Pitfall 8: Environment Variable Mismatch
**What goes wrong:** Frontend calls wrong API URL or backend can't find database
**Why it happens:** VITE_API_URL and DATABASE_URL must be set correctly in Render dashboard for each service
**How to avoid:** Document all required env vars. VITE_API_URL must be set at *build time* (Render static site build), not runtime.
**Warning signs:** Network errors in browser, "connection refused" in backend logs
</common_pitfalls>

<code_examples>
## Code Examples

### Render Web Service Configuration
```
# render.yaml (Blueprint — optional, can also configure via dashboard)
services:
  - type: web
    name: adl-api
    runtime: docker
    repo: https://github.com/USER/adl_contract_admin
    branch: main
    plan: free
    envVars:
      - key: DATABASE_URL
        sync: false  # set manually in dashboard
      - key: MFL_LEAGUE_ID
        value: "60206"
      - key: CORS_ORIGINS
        sync: false

  - type: web
    name: adl-frontend
    runtime: static
    repo: https://github.com/USER/adl_contract_admin
    branch: main
    rootDir: frontend
    buildCommand: pnpm install && pnpm build
    staticPublishPath: dist
    envVars:
      - key: VITE_API_URL
        sync: false
```

### Modified Dockerfile CMD with Startup Script
```dockerfile
# Add to Dockerfile — replaces bare uvicorn CMD
COPY rules/ rules/
COPY start.sh .
RUN chmod +x start.sh
CMD ["./start.sh"]
```

```bash
#!/bin/sh
# start.sh — run migrations then start server
set -e
echo "Running database migrations..."
alembic upgrade head
echo "Starting server..."
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
```
Note: Render sets the `PORT` environment variable — use it.

### GitHub Actions MFL Sync Cron
```yaml
# .github/workflows/mfl-sync.yml
name: MFL Data Sync

on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger MFL sync
        env:
          API_URL: ${{ secrets.API_URL }}
        run: |
          response=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "${API_URL}/api/sync" \
            -H "Content-Type: application/json")
          if [ "$response" -ne 200 ]; then
            echo "Sync failed with status: $response"
            exit 1
          fi
          echo "Sync triggered successfully"
```

### Frontend Build for Static Deployment
```bash
# Build command for Render Static Site
# Root directory: frontend
# Build command:
pnpm install && pnpm build
# Publish directory: dist
```

The Vite build produces a `dist/` folder with static HTML/JS/CSS that Render serves directly.

### Neon Connection String in FastAPI
```python
# No code changes needed — just set DATABASE_URL env var in Render dashboard
# Neon provides: postgresql://user:pass@ep-xxx.us-east-2.aws.neon.tech/dbname?sslmode=require
# SQLAlchemy async: replace postgresql:// with postgresql+asyncpg://
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Heroku free tier | Gone since 2022 | Nov 2022 | Must use alternatives |
| Fly.io free tier | Gone for new users | 2024 | No longer viable for zero-cost |
| Render free Postgres 90 days | Reduced to 30 days | May 2024 | Must use external DB provider |
| Railway free tier | Eliminated | 2023 | $5/mo minimum now |
| In-app schedulers (APScheduler) | GitHub Actions cron | 2023+ | Free, no compute needed, more reliable |

**New tools/patterns to consider:**
- **Neon serverless Postgres:** Mature free tier (doubled compute in 2025), no expiration, auto-sleep, perfect for low-traffic apps
- **Render Blueprints (render.yaml):** Infrastructure-as-code for Render — optional but useful for reproducibility
- **GitHub Actions `workflow_dispatch`:** Manual trigger button for sync — useful for on-demand data refresh

**Deprecated/outdated:**
- **Heroku free tier:** Gone since November 2022
- **Fly.io free allowances:** Gone for new customers since 2024
- **Railway free plan:** Eliminated, replaced with $5/mo Hobby
- **Render free Postgres as a long-term solution:** 30-day expiration makes it unsuitable
</sota_updates>

<open_questions>
## Open Questions

1. **Sync endpoint authentication**
   - What we know: The app has no auth (trust-based). The sync endpoint is publicly accessible.
   - What's unclear: Should the GitHub Actions cron sync endpoint require a secret/API key to prevent unauthorized triggers?
   - Recommendation: Add a simple shared secret header check on the sync endpoint. Set the secret in both GitHub Actions secrets and Render env vars.

2. **Neon region selection**
   - What we know: Neon offers regions in US and EU. Render free tier is in Oregon (us-west).
   - What's unclear: Whether Neon has a us-west region or if cross-region latency matters for this use case.
   - Recommendation: Choose closest Neon region to Render's Oregon datacenter. For ~33 users with cold-start tolerance, cross-region latency is likely negligible.

3. **Data seeding strategy for first deploy**
   - What we know: Sync orchestrator exists and pulls from MFL API. Alembic handles schema.
   - What's unclear: Does the full initial sync need to run interactively (too long for a deploy step), or can it be triggered post-deploy?
   - Recommendation: Run migrations on startup, trigger full sync via GitHub Actions or manual API call after first deploy. Don't block the deploy on data population.

4. **Rules/constants file access**
   - What we know: The app reads from `rules/` directory (JSON/YAML/MD files). Docker COPY handles this.
   - What's unclear: Whether the current Dockerfile copies `rules/` — it does NOT currently.
   - Recommendation: Add `COPY rules/ rules/` to the Dockerfile before the CMD step.
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- [Render Free Tier Docs](https://render.com/docs/free) — 750 instance hours, 15-min spin-down, static sites free, Postgres expires 30 days
- [Neon Plans Docs](https://neon.com/docs/introduction/plans) — Free plan: 0.5 GB storage, 100 CU-hours/month, no expiration, 5-min suspend
- [Neon + Render Integration Guide](https://neon.com/docs/guides/render) — Connection string with sslmode=require, env var setup
- [Render Deploy FastAPI Docs](https://render.com/docs/deploy-fastapi) — Docker and native runtime support
- [GitHub Actions Scheduled Workflows](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows#schedule) — Cron syntax, 60-day inactivity disable

### Secondary (MEDIUM confidence)
- [Render Changelog: Free Postgres 30-day expiration](https://render.com/changelog/free-postgresql-instances-now-expire-after-30-days-previously-90) — Confirmed May 2024 change
- [Fly.io Pricing Page](https://fly.io/pricing/) — Confirmed no free tier for new customers
- [Railway Pricing Page](https://railway.com/pricing) — Confirmed $5/mo minimum
- [Koyeb PostgreSQL Free Tiers Comparison](https://www.koyeb.com/blog/top-postgresql-database-free-tiers-in-2026) — Cross-verified Neon, Supabase, Aiven limits
- [FreeCodeCamp: Deploy FastAPI + PostgreSQL on Render](https://www.freecodecamp.org/news/deploy-fastapi-postgresql-app-on-render/) — Verified deployment patterns

### Tertiary (LOW confidence - needs validation)
- Koyeb as compute alternative (50 active hours/month limit needs verification during implementation)
- Supabase as database alternative (1-week inactivity pause may be more aggressive than documented)
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: Free-tier PaaS hosting for Python/FastAPI + React SPA + PostgreSQL
- Ecosystem: Render, Neon, GitHub Actions, Fly.io (eliminated), Railway (eliminated)
- Patterns: Split frontend/backend deployment, external managed database, cron via CI/CD
- Pitfalls: Free tier expirations, CORS, cold starts, env var configuration

**Confidence breakdown:**
- Standard stack: HIGH — verified with official docs from all platforms
- Architecture: HIGH — Render + Neon is a well-documented combination with official integration guide
- Pitfalls: HIGH — sourced from official changelogs and community reports
- Code examples: MEDIUM — adapted from docs, need validation during implementation

**Research date:** 2026-04-08
**Valid until:** 2026-05-08 (30 days — hosting landscape changes slowly)
</metadata>

---

*Phase: 27-no-cost-deployment*
*Research completed: 2026-04-08*
*Ready for planning: yes*
