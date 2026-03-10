# External Integrations

**Analysis Date:** 2026-03-10

## APIs & External Services

**Fantasy Football Data:**
- MyFantasyLeague (MFL) API - Primary fantasy football data source
  - League ID: `60206` (configured in `app/config/config.py`)
  - Integration method: R's `ffscrapr` package via rpy2 wrapper
  - Client: `app/services/ffscrapr.py` - `ff_connect()`, `ff_franchises()`, `ff_rosters()`
  - Data: Franchises, rosters, player scores for seasons 2020-2024

- NFL Data - Player statistics
  - Integration method: R's `nflreadr` package via rpy2
  - Installed dynamically in `app/services/ffscrapr.py`

**Google Services (available but potentially unused):**
- Google Auth 2.38.0, OAuth2 integration
- gspread 6.1.4 - Google Sheets API client
- Purpose: Likely planned for contract sheet export

**AWS (available but potentially unused):**
- boto3 1.34.34 - AWS SDK
- Purpose: Potential S3 storage for data backup

## Data Storage

**Databases:**
- PostgreSQL on Supabase - Primary data store
  - Host: `aws-0-eu-central-1.pooler.supabase.com`
  - Port: 6543 (Supabase connection pooler)
  - Client: psycopg2-binary (direct SQL queries)
  - Connection: `app/services/database_service.py` - `create_connection()`
  - Tables: `contracts`, `franchises`, `roster`, `playerscores`
  - Schema: `app/data/dump.sql`

**File Storage:**
- Not currently used (boto3 available for future S3 integration)

**Caching:**
- None currently

## Authentication & Identity

**Auth Provider:**
- None for the application itself (no user auth implemented)
- Database auth via Supabase credentials in `app/config/config.py`

## Monitoring & Observability

**Error Tracking:**
- Python `logging` module - Basic logging in `app/services/update_data.py`
- No external error tracking service (no Sentry, etc.)

**Analytics:**
- None

**Logs:**
- stdout/stderr only
- Fly.io built-in log aggregation

## CI/CD & Deployment

**Hosting:**
- Fly.io - Container deployment
  - Config: `fly.toml`
  - Region: `arn` (Amsterdam)
  - Auto-scaling: min 0 machines, auto-stop enabled
  - Port: 8080 (force HTTPS)

**CI Pipeline:**
- None configured (no GitHub Actions, no CI/CD workflows)

## Environment Configuration

**Development:**
- Required env vars: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- Secrets: Currently hardcoded as defaults in `app/config/config.py` (security issue)
- No `.env.example` file exists

**Production:**
- Fly.io environment variables (should be configured in Fly dashboard)
- Docker container with `Dockerfile`

## Webhooks & Callbacks

**Incoming:**
- None

**Outgoing:**
- None

---

*Integration audit: 2026-03-10*
*Update when adding/removing external services*
