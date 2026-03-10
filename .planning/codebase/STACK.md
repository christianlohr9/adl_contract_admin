# Technology Stack

**Analysis Date:** 2026-03-10

## Languages

**Primary:**
- Python 3.11+ (Docker: 3.12-slim) - All application code

**Secondary:**
- R - Statistical computations via rpy2 (ffscrapr, nflreadr packages)

## Runtime

**Environment:**
- Python 3.12 (Docker base image: `python:3.12-slim`)
- R runtime (`r-base`, `r-base-dev`) installed in Docker for rpy2

**Package Manager:**
- Poetry - Primary dependency management (`pyproject.toml`, `poetry.lock`)
- pip fallback via `requirements.txt`

## Frameworks

**Core:**
- Taipy 3.1.0 - Full-stack Python web framework (GUI, REST, Core, Config, Templates)
  - `app/main.py` - Application entry point with GUI initialization
  - `app/pages/*.py` - UI page definitions using Taipy DSL
- Flask 3.0.2 - Underlying WSGI framework (used by Taipy)
  - Flask-CORS 4.0.0, Flask-RESTful 0.3.10, Flask-SocketIO 5.3.6

**Testing:**
- None configured (pytest not in dependencies)

**Build/Dev:**
- Docker - Container runtime (`Dockerfile`)
- docker-compose - Local development (`docker-compose.yaml`)

## Key Dependencies

**Critical:**
- Polars 0.20.15 - Primary DataFrame library for data processing
- Pandas - Secondary DataFrame library (conversion bridge)
- psycopg2-binary 2.9.10+ - PostgreSQL adapter
- rpy2 3.5.15 - Python-R interface for ffscrapr
- NumPy 1.26.4 - Numerical computing
- SciPy 1.15.1 - Scientific computing

**Infrastructure:**
- Gevent 23.9.1 - Coroutine-based networking
- Werkzeug 3.0.1 - WSGI utilities
- SQLAlchemy 2.0.25 - Available but unused (psycopg2 used directly)

## Configuration

**Environment:**
- Environment variables with hardcoded defaults in `app/config/config.py`
- Key vars: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `PORT`
- `.env` support planned but not yet implemented

**Build:**
- `pyproject.toml` - Poetry project metadata and dependencies
- `Dockerfile` - Multi-stage Python + R environment
- `fly.toml` - Fly.io deployment configuration

## Platform Requirements

**Development:**
- Python 3.11+, R runtime, PostgreSQL access
- Docker recommended for full environment (R + Python)

**Production:**
- Fly.io container deployment (Amsterdam/`arn` region)
- VM: 1 CPU, 1GB RAM (shared), auto-scaling min 0
- Port 8080, force HTTPS
- Supabase PostgreSQL (EU Central region)

---

*Stack analysis: 2026-03-10*
*Update after major dependency changes*
