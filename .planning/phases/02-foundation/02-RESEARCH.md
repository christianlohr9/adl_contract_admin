# Phase 2: Foundation - Research

**Researched:** 2026-03-10
**Domain:** FastAPI + async SQLAlchemy + PostgreSQL project foundation
**Confidence:** HIGH

<research_summary>
## Summary

Researched the modern Python async web stack for building a production-ready FastAPI application with PostgreSQL. The standard approach uses FastAPI with async SQLAlchemy 2.0, asyncpg as the PostgreSQL driver, Alembic for migrations, and Pydantic v2 settings for configuration.

Key finding: SQLAlchemy 2.0's async support is mature and well-documented. The `AsyncSession` + `async_sessionmaker` + `create_async_engine` pattern is the established approach. Alembic requires a special async `env.py` that uses `run_sync` to bridge async engines with Alembic's sync migration runner. uv is production-ready for Docker builds with excellent layer caching support.

**Primary recommendation:** Use FastAPI + SQLAlchemy 2.0 (async) + asyncpg + Alembic + Pydantic v2 settings. Organize by domain (not file type). Multi-stage Docker builds with uv for fast, cached installs.
</research_summary>

<standard_stack>
## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| fastapi | 0.135.x | Async web framework | De facto Python API framework, native async, auto-docs |
| sqlalchemy | 2.0.48 | ORM + database toolkit | Industry standard, mature async support in 2.0+ |
| alembic | latest | Database migrations | Official SQLAlchemy migration tool |
| asyncpg | latest | Async PostgreSQL driver | ~5x faster than psycopg3 in raw benchmarks, purpose-built for asyncio |
| pydantic-settings | latest | Configuration management | Official Pydantic v2 settings with env file support |
| uvicorn | latest | ASGI server | Standard FastAPI production server |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| httpx | latest | Async HTTP client | Testing FastAPI with AsyncClient |
| pytest | latest | Test framework | All testing |
| pytest-asyncio | latest | Async test support | Testing async endpoints and DB operations |
| ruff | latest | Linter + formatter | Replaces flake8, black, isort in one tool |
| mypy | latest | Type checker | Static type analysis |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| asyncpg | psycopg3 (async) | psycopg3 has richer PG features (LISTEN/NOTIFY), but asyncpg is faster for standard CRUD |
| SQLAlchemy ORM | SQLModel | SQLModel is simpler but less mature, fewer escape hatches for complex queries |
| uvicorn | gunicorn+uvicorn | gunicorn adds process management; Docker typically handles this instead |

**Installation:**
```bash
uv add fastapi[standard] sqlalchemy[asyncio] asyncpg alembic pydantic-settings
uv add --dev pytest pytest-asyncio httpx ruff mypy
```
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Project Structure
```
src/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app factory, lifespan
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py         # Pydantic settings
│   │   └── db.py             # Engine, session factory, Base
│   ├── models/               # SQLAlchemy ORM models
│   │   ├── __init__.py
│   │   ├── base.py           # DeclarativeBase
│   │   ├── player.py
│   │   ├── team.py
│   │   └── contract.py
│   ├── schemas/              # Pydantic request/response schemas
│   │   └── __init__.py
│   └── api/                  # Route handlers (future phases)
│       └── __init__.py
├── migrations/               # Alembic migrations
│   ├── env.py                # Async-aware env.py
│   ├── versions/
│   └── alembic.ini
├── tests/
│   ├── conftest.py           # Async fixtures, test DB session
│   └── test_health.py
├── pyproject.toml
├── uv.lock
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── .github/
    └── workflows/
        └── ci.yml
```

### Pattern 1: Async Engine + Session Factory
**What:** Create async engine and session maker as module-level singletons
**When to use:** Always — this is the standard async SQLAlchemy setup

```python
# src/app/core/db.py
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.core.config import settings

engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    pool_pre_ping=True,
)

async_session = async_sessionmaker(engine, expire_on_commit=False)

async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
```

### Pattern 2: AsyncAttrs DeclarativeBase
**What:** Use AsyncAttrs mixin for lazy-loadable attributes in async context
**When to use:** Always with async SQLAlchemy — enables `await obj.awaitable_attrs.relationship`

```python
# src/app/models/base.py
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(AsyncAttrs, DeclarativeBase):
    pass
```

### Pattern 3: Pydantic v2 Settings with @lru_cache
**What:** Settings class with env file support, cached to avoid re-parsing
**When to use:** Always — standard FastAPI configuration pattern

```python
# src/app/core/config.py
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "ADL Contract Admin"
    debug: bool = False
    database_url: str = "postgresql+asyncpg://user:pass@localhost:5432/adl"
    test_database_url: str = "postgresql+asyncpg://user:pass@localhost:5432/adl_test"

@lru_cache
def get_settings() -> Settings:
    return Settings()

settings = get_settings()
```

### Pattern 4: FastAPI Lifespan (replaces deprecated on_event)
**What:** Use lifespan context manager instead of deprecated startup/shutdown events
**When to use:** Always — on_event is deprecated in FastAPI

```python
# src/app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    yield
    # shutdown
    await engine.dispose()

app = FastAPI(title="ADL Contract Admin", lifespan=lifespan)
```

### Anti-Patterns to Avoid
- **`@app.on_event("startup")`:** Deprecated — use lifespan context manager instead
- **Sync SQLAlchemy in async routes:** Blocks the event loop, defeats purpose of async
- **`expire_on_commit=True` (default):** Causes lazy load issues in async context — always set `expire_on_commit=False`
- **Creating engine per-request:** Engine is expensive — create once at module level
- **`Base.metadata.create_all()` in production:** Use Alembic migrations instead
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| DB migrations | Manual SQL scripts | Alembic | Autogenerate from model diffs, version tracking, rollbacks |
| Config management | Custom env parser | pydantic-settings | Type validation, .env support, nested settings, secrets |
| Linting + formatting | flake8 + black + isort | Ruff | Single tool, 10-100x faster, drop-in replacement |
| Async PG driver | psycopg2 wrapper | asyncpg | Purpose-built for asyncio, dramatically faster |
| ASGI server | Custom server | uvicorn | Production-tested, auto-reload in dev |
| Test client | requests + manual setup | httpx.AsyncClient | Native async, FastAPI integration via `app=app` |

**Key insight:** The FastAPI + SQLAlchemy async ecosystem is mature. Every piece has a standard, well-maintained solution. Custom alternatives are slower to build and harder to maintain.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Lazy Loading in Async Context
**What goes wrong:** Accessing a relationship attribute raises `MissingGreenlet` error
**Why it happens:** SQLAlchemy lazy loading is sync by default — can't issue SQL in async context without greenlet
**How to avoid:** Use `selectinload()` / `joinedload()` in queries, or use `AsyncAttrs` and `await obj.awaitable_attrs.relationship`
**Warning signs:** `MissingGreenlet: greenlet_spawn has not been called` error

### Pitfall 2: Alembic env.py Not Configured for Async
**What goes wrong:** Alembic can't connect to database, or migrations hang
**Why it happens:** Default Alembic env.py uses sync engine — needs async wrapper
**How to avoid:** Use Alembic's async template (`alembic init -t async`) or manually configure `run_sync` bridge in env.py
**Warning signs:** Alembic commands hang or fail with connection errors

### Pitfall 3: expire_on_commit=True (Default) with Async
**What goes wrong:** Accessing attributes after commit triggers lazy load → `MissingGreenlet`
**Why it happens:** Default `expire_on_commit=True` expires all attributes after commit, next access tries sync lazy load
**How to avoid:** Set `expire_on_commit=False` on `async_sessionmaker`
**Warning signs:** Errors only after `session.commit()`, not during query

### Pitfall 4: asyncpg Version Compatibility
**What goes wrong:** `create_async_engine` fails with cryptic errors
**Why it happens:** Certain asyncpg versions have compatibility issues with SQLAlchemy 2.0.x
**How to avoid:** Pin asyncpg to a known-compatible version, test on upgrade
**Warning signs:** Connection errors only in async mode, sync works fine

### Pitfall 5: Docker Layer Caching with uv
**What goes wrong:** Dependencies reinstalled on every source code change
**Why it happens:** Copying source before `uv sync` invalidates dependency layer cache
**How to avoid:** Two-stage sync — first `COPY pyproject.toml uv.lock` + `uv sync`, then `COPY src/`
**Warning signs:** Slow Docker builds even when only Python source changed
</common_pitfalls>

<code_examples>
## Code Examples

### Async Alembic env.py
```python
# Source: Alembic cookbook + SQLAlchemy async docs
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.core.config import settings
from app.models.base import Base

config = context.config
config.set_main_option("sqlalchemy.url", settings.database_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations():
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

### Dockerfile with uv (Multi-Stage)
```dockerfile
# Source: https://docs.astral.sh/uv/guides/integration/docker/
FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Then install the project
COPY src/ src/
RUN uv sync --frozen --no-dev

# --- Runtime stage ---
FROM python:3.13-slim

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app
COPY --from=builder /app/src /app/src
COPY migrations/ migrations/
COPY alembic.ini .

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Docker Compose (Dev)
```yaml
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: adl
      POSTGRES_PASSWORD: adl_dev
      POSTGRES_DB: adl
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  web:
    build: .
    depends_on:
      - db
    environment:
      DATABASE_URL: postgresql+asyncpg://adl:adl_dev@db:5432/adl
    ports:
      - "8000:8000"
    command: >
      sh -c "alembic upgrade head &&
             uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"
    volumes:
      - ./src:/app/src

volumes:
  pgdata:
```

### GitHub Actions CI
```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
          POSTGRES_DB: adl_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run mypy src/
      - run: uv run pytest
        env:
          DATABASE_URL: postgresql+asyncpg://test:test@localhost:5432/adl_test
```

### Async Session Dependency for FastAPI
```python
# Source: SQLAlchemy 2.1 async docs + FastAPI patterns
from typing import Annotated, AsyncGenerator
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.db import async_session

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session

SessionDep = Annotated[AsyncSession, Depends(get_session)]
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `@app.on_event("startup")` | `lifespan` context manager | FastAPI 0.109+ | on_event deprecated, lifespan is cleaner |
| SQLAlchemy 1.x style | SQLAlchemy 2.0 `Mapped[]` annotations | SQLAlchemy 2.0 (2023) | Type-safe models, better IDE support |
| pip + requirements.txt | uv + pyproject.toml + uv.lock | uv 0.1+ (2024) | 10-100x faster installs, cross-platform lockfile |
| flake8 + black + isort | Ruff | Ruff 0.1+ (2023) | Single tool, 10-100x faster, drop-in replacement |
| psycopg2 (sync) | asyncpg (async) | SQLAlchemy 2.0+ | Native async, ~5x faster for async workloads |
| Session() sync | AsyncSession | SQLAlchemy 2.0+ | Non-blocking DB operations in async frameworks |

**New tools/patterns to consider:**
- **SQLAlchemy 2.1 (beta):** Now in beta (2.1.0b1, Jan 2026). Targets Python 3.10+, free-threaded Python support. Stay on 2.0.48 stable for now.
- **uv in Docker:** Official Astral Docker image (`ghcr.io/astral-sh/uv`) for multi-stage builds. 75% reduction in build times.

**Deprecated/outdated:**
- **`@app.on_event`:** Use lifespan instead
- **`declarative_base()`:** Use `DeclarativeBase` class instead
- **`Column()` + `relationship()`:** Use `Mapped[]` + `mapped_column()` + `relationship()` with type annotations
- **pip/poetry in Docker:** uv is significantly faster with better caching
</sota_updates>

<open_questions>
## Open Questions

1. **asyncpg version pinning**
   - What we know: Some asyncpg versions have SQLAlchemy 2.0.x compatibility issues
   - What's unclear: Exact version constraints for SQLAlchemy 2.0.48
   - Recommendation: Don't pin preemptively — test during setup, pin only if issues arise

2. **SQLAlchemy 2.1 adoption**
   - What we know: 2.1.0b1 released Jan 2026, targets Python 3.10+
   - What's unclear: Timeline to stable, breaking changes from 2.0
   - Recommendation: Use 2.0.48 stable. Upgrade when 2.1 goes GA.
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- Context7: /websites/sqlalchemy_en_21 — async session, engine, AsyncAttrs patterns
- Context7: /websites/fastapi_tiangolo — dependency injection, session management
- Context7: /sqlalchemy/alembic — async migration cookbook
- Context7: /websites/benavlabs_github_io_fastapi-boilerplate — project structure, Docker setup
- https://docs.astral.sh/uv/guides/integration/docker/ — uv Docker best practices
- https://docs.astral.sh/uv/guides/projects/ — uv project setup

### Secondary (MEDIUM confidence)
- https://fastlaunchapi.dev/blog/fastapi-best-practices-production-2026 — production patterns (verified against official docs)
- https://medium.com/@tclaitken/setting-up-a-fastapi-app-with-async-sqlalchemy-2-0-pydantic-v2-e6c540be4308 — async setup patterns (verified)
- https://fernandoarteaga.dev/blog/psycopg-vs-asyncpg/ — driver comparison (cross-referenced with benchmarks)
- https://depot.dev/docs/container-builds/how-to-guides/optimal-dockerfiles/python-uv-dockerfile — Docker patterns (verified against official uv docs)

### Tertiary (LOW confidence - needs validation)
- None — all findings verified against primary sources
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: FastAPI 0.135.x + SQLAlchemy 2.0.48 (async) + PostgreSQL
- Ecosystem: asyncpg, Alembic, pydantic-settings, uv, Ruff, mypy, pytest
- Patterns: Async session management, lifespan, multi-stage Docker, CI
- Pitfalls: Lazy loading in async, Alembic async env, expire_on_commit, Docker caching

**Confidence breakdown:**
- Standard stack: HIGH — verified with Context7 + official docs
- Architecture: HIGH — from official examples and production boilerplates
- Pitfalls: HIGH — documented in SQLAlchemy docs, community-verified
- Code examples: HIGH — from Context7/official sources, cross-referenced

**Research date:** 2026-03-10
**Valid until:** 2026-04-10 (30 days — mature ecosystem, stable)
</metadata>

---

*Phase: 02-foundation*
*Research completed: 2026-03-10*
*Ready for planning: yes*
