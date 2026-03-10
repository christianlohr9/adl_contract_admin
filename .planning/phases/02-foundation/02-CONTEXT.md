# Phase 2: Foundation - Context

**Gathered:** 2026-03-10
**Status:** Ready for planning

<vision>
## How This Should Work

Clean slate. Archive the old Taipy app to a separate git branch, then build a fresh, production-ready project from scratch. This should feel like starting a real software project — proper `src/` layout, `pyproject.toml`, Docker, CI — not a prototype or experiment.

Everything runs through Docker Compose. `docker compose up` and you have FastAPI + PostgreSQL running. `docker compose run test` and your test suite executes against a real test database. No mocks, no faking it.

The DB schema should be designed by Claude based on deep knowledge of the bylaws rules — whatever structure makes the contract engine easiest to build in Phase 4. Alembic migrations from the start so schema evolution is tracked.

Fully async from day one — async SQLAlchemy sessions, async FastAPI routes. Pydantic v2 settings for configuration (env vars, `.env` files). Modern Python throughout.

</vision>

<essential>
## What Must Be Nailed

- **Schema accuracy** — DB models must perfectly reflect the bylaws rules. If the schema is right, everything downstream follows.
- **Developer experience** — One command to run, easy migrations, clean imports. Building on top of this should be fast and pleasant.
- **Both are non-negotiable** — a beautiful DX on a wrong schema is useless, and a perfect schema that's painful to work with slows everything down.

</essential>

<boundaries>
## What's Out of Scope

- No business logic — no contract calculations, no EPV engine, no validation. Just structure and storage.
- No real API endpoints — FastAPI scaffold exists with health check, but no domain routes. Those come in Phase 6.
- No seed data, no mocks — real data comes from MFL API in Phase 3. Schemas only.
- No deployment target — Docker Compose for local dev/testing only. Production deployment is a later concern.

</boundaries>

<specifics>
## Specific Ideas

- **uv** for package management (not pip/poetry)
- **Ruff + mypy** for code quality from day one (strict linting, formatting, type checking)
- **GitHub Actions CI** running pytest on every push from day one
- **Pydantic v2 settings** for config management
- **Fully async** — async SQLAlchemy, async FastAPI routes
- **Docker Compose** as the only way to run/test the app
- **Old app archived to a separate branch** — keep main clean, git history preserved

</specifics>

<notes>
## Additional Context

The user explicitly does not want mocks or test fixtures with fake data. The foundation should be real infrastructure that real data (from MFL API in Phase 3) will flow into.

Schema design is delegated to Claude — use knowledge of the bylaws and Phase 1 rules extraction to design whatever structure makes the contract engine (Phase 4) easiest to build.

</notes>

---

*Phase: 02-foundation*
*Context gathered: 2026-03-10*
