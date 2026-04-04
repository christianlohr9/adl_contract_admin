# Phase 26: Production Configuration - Context

**Gathered:** 2026-04-04
**Status:** Ready for planning

<vision>
## How This Should Work

Simple environment switching — dev vs prod controlled by environment variables. Someone can clone the repo, set a few env vars, and run it in production mode without surprises. No hardcoded URLs, secrets, or dev-only settings leaking into prod.

The .env.example file should be the complete source of truth — every configurable value documented there. If you set the vars and run it, it just works.

</vision>

<essential>
## What Must Be Nailed

- **Clean env config** — One .env file controls everything, nothing hardcoded that shouldn't be
- **No secrets exposed** — API keys, DB credentials, nothing sensitive in the repo or leaking to the frontend
- **Clear setup docs** — A README section that says "set these env vars and run" — no guessing
- **Just works anywhere** — Set env vars, run it, production mode works without surprises

</essential>

<boundaries>
## What's Out of Scope

- No domain/DNS/CORS configuration — handle that when we know the actual deployment URL in Phase 27
- No deployment infrastructure — no Dockerfiles, CI/CD, hosting setup (that's Phase 27)
- Runtime decisions (Docker vs bare-metal) — defer to Phase 27
- Custom error pages, structured logging, health checks — not in scope for this config-focused phase

</boundaries>

<specifics>
## Specific Ideas

- Clean up .env.example to be complete and accurate — every env var documented
- Audit for hardcoded values that should be configurable
- Make sure debug/dev-only features don't leak into production mode
- No sensitive values committed to the repo

</specifics>

<notes>
## Additional Context

This is a fantasy football league contract admin tool used by 32 GMs. The "someone else running it" scenario is a co-commissioner. The bar is: they can follow setup docs and get it running without asking questions.

Phase 27 handles the actual deployment (free-tier hosting, CI/CD, Docker config). This phase just makes the app *ready* to deploy by cleaning up configuration.

</notes>

---

*Phase: 26-production-configuration*
*Context gathered: 2026-04-04*
