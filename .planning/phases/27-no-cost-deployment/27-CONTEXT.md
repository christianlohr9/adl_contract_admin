# Phase 27: No-Cost Deployment - Context

**Gathered:** 2026-04-08
**Status:** Ready for research

<vision>
## How This Should Work

The app gets deployed to whatever free-tier platform works easiest for FastAPI + Postgres + React. The whole point is simplicity — get it live with minimal fuss so I can paste a link in the league group chat and GMs can start using it.

When I push code to main, it should just deploy automatically. No SSH, no manual steps, no deploy commands. Push and forget.

The app should periodically sync from MFL API to keep rosters and data fresh, rather than calling MFL live on every request or relying on a one-time snapshot.

Cold starts are totally fine — it's a small league tool, a few seconds of spin-up is no big deal.

</vision>

<essential>
## What Must Be Nailed

- **Zero cost** — Must be completely free. Credit card on file is acceptable if there's a hard spending cap, but no surprise bills ever.
- **Push-to-deploy** — Git push to main triggers automatic redeployment. No manual intervention needed.
- **Data seeding on deploy** — The deployment should handle getting contract/roster data into the production database, pulling from MFL API as part of setup.
- **Periodic MFL sync** — Production app syncs from MFL on a schedule (daily/weekly) to stay current without hammering the API.

</essential>

<boundaries>
## What's Out of Scope

- Custom domain / DNS — platform-provided URL (e.g., myapp.fly.dev) is perfectly fine
- Auth / login — no user accounts, anyone with the link can use it
- Multiple environments — no staging/test, just production
- Database backups — not this phase
- Monitoring/alerting — not this phase

</boundaries>

<specifics>
## Specific Ideas

- Share the app by pasting the URL in the league group chat — no onboarding needed
- 32 GMs + commissioner are the only users — low traffic, no scale concerns
- Platform should have no-charge guarantees or hard spending limits to prevent cost surprises

</specifics>

<notes>
## Additional Context

This is the final phase of v1.4 and the last phase in the roadmap. The goal is to get the app live so commissioners can evaluate it as a replacement for the spreadsheet. The UX redesign (Phase 25) was specifically designed to make commissioners say "yes, this replaces the spreadsheet" — now it needs to be accessible.

The user prefers the simplest possible path. Research should evaluate platforms (Fly.io, Render, Railway) for the best fit given FastAPI + PostgreSQL + React SPA on a free tier.

</notes>

---

*Phase: 27-no-cost-deployment*
*Context gathered: 2026-04-08*
