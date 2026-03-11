# Phase 6: API Layer - Context

**Gathered:** 2026-03-11
**Status:** Ready for planning

<vision>
## How This Should Work

The API is a GM-centric data-browsing layer with contract tools built in. When a GM opens the app, they should be able to pull their full team picture — roster, contracts, cap situation — in a single call. It's their franchise at the center of everything.

For contract tools, it's player-centric: hit one endpoint with a player ID and get back everything available — extension terms, tag prices, tender eligibility, buyout costs. "What can I do with this player?" answered in one call.

The API is read-only. Data flows in through MFL sync (already built in Phase 3), and the API exposes it for browsing and calculations. No write operations, no auth — keep it simple and focused.

Team-scoped endpoints only for this phase. League-wide views (cap standings, player search, free agent lists) come later when the frontend needs them.

</vision>

<essential>
## What Must Be Nailed

- **Contract tools are accurate** — Extension, tag, tender, and buyout calculations return correct, complete results a GM can act on
- **Team context is rich** — A GM can pull their full picture (roster, contracts, cap, scores) in a way that makes the frontend easy to build
- **Bundled team snapshot** — One endpoint returns the full team situation, not a dozen separate calls

</essential>

<boundaries>
## What's Out of Scope

- No write operations — API is read-only plus calculations. Data changes only happen through MFL sync
- No authentication/authorization — any request can access any team's data for now
- No league-wide aggregation views — defer to frontend phase when needed
- Existing sync endpoint stays as-is — don't evolve or modify it

</boundaries>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches

</specifics>

<notes>
## Additional Context

The API serves 32 GMs who primarily care about their own franchise. The design should optimize for that use case — quick access to "my team" and "my player options" rather than broad league browsing.

Five phases of backend work are already complete: models, sync, contract engine, cap logic. This phase is about exposing all of that through clean REST endpoints that make the frontend (Phases 7-8) straightforward to build.

</notes>

---

*Phase: 06-api-layer*
*Context gathered: 2026-03-11*
