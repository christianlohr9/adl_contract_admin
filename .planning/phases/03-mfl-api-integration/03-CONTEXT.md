# Phase 3: MFL API Integration - Context

**Gathered:** 2026-03-10
**Status:** Ready for research

<vision>
## How This Should Work

MFL data syncs into our database on a scheduled basis — franchises, rosters, player scores — all staying fresh automatically without manual intervention. MFL is the source of truth; our app reads from it and layers contract calculations on top.

The sync should just run in the background. GMs open the app and see current data without thinking about where it comes from. When MFL is down or a sync fails, the app keeps working with the last good data — just show when it was last refreshed.

Historical data matters too. Past season data feeds into EPV calculations and contract decisions, so the sync needs to handle multiple seasons, not just the current one.

</vision>

<essential>
## What Must Be Nailed

- **Right API approach** — Evaluate pymfl, ffscrapr, and direct MFL API calls to pick the approach that fits best. This is the foundation for all MFL data access going forward, so getting it right matters more than getting it fast.
- **MFL is source of truth** — Our app reads from MFL. The data pipeline needs to be reliable and accurate.
- **ADL-specific configurations** — The ADL has custom scoring rules AND custom roster positions in MFL. The sync needs to handle these non-standard configurations correctly.

</essential>

<boundaries>
## What's Out of Scope

- No contract calculations — just get raw data in. EPV logic and contract tools are Phase 4.
- No UI for sync status — backend sync only, no admin dashboard or sync monitoring screens yet.
- Access control — all GMs see the same data; permissions are a later concern.

</boundaries>

<specifics>
## Specific Ideas

- Three API approaches to evaluate: [pymfl](https://github.com/joeyagreco/pymfl) (Python), [ffscrapr](https://github.com/ffverse/ffscrapr) (R), or direct API calls via [MFL API](https://api.myfantasyleague.com/2023/api_info)
- Graceful degradation on API failures — app works with stale data, shows last sync timestamp
- Scheduled background sync (frequency TBD during research)
- Historical season data needed, not just current season

</specifics>

<notes>
## Additional Context

The ADL is a 32-team dynasty league with custom scoring and roster configurations in MFL. The research phase should investigate what MFL endpoints expose these custom settings and ensure whichever API approach is chosen can handle them.

The user is open to any approach — the research phase should make the call based on what fits best with the Python/FastAPI stack and the specific data needs.

</notes>

---

*Phase: 03-mfl-api-integration*
*Context gathered: 2026-03-10*
