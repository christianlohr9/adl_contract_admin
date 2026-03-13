# Phase 14: Historical Data Imports - Context

**Gathered:** 2026-03-13
**Status:** Ready for research

<vision>
## How This Should Work

When the app starts up, it automatically detects any missing historical data and begins pulling it from MFL in the background. There's no admin trigger, no manual import — it just knows what's missing and fills the gaps silently.

The key timing insight: MFL rolls over to the new league year on February 21st each year. That's when the previous season's data becomes "last season" and is available for historical pull. The app should be aware of this boundary.

While backfilling, the app serves what it has immediately but warns users when eligibility checks might be incomplete due to missing historical data. Once the backfill completes, those warnings disappear and eligibility is fully accurate.

</vision>

<essential>
## What Must Be Nailed

- **Accurate eligibility data** — The entire reason for this phase. Historical scores feed extension eligibility (EPV-based), and past contract history feeds tag/tender eligibility. Both are equally critical — can't have accurate eligibility without both.
- **Automatic gap detection** — App knows what historical data it has and what's missing, no manual intervention needed.

</essential>

<boundaries>
## What's Out of Scope

- No UI for imports — purely backend, no admin dashboard or progress indicators
- No eligibility logic changes — just get the data in; Phase 15 handles auditing and fixing eligibility logic
- No pre-2020 data — league exists from ~2016-2017 but historical data from before 2020 isn't needed for eligibility purposes

</boundaries>

<specifics>
## Specific Ideas

- Pull data from 2020 season onwards — that's the practical range needed
- February 21st is the MFL league year rollover date — import logic should be aware of this boundary
- Background sync on startup, not scheduled jobs
- Warn users when eligibility results might be incomplete due to missing history (don't block, don't silently give wrong answers)

</specifics>

<notes>
## Additional Context

This phase directly addresses two deferred issues:
- ISSUE-001 (Phase 8): Extension eligibility needs historical player scores imported
- ISSUE-002 (Phase 8): Tags/tenders need multi-season contract history import

Both score history and contract history are equally important — they serve different eligibility checks but both must be present for the system to give accurate results. Storage approach (same tables vs separate) is a technical decision to be made during research/planning.

</notes>

---

*Phase: 14-historical-data-imports*
*Context gathered: 2026-03-13*
