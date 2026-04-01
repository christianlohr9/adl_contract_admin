# Phase 18: Franchise Tags — Issues

## ISS-018-001: Incomplete roster data for teams 129-144 (Conference 1)

**Discovered:** 2026-04-01 during Phase 18-01 FT eligibility validation
**Severity:** Data integrity
**Status:** Open

Teams 129-144 have only 2-12 roster entries in season 2026, while teams 145-160 have the expected 45-53. This suggests a partial roster import — likely Conference 1 rosters were not fully loaded.

**Impact:** Roster and contract management pages show incomplete data for affected teams. Eligibility checks still work correctly for players that ARE in the DB, but many players are missing entirely.

**Expected:** Each team should have ~45-53 roster entries.

**Actual counts (season 2026):**
- Teams 129-144: 2-12 players each
- Teams 145-160: 45-53 players each (correct)
