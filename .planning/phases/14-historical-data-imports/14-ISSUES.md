# UAT Issues: Phase 14

**Tested:** 2026-03-13 (re-verified after 14-FIX)
**Source:** .planning/phases/14-historical-data-imports/14-*-SUMMARY.md
**Tester:** User via /gsd:verify-work

## Open Issues

### UAT-003: Rate limit retries still insufficient — all years fail

**Discovered:** 2026-03-13 (re-verify after 14-FIX)
**Phase/Plan:** 14-FIX
**Severity:** Blocker
**Feature:** Historical score and contract sync
**Description:** Despite 14-FIX adding MFLRateLimitError to tenacity retry (7 attempts, exponential 4-60s) and per-year resilience, every year still fails with rate limit errors. The per-year try/except works correctly (continues to next year, logs errors), but the underlying rate limiting defeats all 7 retry attempts for every year. The 1-second `request_delay` is too aggressive for MFL's rate limits when making 18 requests per year (weeks 1-17 + YTD) across 5 years.
**Expected:** Backfill should successfully import historical data, even if slowly
**Actual:** All 5 score years and at least 1 contract year fail: `['Scores 2021: MFL API rate limit exceeded', 'Scores 2022: MFL API rate limit exceeded', ..., 'Contracts 2023: MFL API rate limit exceeded']`. Zero historical data imported.
**Repro:**
1. Start the application (backfill auto-triggers)
2. Wait ~15 minutes for backfill to exhaust retries on all years
3. Check `GET /api/sync/backfill-status` — shows `in_progress: false`, error populated
4. All `missing_score_years` and `missing_contract_years` unchanged

**Root cause analysis:**
- MFL rate limit is more aggressive than expected. 1s delay + 7 retries (4-60s backoff) is not enough.
- 90 total score API calls (5 years × 18 weeks) likely triggers a longer-term rate limit window
- Possible fix: increase `request_delay` to 3-5s, add inter-year cooldown (30-60s), or implement adaptive rate limiting that backs off based on 429 responses

### UAT-004: `completed_at` not set when backfill finishes with errors

**Discovered:** 2026-03-13
**Phase/Plan:** 14-02
**Severity:** Minor
**Feature:** Backfill status tracking
**Description:** When backfill completes with partial errors, `completed_at` remains `null` because it's only set in the no-errors branch (line 171 of historical_sync.py).
**Expected:** `completed_at` should be set whenever the backfill finishes, regardless of errors
**Actual:** `completed_at: null` when errors exist, even though `in_progress: false`
**Repro:** Check backfill-status after a run with errors — `completed_at` is null

## Resolved Issues

### UAT-001: Backfill fails on MFL API rate limit with no retry/backoff
**Resolved:** 2026-03-13 — Partially fixed in 14-FIX (retry logic added but insufficient)
**Commit:** f9fbe19
**Note:** Retry was added but rate limits still defeat it. Superseded by UAT-003.

### UAT-002: No historical data actually imported
**Resolved:** 2026-03-13 — Root cause same as UAT-001, superseded by UAT-003
**Commit:** 4ea8477
**Note:** Per-year resilience works correctly (continues on failure), but data still not imported due to UAT-003.

---

*Phase: 14-historical-data-imports*
*Tested: 2026-03-13*
