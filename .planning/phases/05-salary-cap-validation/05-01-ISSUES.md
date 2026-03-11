# UAT Issues: Phase 5 Plan 1

**Tested:** 2026-03-11
**Source:** .planning/phases/05-salary-cap-validation/05-01-SUMMARY.md
**Tester:** User via /gsd:verify-work

## Open Issues

[None — all issues resolved]

## Resolved Issues

### UAT-001: NEFToff/TToff designations not matched as tag contracts
**Resolved:** 2026-03-11 - Fixed in 05-01-FIX.md
**Commit:** 5a91db9
**Severity:** Major
**Fix:** Updated `_TAG_RE` to `r"\b(EFT|NEFT|TT)(\d+|OFF)?\b"` — matches tag variants with uppercase

### UAT-002: iEXT/oEXT not matched by EXT regex
**Resolved:** 2026-03-11 - Fixed in 05-01-FIX.md
**Commit:** 5a91db9
**Severity:** Minor
**Fix:** Updated `_EXT_RE` to `r"\b[IO]?EXT\b"` — matches extension variants with uppercase prefix

---

*Phase: 05-salary-cap-validation*
*Plan: 01*
*Tested: 2026-03-11*
