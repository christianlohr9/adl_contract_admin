# Project Issues Log

Enhancements discovered during execution. Not critical - address in future phases.

## Open Enhancements

### ISS-001: Extension window awareness — offseason/in-season signing periods

- **Discovered:** Phase 08 Task 04 (2026-03-12)
- **Type:** UX / Business Logic
- **Description:** The system needs to know the current league calendar period to correctly surface available actions. Currently in offseason: years_remaining=0 players are free agents eligible for tags and tenders (not extensions). Extensions have two distinct windows: offseason extensions end at NFL Combine TV coverage start (Feb 27 @ 3pm ET), and in-season extensions (iEXT) open at each player's Week 1 kickoff. The dashboard and contract tools should reflect which actions are actually available based on the current date relative to these deadlines.
- **Impact:** Low (tools still calculate correctly, but dashboard could mislead about what's actionable right now)
- **Effort:** Medium — requires a league calendar/period system and conditional logic in the dashboard action items
- **Suggested phase:** Future milestone — league calendar system

### ISS-002: Franchise tag / tender eligibility checks query wrong season for expired contracts

- **Discovered:** Phase 12-02 checkpoint verification (2026-03-12)
- **Type:** Bug / Data Model Mismatch
- **Description:** `check_tag_eligibility` (franchise_tags.py:185), `check_erfa_eligibility`, and `check_rfa_eligibility` all look for a contract in `season - 1` to find expired contracts (years_remaining=0). However, the MFL sync stores all contracts in the *current* season (2026), not the previous season. This means these eligibility checks always return "No previous contract found" and zero players appear as tag/tender eligible, even though 26 out of 45 Jets players have expired contracts (years_remaining=0) in season 2026. The franchise_tag column appears in the dashboard (window is open) but all cells show "—" because no action_group is built.
- **Example:** Bolton, Nick (player_id=1893, Jets) has a 2026 contract with years_remaining=0, salary=8.03, type=SD. `check_tag_eligibility(session, 1893, 2026)` queries `Contract.season == 2025` which returns nothing → "No previous contract found". The fix should query `Contract.season == season` with `Contract.years_remaining == 0` instead of `Contract.season == season - 1`.
- **Impact:** High — franchise tags, ERFA tenders, and RFA tenders are completely broken; no players show values for these actions
- **Effort:** Low — fix the season query in each eligibility checker (franchise_tags.py, tenders.py)
- **Suggested phase:** Phase 13 or hotfix — this is a functional bug, not an enhancement

## Closed Enhancements

[None yet]
