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

## Closed Enhancements

[None yet]
