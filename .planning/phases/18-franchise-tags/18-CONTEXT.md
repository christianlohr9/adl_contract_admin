# Phase 18: Franchise Tags - Context

**Gathered:** 2026-04-01
**Status:** Ready for planning

<vision>
## How This Should Work

The app should perfectly calculate every player's franchise tag eligibility and pricing. The user will compare the app's UI against the 2026 ADL Contract Admin spreadsheet themselves — no new export tools or comparison pages needed. The existing player/roster pages should display FT eligibility and prices clearly enough to cross-check.

The approach is fix-as-you-go, one layer at a time:
1. **FT eligibility** — get every player's yes/no right first
2. **EFT prices** — exclusive franchise tag dollar amounts
3. **NEFT prices** — non-exclusive franchise tag dollar amounts
4. **TT prices** — transition tag dollar amounts

Each step gets validated (spot-checked in the UI against the spreadsheet) before moving to the next. The bylaw rules drive all calculations.

</vision>

<essential>
## What Must Be Nailed

- **Eligibility accuracy** — every player's FT eligibility must match the spreadsheet (687 players)
- **Price accuracy** — EFT, NEFT, and TT dollar amounts must be exact against the positional salary table
- Both are equally critical — can't ship one without the other being correct

</essential>

<boundaries>
## What's Out of Scope

- No new UI pages — don't build dedicated franchise tag pages, just ensure existing views display correct data
- No GM-facing tag application workflows — this phase is about getting calculations right, not building tag-use flows
- No automated comparison tooling — the user will manually cross-check against the spreadsheet

</boundaries>

<specifics>
## Specific Ideas

- Step-by-step validation order: eligibility -> EFT -> NEFT -> TT
- Verification is manual spot-checking in the UI after each fix
- Existing player detail pages should surface FT info clearly enough to cross-check (need to verify current state during research)

</specifics>

<notes>
## Additional Context

The spreadsheet references are TagElig26 (eligibility) and FT/5YO $ (positional salary table for pricing). All calculations should be driven by bylaw rules, not spreadsheet formulas — the spreadsheet is the validation target, not the source of truth for logic.

</notes>

---

*Phase: 18-franchise-tags*
*Context gathered: 2026-04-01*
