# Phase 19: Extensions - Context

**Gathered:** 2026-04-02
**Status:** Ready for research

<vision>
## How This Should Work

Same two-step pattern as Phase 18 (Franchise Tags): validate eligibility first, then validate pricing separately. Each step produces a comparison script that checks app output against spreadsheet values cell-by-cell, and we fix discrepancies until zero remain.

The iEXT vs oEXT distinction is determined by a date cutoff (season beginning) — a player is one or the other at any given time, never both. It's possible that all players are EXT-eligible by default per the rules, which would simplify the eligibility pass.

For pricing, EPV/EYS calculations are the real complexity. The EXT spreadsheet has two mirrored column ranges (NFC in A-Y, AFC in AA-AY — same player pool, only need to check one). Three key columns — PRcurrentNR, PRrecentR, PRpreviousR — represent the last three robust seasons and feed into the pricing formula.

</vision>

<essential>
## What Must Be Nailed

- **EPV/EYS pricing accuracy** — This is the hard part and the core of the phase. Extension price calculations must match the spreadsheet exactly.
- **iEXT vs oEXT classification** — Date-based distinction must be correct for all ~1,008 players.

</essential>

<boundaries>
## What's Out of Scope

- Cross-tool interactions (how extensions interact with tags, tenders, etc.) — that's Phase 23
- UI changes — this is purely backend validation and fixes
- ADL Cap Percentage applicability to extensions — unknown yet, will determine during research

</boundaries>

<specifics>
## Specific Ideas

- Follow Phase 18's proven two-plan structure: Plan 19-01 for eligibility, Plan 19-02 for pricing
- EXT spreadsheet structure: NFC (cols A-Y) and AFC (cols AA-AY) are mirrored — only need one range for validation
- PRcurrentNR, PRrecentR, PRpreviousR columns are the three robust season inputs to pricing

</specifics>

<notes>
## Additional Context

User suspects all players may be EXT-eligible by default — needs verification against bylaws and spreadsheet during research. The eligibility pass may be simpler than franchise tags.

The pricing pass is where the real work lives — understanding how the three robust season columns feed into EPV/EYS calculations.

</notes>

---

*Phase: 19-extensions*
*Context gathered: 2026-04-02*
