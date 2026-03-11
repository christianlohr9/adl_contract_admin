# Phase 4: Contract Engine - Context

**Gathered:** 2026-03-11
**Status:** Ready for planning

<vision>
## How This Should Work

Each contract tool (extensions, franchise/transition tags, ERFA tenders, buyouts/restructures) is built as a complete end-to-end feature. A GM provides a player and contract, and the engine returns all valid options with calculated values — "here are your extension options and what they'd cost."

The EPV calculation logic is baked into each tool as needed, not built as a standalone layer first. The tools are internal services at this point — no REST endpoints, just the calculation engine that Phase 6 will expose.

Think of it as: give me a player, I'll tell you everything you can do with their contract and what each option costs.

</vision>

<essential>
## What Must Be Nailed

- **All tools functional** — Every contract tool (extensions X-A/B, franchise/transition tags X-C, ERFA tenders X-D, buyouts/restructures X-E) must be working. Breadth of coverage matters most.
- **Bylaws as source of truth** — The bylaws and rules YAML files are the authority. If the old spreadsheet or old EPV code disagrees, bylaws win. Phase 1's extracted rules drive every calculation.
- **Input → options back** — Clean service interface: provide a player/contract, get back all valid options with calculated values.

</essential>

<boundaries>
## What's Out of Scope

- No API endpoints — REST layer comes in Phase 6
- No salary cap impact calculations — that's Phase 5
- No frontend or user-facing interface
- No transaction persistence or audit trails yet

</boundaries>

<specifics>
## Specific Ideas

- Bylaws/rules YAML are the source of truth for all calculations, not the old spreadsheet
- Each tool should be a self-contained service that takes inputs and returns structured results
- EPV logic is embedded within the tools that need it, not a separate abstraction layer

</specifics>

<notes>
## Additional Context

The roadmap has four plans for this phase: EPV calculation logic (04-01), extensions (04-02), tags and tenders (04-03), buyouts and restructures (04-04). The user's preference for end-to-end tools means EPV porting in 04-01 should focus on getting the core calculation working so the subsequent plans can build complete tools on top of it.

Phase 1 extracted all rules into `rules/constants/`, `rules/formulas/`, and `rules/docs/` — these are the authoritative source for all contract engine calculations.

</notes>

---

*Phase: 04-contract-engine*
*Context gathered: 2026-03-11*
