# Phase 8: Frontend UI - Context

**Gathered:** 2026-03-11
**Status:** Ready for research

<vision>
## How This Should Work

GMs open the app and land on a full dashboard experience — their home base. The first thing they see is what needs attention: expiring contracts, pending decisions, upcoming deadlines. It's action-item driven, not just a data dump. The roster and cap numbers are there but secondary to "here's what you should deal with."

When a GM wants to evaluate a player's options, they go to the player detail page and see a comparison view — all contract tools (extensions, tags, tenders, buyouts) laid out side-by-side. No clicking through tabs or wizards. One fetch, everything visible, so they can compare "what does extending vs tagging vs buying out look like?"

The salary cap page goes beyond a static ledger — it supports scenario modeling. "What if I extend this player? What does my cap picture look like then?" This ties the contract tools directly into cap planning.

Data tables are clean and minimal — Apple-style with whitespace, only essential columns visible. Not a Bloomberg terminal, not a fantasy sports app. Trust in the data is paramount — numbers need to be clearly formatted, sortable, and match what GMs expect from the bylaws.

GMs find players two ways: a quick search bar always available in the header for finding someone fast, plus browse-by-team as the main roster navigation path.

The app supports light actions — flagging players, saving notes, bookmarking scenarios — but no actual transactions. Decisions still happen through MFL or the commish. This is a decision-support tool that helps GMs see the numbers and plan their moves.

</vision>

<essential>
## What Must Be Nailed

- **Data feels trustworthy** — Numbers match expectations, tables are sortable/filterable, GMs trust what they see. If they don't trust the data, nothing else matters.
- **Comparison view for contract tools** — All options for a player visible side-by-side. This is the killer feature enabled by the bundled API endpoint.
- **Action-item driven dashboard** — GMs immediately see what needs their attention when they land.

</essential>

<boundaries>
## What's Out of Scope

- Auth & multi-user — No login, no per-GM permissions. Anyone with the URL can use it.
- Mobile optimization — Desktop-first. GMs use laptops/desktops for this work.
- Full transaction workflows — No executing trades or submitting extensions through MFL. Read-only decision support with light personal actions (flags, notes, bookmarks).

</boundaries>

<specifics>
## Specific Ideas

- Comparison view on player detail page showing extensions vs tags vs tenders vs buyouts side-by-side
- Salary cap scenario modeling — "what if I extend this player?" preview of cap impact
- Always-visible search bar in the header for quick player lookup
- Clean, minimal table design with whitespace — not dense/cluttered
- Light actions: flag players, bookmark scenarios, save notes (no MFL write-back)
- Dashboard surfaces expiring contracts and pending decisions first

</specifics>

<notes>
## Additional Context

The bundled API endpoint (/{player_id}/all) already returns all contract tools in one response, making the comparison view a natural fit — simpler than building a multi-step wizard.

GM trust in the data is the top priority. The app replaces a Google Sheet that GMs are familiar with, so numbers need to match their expectations from the bylaws. Clear formatting and sortable data are non-negotiable.

</notes>

---

*Phase: 08-frontend-ui*
*Context gathered: 2026-03-11*
