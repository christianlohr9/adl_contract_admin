# Phase 7: Frontend Placeholder - Context

**Gathered:** 2026-03-11
**Status:** Ready for planning

<vision>
## How This Should Work

The frontend is a GM-centric dashboard. When a GM opens the app, they should immediately see what actions they can take — extensions available, tags to use, upcoming deadlines. It's action-oriented, not just a data dump.

The app has a single-page feel with sidebar navigation between sections. The sidebar gives access to different areas (dashboard/home, roster, contract tools, salary cap) without full page reloads. It should feel modern and clean — think Tailwind + shadcn/ui style: minimal, professional, no visual clutter.

When a GM clicks on a player from their roster, they navigate to a dedicated player detail page. That page shows all the contract tools (extensions, tags, tenders, buyout, 5YO, PPE) organized in tabs — one tool at a time, clean and focused.

</vision>

<essential>
## What Must Be Nailed

- **Page structure and routing** — Get the right pages and routes so Phase 8 just fills in real components. Navigation, layout shell, page shells all in place.
- **Sidebar navigation pattern** — Single-page feel with sidebar sections, not a traditional multi-page nav bar.
- **Player detail page with tabs** — The key interaction pattern: roster → click player → tabbed contract tools view.

</essential>

<boundaries>
## What's Out of Scope

- No real API data — placeholder pages with mock/static content only. No API calls until Phase 8.
- No authentication/login — just assume you're a GM for now. Auth is a future concern.
- No team-scoping decision — whether GMs can browse other teams is deferred.
- No component library selection deep-dive — pick something sensible (Tailwind + shadcn/ui direction) and move on.

</boundaries>

<specifics>
## Specific Ideas

- Modern & clean aesthetic: Tailwind + shadcn/ui or similar component approach
- Sidebar navigation with sections (not top nav bar)
- Player detail page uses tabs for contract tools (extensions, tags, tenders, buyout, 5YO, PPE)
- Action-oriented dashboard home: "What can I do right now?" not "Here's all your data"

</specifics>

<notes>
## Additional Context

This is a scaffold phase — the goal is getting the bones right so Phase 8 can focus on building real UI components without reworking structure. Every page should exist as a placeholder with the right route, the right position in navigation, and a clear indication of what will go there.

</notes>

---

*Phase: 07-frontend-placeholder*
*Context gathered: 2026-03-11*
