# Phase 24: Repo Polish - Context

**Gathered:** 2026-04-04
**Status:** Ready for planning

<vision>
## How This Should Work

A full spring cleaning of the codebase. Every file gets scrutinized — dead code removed, unused dependencies purged, AI-generated comment slop stripped out, orphaned files deleted. When you open any file afterward, every line earns its place. The code speaks for itself.

At the repo level, it should look like a well-maintained, professional codebase — proper .gitignore, no dev artifacts checked in, no database files or spreadsheets in the root. This is portfolio-grade work that reflects strong engineering discipline to potential employers.

The ADL shield logo gets added as the favicon, replacing the default React logo.

</vision>

<essential>
## What Must Be Nailed

- **Dead code & dependency removal** — The #1 priority. Unused imports, unreachable code paths, orphaned files, dependencies that nothing references anymore. All of it goes.
- **Lean, intentional code** — No verbose AI-generated comments explaining obvious things, no redundant docstrings, no over-commented code. If the code is clear, it doesn't need a comment.
- **Professional repo hygiene** — No .db files, .xlsx files, or other artifacts in the repo root. Clean .gitignore. The repo looks like something you'd be proud to share.

</essential>

<boundaries>
## What's Out of Scope

- No refactoring — don't restructure or rewrite working code, just remove what's dead and clean what's messy
- No new features — this phase is purely subtractive (except the favicon)
- No UX changes — that's Phase 25
- .planning/ directory stays — it shows the engineering process and is a positive portfolio signal

</boundaries>

<specifics>
## Specific Ideas

- Use the existing ADL shield logo (provided by user) as the favicon
- Clean tests too — remove dead test files, unused fixtures, and tests for code that no longer exists
- Trust Claude to scan the full codebase and identify what needs cleaning — no specific hit list from the user

</specifics>

<notes>
## Additional Context

The audience for this repo is potential employers. This isn't just functional cleanup — it's about the repo reflecting professional engineering quality. The .planning/ directory is deliberately kept because it demonstrates process and architectural thinking, which is a positive signal.

</notes>

---

*Phase: 24-repo-polish*
*Context gathered: 2026-04-04*
