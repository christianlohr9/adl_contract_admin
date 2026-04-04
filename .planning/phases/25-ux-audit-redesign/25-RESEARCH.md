# Phase 25: UX Audit & Redesign - Research

**Researched:** 2026-04-04
**Domain:** Dark-mode sports UI redesign with dynamic team branding (Tailwind v4 + shadcn)
**Confidence:** HIGH

<research_summary>
## Summary

Researched how to transform the existing ADL Contract Admin app into a Sleeper-inspired dark sports product with per-team accent branding. The current stack (Tailwind CSS v4 + shadcn/ui + React 19) is ideal for this — Tailwind v4's CSS custom properties system enables runtime theme switching via `data-theme` attributes without rebuilds, and shadcn's variable-based theming makes dynamic accent colors trivial.

The key insight: **this is a CSS variables problem, not a component library problem.** The existing shadcn components already consume `--primary`, `--accent`, etc. By swapping those variables per-team via a `[data-team="xxx"]` selector, every button, card, badge, and sidebar element automatically picks up team branding. No component changes needed for the color system.

**Primary recommendation:** Keep the dark base theme constant (dark grays/near-black backgrounds). Define per-team accent colors as CSS variable overrides scoped to `[data-team]` selectors. Use MFL player photo URLs for headshots. Build a splash-screen team picker as the landing page with a grid of NFL team logos/helmets.
</research_summary>

<standard_stack>
## Standard Stack

### Core (Already Installed — No Changes)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tailwindcss | 4.2.1 | Utility-first CSS with v4 CSS variable theming | Already in place, v4 enables runtime theme switching |
| shadcn/ui | 4.0.5 | Component primitives with CSS variable theming | Already in place, components consume theme variables |
| @tanstack/react-table | 8.21.3 | Headless table for extension/roster data | Already in place |
| recharts | 2.15.4 | Charts for cap/salary visualization | Already in place |
| lucide-react | 0.577.0 | Icon set | Already in place |
| react-router-dom | 7.13.1 | Routing (splash → team → dashboard) | Already in place |

### No New Dependencies Needed

The entire redesign can be accomplished with the existing stack. The theming infrastructure is CSS-only. Team logos can be static SVG assets or fetched from MFL/ESPN CDN URLs.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Static SVG team logos | react-nfl-logos npm package | Package last updated Dec 2022, not maintained. Better to use CDN URLs or bundled SVGs |
| CSS variable theming | next-themes | Not needed — we're not using Next.js, and manual `data-theme` attribute is simpler for team switching |
| Custom team color config | Tailwind v4 @theme inline | @theme is for build-time; we need runtime switching via CSS variable overrides |
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Pattern 1: Per-Team Accent Colors via CSS Custom Properties

**What:** Define a constant dark base theme, then override only accent/highlight variables per-team using `[data-team]` CSS selectors.

**When to use:** Always — this is the core theming approach.

**How it works:**

The existing shadcn setup already has variables like `--primary`, `--accent`, `--sidebar-primary`. These are consumed by every shadcn component. By scoping overrides to a `data-team` attribute on `<html>` or `<body>`, all components automatically inherit team colors.

```css
/* globals.css - Base dark theme (constant for all teams) */
.dark {
  --background: oklch(0.145 0 0);       /* near-black */
  --foreground: oklch(0.985 0 0);       /* near-white */
  --card: oklch(0.18 0 0);              /* slightly lighter card */
  --muted: oklch(0.22 0 0);
  --border: oklch(1 0 0 / 10%);
  /* ... other dark base tokens ... */
}

/* Team accent overrides — only accent-related variables change */
[data-team="BUF"] {
  --primary: oklch(0.45 0.15 250);       /* Bills Royal Blue */
  --accent: oklch(0.55 0.20 25);         /* Bills Red accent */
  --sidebar-primary: oklch(0.45 0.15 250);
  --ring: oklch(0.45 0.15 250);
}

[data-team="KC"] {
  --primary: oklch(0.52 0.22 25);        /* Chiefs Red */
  --accent: oklch(0.75 0.15 85);         /* Chiefs Gold accent */
  --sidebar-primary: oklch(0.52 0.22 25);
  --ring: oklch(0.52 0.22 25);
}
/* ... repeat for all 32 teams ... */
```

**Runtime switching (JavaScript):**
```typescript
// When user selects a team
function setTeamTheme(teamAbbr: string) {
  document.documentElement.setAttribute('data-team', teamAbbr);
}
```

### Pattern 2: Splash Screen Team Picker

**What:** A full-screen dark landing page with a grid of all 32 team logos. Click one → navigate to dashboard with team context.

**When to use:** App entry point (replaces current dropdown selector).

**Structure:**
```
/                    → SplashPage (team picker grid)
/dashboard?team=BUF  → DashboardPage (team-branded)
/roster?team=BUF     → RosterPage
```

**Implementation approach:**
```typescript
// SplashPage.tsx
function SplashPage() {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen bg-[oklch(0.08_0_0)] flex flex-col items-center justify-center">
      <h1 className="text-4xl font-bold text-white mb-2">ADL Contract Admin</h1>
      <p className="text-muted-foreground mb-12">Select your franchise</p>
      <div className="grid grid-cols-4 md:grid-cols-8 gap-4 max-w-5xl">
        {teams.map(team => (
          <button
            key={team.id}
            onClick={() => {
              setTeamTheme(team.abbr);
              navigate(`/dashboard?team=${team.id}`);
            }}
            className="group p-4 rounded-xl hover:bg-white/10 transition-all"
          >
            <img src={team.logoUrl} alt={team.name} className="w-16 h-16" />
          </button>
        ))}
      </div>
    </div>
  );
}
```

### Pattern 3: Action-Oriented Dashboard Layout

**What:** Dashboard prioritizes deadlines and pending actions over static data.

**Layout:**
```
┌──────────────────────────────────────────┐
│  [Team Logo] [Team Name]    [Deadline ▼] │  ← Header with team branding
├──────────────────────────────────────────┤
│                                          │
│  ┌─────────────────────────────────────┐ │
│  │  ⏰ EXTENSION DEADLINE IN 3 DAYS   │ │  ← Prominent countdown
│  └─────────────────────────────────────┘ │
│                                          │
│  ┌─── Pending Actions ──┐ ┌── Cap ────┐ │
│  │ 2 players eligible   │ │ $142.3M   │ │  ← Action cards
│  │ 1 tender pending     │ │ $7.7M     │ │
│  └──────────────────────┘ │ remaining │ │
│                           └───────────┘ │
│  ┌─── Roster Summary ──────────────────┐ │
│  │ [Compact roster table]              │ │  ← Secondary content
│  └─────────────────────────────────────┘ │
└──────────────────────────────────────────┘
```

### Pattern 4: Adaptive Result Layouts

**What:** Cards for small result sets, tables for large ones.

**When to use:**
- **Cards:** Tenders (2-5 results), Tags (1-2 results), 5YO/PPE (1 result each)
- **Tables:** Extensions (10-30+ results), Roster (53 players), Cap details

```typescript
// Card layout for tender results
<div className="grid grid-cols-1 md:grid-cols-2 gap-4">
  {tenders.map(t => (
    <Card key={t.id} className="border-l-4 border-l-primary">
      <CardHeader>
        <span className="text-3xl font-bold">${t.amount.toLocaleString()}</span>
        <span className="text-muted-foreground">{t.type} Tender</span>
      </CardHeader>
      <CardContent>
        <p>{t.playerName} • {t.position}</p>
        <Collapsible>
          <CollapsibleTrigger>View calculation details</CollapsibleTrigger>
          <CollapsibleContent>...</CollapsibleContent>
        </Collapsible>
      </CardContent>
    </Card>
  ))}
</div>
```

### Anti-Patterns to Avoid
- **Full team-colored backgrounds:** Only use team colors for accents (badges, borders, highlights). The dark base must stay consistent or readability suffers.
- **Logo as favicon:** Favicons are tiny — team logos don't read at 16x16. Keep the ADL shield favicon.
- **Rebuilding shadcn components:** The CSS variable system means you never modify component internals for theming.
- **Light mode:** The Sleeper aesthetic is dark-mode-only. Adding light mode doubles the theming work for no value in this context.
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Team color definitions | Manual hex-to-oklch conversion for 32 teams | Pre-computed OKLCH values from hex references | OKLCH conversion is math-heavy; compute once, store in CSS |
| Component theming | Custom styled components with team colors | shadcn CSS variable system | Every shadcn component already consumes `--primary`, `--accent` |
| Dark mode implementation | Custom dark class toggling | shadcn's existing `.dark` class + `@custom-variant` | Already configured in the codebase |
| Countdown timers | Custom interval-based countdown | Simple date math with `Date.now()` | Countdown is just `deadline - now`, no library needed |
| Team logo hosting | Self-hosting 32 SVG/PNG files | ESPN/NFL CDN URLs or bundled lightweight SVGs | CDN is faster, no asset pipeline work |
| Data tables | Custom table components | Existing TanStack Table + shadcn DataTable | Already built and working in the app |

**Key insight:** The existing stack handles 95% of this redesign. The work is CSS theming + layout restructuring + new splash page component. No new libraries, no new patterns — just better use of what's already there.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Team Colors That Don't Work on Dark Backgrounds
**What goes wrong:** Some NFL team colors (e.g., Jets green #125740, Ravens purple #241773) are too dark to use as accent colors on dark backgrounds — they disappear.
**Why it happens:** NFL official colors are designed for white/light backgrounds (jerseys, print).
**How to avoid:** Adjust each team's accent color for dark-mode visibility. Use OKLCH lightness channel — ensure primary accent has L ≥ 0.45 for sufficient contrast against dark backgrounds. Test every team's colors on `oklch(0.145 0 0)` background.
**Warning signs:** Badges, buttons, or links that are unreadable for certain teams.

### Pitfall 2: Too Many CSS Variable Overrides Per Team
**What goes wrong:** Defining 15+ variables per team creates 480+ CSS lines (32 teams × 15 vars), making the stylesheet bloated and hard to maintain.
**Why it happens:** Over-customizing per team instead of using a minimal accent approach.
**How to avoid:** Override only 3-4 variables per team: `--primary`, `--accent`, `--sidebar-primary`, `--ring`. All other colors derive from the constant dark base. This keeps it to ~160 lines total.
**Warning signs:** Team CSS section growing beyond 200 lines.

### Pitfall 3: Splash Screen Blocking Navigation
**What goes wrong:** Users must always go through splash screen to select team, even on return visits.
**Why it happens:** No persistence of team selection.
**How to avoid:** Store last-selected team in localStorage. On return visit, auto-navigate to dashboard with stored team. Show "Switch Team" button in sidebar for changing.
**Warning signs:** Users complaining about extra clicks to reach their dashboard.

### Pitfall 4: Deadline Countdown Without Period Data
**What goes wrong:** Countdown shows wrong or no deadline because the app can't determine the current league period.
**Why it happens:** The calendar/period system already exists but the dashboard doesn't surface it prominently.
**How to avoid:** Reuse existing period detection API (`/api/calendar/current-period`). The deadline countdown is a view concern, not a data concern.
**Warning signs:** Countdown showing "No upcoming deadlines" when there should be one.

### Pitfall 5: Over-Designing the Splash Screen
**What goes wrong:** Splash screen becomes complex with animations, transitions, loading states.
**Why it happens:** Trying to make it "impressive" instead of functional.
**How to avoid:** Keep it dead simple: dark background, centered logo/title, grid of team logos. Click → navigate. No loading spinners, no animations beyond simple hover states.
**Warning signs:** Splash screen component exceeding 100 lines.
</common_pitfalls>

<code_examples>
## Code Examples

### Dynamic Team Theme Switching (Tailwind v4 + shadcn)
```css
/* Source: Tailwind v4 docs + shadcn theming docs */
/* In globals.css — team accent overrides */

/* Only override accent-related variables. Dark base stays constant. */
[data-team="ARI"] { --primary: oklch(0.52 0.18 350); --ring: oklch(0.52 0.18 350); }
[data-team="ATL"] { --primary: oklch(0.50 0.20 25);  --ring: oklch(0.50 0.20 25);  }
[data-team="BAL"] { --primary: oklch(0.45 0.18 285); --ring: oklch(0.45 0.18 285); }
[data-team="BUF"] { --primary: oklch(0.45 0.15 250); --ring: oklch(0.45 0.15 250); }
/* ... all 32 teams ... */
```

### Team Selection with Theme Application
```typescript
// Source: React Router + localStorage pattern
import { useNavigate } from 'react-router-dom';

function useTeamSelection() {
  const navigate = useNavigate();

  const selectTeam = (teamId: number, teamAbbr: string) => {
    // Apply theme
    document.documentElement.setAttribute('data-team', teamAbbr);
    document.documentElement.classList.add('dark');

    // Persist selection
    localStorage.setItem('adl-team', JSON.stringify({ id: teamId, abbr: teamAbbr }));

    // Navigate to dashboard
    navigate(`/dashboard?team=${teamId}`);
  };

  // Restore on mount
  const restoreTeam = () => {
    const saved = localStorage.getItem('adl-team');
    if (saved) {
      const { abbr } = JSON.parse(saved);
      document.documentElement.setAttribute('data-team', abbr);
      document.documentElement.classList.add('dark');
    }
  };

  return { selectTeam, restoreTeam };
}
```

### Deadline Countdown Component
```typescript
// Source: Standard React pattern
function DeadlineCountdown({ deadline }: { deadline: Date }) {
  const now = new Date();
  const diff = deadline.getTime() - now.getTime();
  const days = Math.floor(diff / (1000 * 60 * 60 * 24));
  const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));

  if (diff <= 0) return <Badge variant="destructive">Deadline passed</Badge>;

  return (
    <div className="rounded-xl bg-primary/10 border border-primary/20 p-6 text-center">
      <p className="text-sm text-muted-foreground uppercase tracking-wide">
        Extension Deadline
      </p>
      <p className="text-5xl font-bold text-primary mt-2">
        {days}d {hours}h
      </p>
      <p className="text-sm text-muted-foreground mt-1">remaining</p>
    </div>
  );
}
```

### MFL Player Photo URLs
```typescript
// Source: MFL API documentation / forum posts
// Player headshots available via MFL CDN
function getPlayerPhotoUrl(mflId: string): string {
  // MFL hosts player photos at this URL pattern
  // The year in the path should be the current MFL league year
  return `https://a.]espncdn.com/combiner/i?img=/i/headshots/nfl/players/full/${mflId}.png&w=96&h=70&cb=1`;
  // Alternative: MFL direct (lower quality)
  // return `https://www.myfantasyleague.com/fflnetdynamic2025/playerimages/${mflId}.jpg`;
}
```
*Note: Player photo availability and URL patterns should be validated during implementation. ESPN CDN may require the ESPN player ID, not MFL ID. MFL's own image hosting is more reliable for MFL player IDs.*
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Tailwind v3 `tailwind.config.js` theming | Tailwind v4 `@theme inline` + CSS custom properties | 2025 | Runtime theme switching without rebuilds |
| `rgb(var(--color) / alpha)` pattern | OKLCH color space with native CSS | 2025 | Better color interpolation, simpler syntax |
| next-themes for dark mode | CSS `@custom-variant dark` + manual class | 2025 | No library needed, simpler |
| Component-level color props | CSS variable cascade | 2024+ | One `data-team` attribute themes entire app |

**New tools/patterns to consider:**
- **OKLCH color space:** Perceptually uniform — adjusting lightness channel gives predictable results across all hues. Perfect for ensuring team colors work on dark backgrounds.
- **Container queries:** Available in all modern browsers. Can make cards responsive to their container width, not viewport width.
- **tweakcn.com:** Theme editor for shadcn/ui — useful for previewing dark theme color combinations before implementing.

**Deprecated/outdated:**
- **HSL for theming:** shadcn migrated from HSL to OKLCH in v4. Don't convert back.
- **react-nfl-logos:** Last updated Dec 2022, not maintained.
</sota_updates>

<open_questions>
## Open Questions

1. **MFL-to-NFL team mapping**
   - What we know: ADL has 32 franchises with franchise_ids (e.g., '0001'). Each maps to an NFL team.
   - What's unclear: The exact mapping of franchise_id → NFL team abbreviation. Need to check MFL API or existing league data.
   - Recommendation: Query MFL API during implementation to get franchise names/abbreviations, or hardcode the known mapping.

2. **Player headshot availability**
   - What we know: MFL has player photos at predictable URLs. ESPN CDN also has headshots.
   - What's unclear: Whether MFL player IDs work with ESPN CDN (different ID systems). Quality and coverage of MFL's own photos.
   - Recommendation: Test both sources during implementation. Player photos are "nice to have" per CONTEXT.md — don't block the redesign for them.

3. **Team logo assets**
   - What we know: NFL logos are trademarked. Free SVG sources exist but licensing is unclear for web apps.
   - What's unclear: Whether using NFL logos in a private fantasy league admin tool is acceptable.
   - Recommendation: This is a private tool for 32 GMs in one league, not a public app. Use simple team abbreviation badges on the splash screen with team colors as a safe fallback. If logo assets are available from MFL, use those.

4. **OKLCH values for 32 teams**
   - What we know: NFL hex values are documented. OKLCH conversion is straightforward math.
   - What's unclear: Which teams' colors need lightness adjustment for dark backgrounds.
   - Recommendation: Convert all 32 team primary colors to OKLCH, then audit lightness values. Any team with L < 0.45 needs adjustment upward. Pre-compute during planning.
</open_questions>

<nfl_team_colors>
## NFL Team Color Reference

All 32 teams — primary and secondary hex values for OKLCH conversion:

| Team | Abbr | Primary Hex | Secondary Hex |
|------|------|-------------|---------------|
| Arizona Cardinals | ARI | #97233F | #000000 |
| Atlanta Falcons | ATL | #A71930 | #000000 |
| Baltimore Ravens | BAL | #241773 | #000000 |
| Buffalo Bills | BUF | #00338D | #C60C30 |
| Carolina Panthers | CAR | #0085CA | #101820 |
| Chicago Bears | CHI | #0B162A | #C83803 |
| Cincinnati Bengals | CIN | #FB4F14 | #000000 |
| Cleveland Browns | CLE | #FF3C00 | #311D00 |
| Dallas Cowboys | DAL | #003594 | #869397 |
| Denver Broncos | DEN | #FB4F14 | #002244 |
| Detroit Lions | DET | #0076B6 | #B0B7BC |
| Green Bay Packers | GB | #203731 | #FFB612 |
| Houston Texans | HOU | #03202F | #A71930 |
| Indianapolis Colts | IND | #002C5F | #FFFFFF |
| Jacksonville Jaguars | JAX | #006778 | #D7A22A |
| Kansas City Chiefs | KC | #E31837 | #FFB81C |
| Las Vegas Raiders | LV | #A5ACAF | #000000 |
| Los Angeles Chargers | LAC | #0080C6 | #FFC20E |
| Los Angeles Rams | LAR | #003594 | #FFA300 |
| Miami Dolphins | MIA | #008E97 | #FC4C02 |
| Minnesota Vikings | MIN | #4F2683 | #FFC62F |
| New England Patriots | NE | #002244 | #C60C30 |
| New Orleans Saints | NO | #D3BC8D | #101820 |
| New York Giants | NYG | #0B2265 | #A71930 |
| New York Jets | NYJ | #125740 | #FFFFFF |
| Philadelphia Eagles | PHI | #004C54 | #A5ACAF |
| Pittsburgh Steelers | PIT | #FFB612 | #101820 |
| San Francisco 49ers | SF | #AA0000 | #B3995D |
| Seattle Seahawks | SEA | #002244 | #69BE28 |
| Tampa Bay Buccaneers | TB | #D50A0A | #34302B |
| Tennessee Titans | TEN | #0C2340 | #4B92DB |
| Washington Commanders | WAS | #5A1414 | #FFB612 |

**Dark-mode concern teams** (primary hex too dark for dark backgrounds):
- BAL #241773, CHI #0B162A, HOU #03202F, IND #002C5F, NE #002244, NYG #0B2265
- NYJ #125740, PHI #004C54, SEA #002244, TEN #0C2340, WAS #5A1414
- GB #203731, BUF #00338D, DAL #003594, LAR #003594, CLE #311D00

→ These teams need lightened primary colors (boost OKLCH lightness to ≥0.45) or should use their secondary/accent color as primary accent instead.
</nfl_team_colors>

<sleeper_design_reference>
## Sleeper Design Reference

Key design elements to emulate:

**Color System:**
- Deep navy/black backgrounds: `oklch(0.08 0 0)` to `oklch(0.15 0 0)`
- Layered card surfaces: `oklch(0.18 0 0)` to `oklch(0.22 0 0)`
- Bright accent colors for highlights (cyan, orange, green)
- Semi-transparent borders: `oklch(1 0 0 / 10%)`

**Typography:**
- Bold, large numbers for key stats (3xl-5xl)
- Small uppercase labels for categories
- High contrast: near-white text on near-black backgrounds
- Tabular numerals for aligned data columns

**Layout Patterns:**
- Cards with rounded corners (xl radius)
- Subtle backdrop blur on overlays
- Left-colored borders on cards to indicate category/team
- Generous padding and spacing
- Grid layouts with `gap-4` to `gap-6`

**Navigation:**
- Clean sidebar with icon + text
- Active state uses accent color highlight
- Minimal — 5-7 nav items max
- Team branding at top of sidebar

**Data Presentation:**
- Primary number BIG and bold, detail text small and muted
- Expandable/collapsible detail sections
- Color-coded status indicators (green=good, red=warning, yellow=attention)
- Skeleton loading states for perceived performance
</sleeper_design_reference>

<sources>
## Sources

### Primary (HIGH confidence)
- shadcn/ui theming docs (Context7) — CSS variable theming pattern, OKLCH color system, `@theme inline` directive
- Tailwind CSS v4 theme variables docs — `@theme` directive, CSS custom property approach
- [simonswiss.com — Tailwind v4 Multi-Theme Strategy](https://simonswiss.com/posts/tailwind-v4-multi-theme) — `data-theme` attribute pattern, runtime switching
- [teampalettes.com — NFL Team Colors](https://teampalettes.com/nfl) — All 32 team hex codes verified

### Secondary (MEDIUM confidence)
- [Sleeper app homepage](https://sleeper.com) — Design system analysis (visual inspection of live site)
- [Sleeper app review](https://sleeperdynasty.com/blog/sleeper-app-fantasy-football-review-2025) — UX philosophy and design principles
- MFL API documentation — Player photo URL patterns (forum-sourced, needs implementation validation)

### Tertiary (LOW confidence - needs validation)
- MFL player photo URLs — Format `https://www.myfantasyleague.com/fflnetdynamic2025/playerimages/{id}.jpg` needs testing
- react-nfl-logos npm package — Exists but unmaintained (Dec 2022), not recommended
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: Tailwind CSS v4 + shadcn/ui CSS variable theming
- Ecosystem: No new libraries needed — existing stack sufficient
- Patterns: Dynamic team theming, splash screen, action-oriented dashboard, adaptive layouts
- Pitfalls: Dark-mode color contrast, CSS bloat, team selection persistence

**Confidence breakdown:**
- Standard stack: HIGH — no changes needed, verified against existing codebase
- Architecture: HIGH — CSS variable theming pattern verified via Tailwind v4 docs and shadcn docs
- Pitfalls: HIGH — color contrast issues are well-documented in dark-mode design
- Code examples: HIGH — patterns from official docs and established community approaches
- NFL team colors: HIGH — verified from teampalettes.com (consistent with other sources)
- Player photos: MEDIUM — URL patterns need validation during implementation

**Research date:** 2026-04-04
**Valid until:** 2026-05-04 (30 days — Tailwind v4 and shadcn are stable)
</metadata>

---

*Phase: 25-ux-audit-redesign*
*Research completed: 2026-04-04*
*Ready for planning: yes*
