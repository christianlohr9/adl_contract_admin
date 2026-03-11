# Phase 8: Frontend UI - Research

**Researched:** 2026-03-11
**Domain:** React 19 data-driven dashboard with contract tools, data tables, charts, and search
**Confidence:** HIGH

<research_summary>
## Summary

Researched the React ecosystem for building a functional frontend UI with data tables, charts, search, and contract tool comparison views — on top of the existing React 19 + Vite 7 + shadcn/ui v4 + Tailwind v4 scaffold.

The standard approach uses TanStack Query v5 for data fetching/caching, TanStack Table v8 for headless data tables (with shadcn/ui's official data table guide), shadcn/ui's built-in Chart component (Recharts under the hood) for salary cap visualizations, and the shadcn/ui Command component (cmdk) for player search. Local persistence (flags, notes, bookmarks) is handled by Zustand with persist middleware to localStorage.

Key finding: shadcn/ui already provides official integrations for data tables (TanStack Table), charts (Recharts), and search (cmdk Command). The entire UI can be built within the shadcn/ui ecosystem without introducing competing component libraries.

**Primary recommendation:** Stay within the shadcn/ui ecosystem. Use TanStack Query for all API data, TanStack Table + shadcn Table for data grids, shadcn Chart for visualizations, shadcn Command for search, and Zustand + persist for local state (flags/notes/bookmarks). No need for AG Grid, separate chart libraries, or external search components.
</research_summary>

<standard_stack>
## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| @tanstack/react-query | 5.x | Server state / data fetching | De facto standard for React API data; caching, dedup, background refetch, error states |
| @tanstack/react-table | 8.x | Headless data table | shadcn/ui's official data table guide uses it; sorting, filtering, pagination built-in |
| recharts | 2.x (shadcn bundled) | Charts / visualization | shadcn/ui Chart component uses Recharts; theming auto-integrates with shadcn |
| zustand | 5.x | Client state / local persistence | Tiny (~1KB), persist middleware for localStorage; ideal for flags/notes/bookmarks |
| nuqs | 2.x | URL state management | Type-safe query params; shareable filter/sort state in URL; ~5.5KB |

### Supporting (already installed)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| react-router-dom | 7.x | Routing | Already in scaffold |
| shadcn/ui | 4.x | Component library | Already in scaffold — Card, Tabs, Button, Input, etc. |
| lucide-react | 0.577.x | Icons | Already in scaffold |
| clsx + tailwind-merge | latest | Class merging | Already in scaffold via cn() utility |

### New shadcn/ui Components to Add
| Component | Purpose | Install Command |
|-----------|---------|-----------------|
| chart | Salary cap charts, cap space visualization | `pnpm dlx shadcn@latest add chart` |
| command | Player search (cmdk-based) | `pnpm dlx shadcn@latest add command` |
| dialog | Search overlay, scenario modals | `pnpm dlx shadcn@latest add dialog` |
| table | Data table base (for TanStack Table) | `pnpm dlx shadcn@latest add table` |
| badge | Contract type labels, status indicators | `pnpm dlx shadcn@latest add badge` |
| select | Filter dropdowns (position, team) | `pnpm dlx shadcn@latest add select` |
| popover | Combobox containers, filter popovers | `pnpm dlx shadcn@latest add popover` |
| dropdown-menu | Row actions, sort options | `pnpm dlx shadcn@latest add dropdown-menu` |
| progress | Cap usage bars | `pnpm dlx shadcn@latest add progress` |
| alert | Warnings for cap violations | `pnpm dlx shadcn@latest add alert` |
| scroll-area | Long lists, scrollable comparison views | `pnpm dlx shadcn@latest add scroll-area` |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| TanStack Table | AG Grid | AG Grid is batteries-included but heavy (~300KB+), enterprise features cost $999/license; overkill for ~500 player dataset |
| Recharts (via shadcn) | Nivo, visx | Nivo heavier; visx lower-level; Recharts already bundled with shadcn theming |
| Zustand | Jotai, Redux | Redux overkill for local flags/notes; Jotai works but Zustand persist middleware is simpler |
| nuqs | Manual URLSearchParams | Manual approach error-prone; nuqs provides type-safe parsers and debouncing |
| cmdk (via shadcn) | Algolia, Fuse.js | Client-side search over ~500 players doesn't need Algolia; cmdk handles fuzzy matching |

**Installation:**
```bash
cd frontend
pnpm add @tanstack/react-query @tanstack/react-table zustand nuqs
pnpm dlx shadcn@latest add chart command dialog table badge select popover dropdown-menu progress alert scroll-area
```
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Project Structure
```
frontend/src/
├── api/                    # API client + query hooks
│   ├── client.ts           # fetch wrapper (base URL, error handling)
│   ├── queries/
│   │   ├── players.ts      # usePlayer, usePlayers, usePlayerSearch
│   │   ├── teams.ts        # useTeams, useTeamRoster
│   │   ├── contracts.ts    # usePlayerTools (bundled endpoint)
│   │   └── cap.ts          # useSalaryCap, useCapSnapshot
│   └── types.ts            # API response types
├── components/
│   ├── ui/                 # shadcn/ui components (existing)
│   ├── layout/             # AppLayout, AppSidebar (existing)
│   ├── data-table/         # Reusable TanStack Table wrapper
│   │   ├── DataTable.tsx   # Generic table component
│   │   ├── columns/        # Column definitions per view
│   │   ├── DataTablePagination.tsx
│   │   └── DataTableToolbar.tsx
│   ├── player/             # Player-specific components
│   │   ├── PlayerSearch.tsx # Command palette search
│   │   ├── ContractComparison.tsx  # Side-by-side tool view
│   │   └── PlayerHeader.tsx
│   ├── cap/                # Salary cap components
│   │   ├── CapChart.tsx    # Recharts visualization
│   │   ├── CapScenario.tsx # What-if modeling
│   │   └── CapSummary.tsx
│   └── dashboard/          # Dashboard widgets
│       ├── ExpiringContracts.tsx
│       ├── PendingDecisions.tsx
│       └── CapOverview.tsx
├── stores/                 # Zustand stores
│   └── userStore.ts        # Flags, notes, bookmarks (persisted)
├── hooks/                  # Custom hooks
│   └── use-mobile.ts       # (existing)
├── lib/
│   ├── utils.ts            # (existing cn() utility)
│   └── format.ts           # Currency, percentage formatters
├── pages/                  # Route pages (existing)
│   ├── DashboardPage.tsx
│   ├── RosterPage.tsx
│   ├── PlayerDetailPage.tsx
│   └── SalaryCapPage.tsx
└── App.tsx                 # Routes (existing)
```

### Pattern 1: TanStack Query for All API Data
**What:** Declare data needs with useQuery hooks; never use useEffect + useState for fetching
**When to use:** Every API call
**Example:**
```typescript
// api/queries/players.ts
import { useQuery } from '@tanstack/react-query'
import { api } from '../client'

export function usePlayerTools(playerId: number) {
  return useQuery({
    queryKey: ['player', playerId, 'tools'],
    queryFn: () => api.get(`/players/${playerId}/all`),
    staleTime: 5 * 60 * 1000, // 5 min — contract data doesn't change fast
  })
}

export function useTeamRoster(teamId: number) {
  return useQuery({
    queryKey: ['team', teamId, 'roster'],
    queryFn: () => api.get(`/teams/${teamId}/roster`),
    staleTime: 5 * 60 * 1000,
  })
}
```

### Pattern 2: TanStack Table with shadcn/ui Table Components
**What:** Headless table logic (useReactTable) + shadcn Table for rendering
**When to use:** Roster tables, contract lists, cap ledgers
**Example:**
```typescript
// components/data-table/DataTable.tsx
import { flexRender, useReactTable, getCoreRowModel,
  getSortedRowModel, getFilteredRowModel, getPaginationRowModel
} from '@tanstack/react-table'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

export function DataTable<TData>({ columns, data }) {
  const table = useReactTable({
    data, columns,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
  })

  return (
    <Table>
      <TableHeader>
        {table.getHeaderGroups().map(headerGroup => (
          <TableRow key={headerGroup.id}>
            {headerGroup.headers.map(header => (
              <TableHead key={header.id}>
                {flexRender(header.column.columnDef.header, header.getContext())}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.map(row => (
          <TableRow key={row.id}>
            {row.getVisibleCells().map(cell => (
              <TableCell key={cell.id}>
                {flexRender(cell.column.columnDef.cell, cell.getContext())}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}
```

### Pattern 3: shadcn/ui Chart for Salary Cap Visualization
**What:** Use shadcn Chart component (Recharts) with ChartConfig for theming
**When to use:** Cap breakdown, scenario comparisons
**Example:**
```typescript
// components/cap/CapChart.tsx
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { ChartContainer, ChartTooltip, ChartTooltipContent } from '@/components/ui/chart'

const chartConfig = {
  committed: { label: "Committed", color: "hsl(var(--chart-1))" },
  available: { label: "Available", color: "hsl(var(--chart-2))" },
}

export function CapChart({ data }) {
  return (
    <ChartContainer config={chartConfig}>
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="category" />
        <YAxis />
        <ChartTooltip content={<ChartTooltipContent />} />
        <Bar dataKey="committed" stackId="a" fill="var(--color-committed)" />
        <Bar dataKey="available" stackId="a" fill="var(--color-available)" />
      </BarChart>
    </ChartContainer>
  )
}
```

### Pattern 4: Zustand with Persist for Local User State
**What:** Client-side state for flags, notes, bookmarks persisted to localStorage
**When to use:** Any user-specific data that doesn't go to the server
**Example:**
```typescript
// stores/userStore.ts
import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface UserStore {
  flaggedPlayers: Set<number>
  notes: Record<number, string>  // playerId -> note
  toggleFlag: (playerId: number) => void
  setNote: (playerId: number, note: string) => void
}

export const useUserStore = create<UserStore>()(
  persist(
    (set) => ({
      flaggedPlayers: new Set(),
      notes: {},
      toggleFlag: (playerId) => set((state) => {
        const next = new Set(state.flaggedPlayers)
        next.has(playerId) ? next.delete(playerId) : next.add(playerId)
        return { flaggedPlayers: next }
      }),
      setNote: (playerId, note) => set((state) => ({
        notes: { ...state.notes, [playerId]: note }
      })),
    }),
    {
      name: 'adl-user-prefs',
      // Sets don't serialize natively — use partialize/storage transform
    }
  )
)
```

### Pattern 5: Command Palette Search
**What:** Always-visible search bar using shadcn Command (cmdk) for player lookup
**When to use:** Global player search in the header
**Example:**
```typescript
// components/player/PlayerSearch.tsx
import { CommandDialog, CommandInput, CommandList, CommandItem, CommandEmpty } from '@/components/ui/command'

export function PlayerSearch() {
  const [open, setOpen] = useState(false)
  const { data: players } = usePlayers()

  return (
    <>
      <Button variant="outline" onClick={() => setOpen(true)}>
        Search players...
      </Button>
      <CommandDialog open={open} onOpenChange={setOpen}>
        <CommandInput placeholder="Search players..." />
        <CommandList>
          <CommandEmpty>No players found.</CommandEmpty>
          {players?.map(p => (
            <CommandItem key={p.id} onSelect={() => navigate(`/roster/${p.id}`)}>
              {p.name} — {p.position} — {p.team}
            </CommandItem>
          ))}
        </CommandList>
      </CommandDialog>
    </>
  )
}
```

### Anti-Patterns to Avoid
- **useEffect + useState for API data:** Use TanStack Query instead — handles caching, loading, errors, refetching automatically
- **Prop drilling API data:** Use query hooks directly in the components that need the data; TanStack Query deduplicates
- **Building custom table components from scratch:** Use TanStack Table headless logic + shadcn Table for rendering
- **Custom chart abstraction layer:** Use Recharts components directly inside shadcn ChartContainer; don't wrap further
- **Redux for local preferences:** Zustand persist is simpler for flags/notes/bookmarks that don't need server sync
- **Building custom search:** shadcn Command (cmdk) handles fuzzy matching, keyboard nav, and large lists
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Data fetching + caching | Custom useEffect + useState | TanStack Query useQuery | Handles caching, dedup, retry, background refetch, loading/error states |
| Table sorting/filtering | Custom sort functions + state | TanStack Table | Handles multi-sort, column filters, pagination, row selection; headless so you keep full UI control |
| Chart components | Custom SVG drawing | shadcn Chart (Recharts) | Responsive, interactive, themed to match shadcn; tooltip, legend, animation built-in |
| Search with keyboard nav | Custom input + dropdown | shadcn Command (cmdk) | Fuzzy matching, keyboard navigation, accessible, handles large lists performantly |
| URL state sync | Manual URLSearchParams | nuqs | Type-safe parsers, debouncing, history management, framework-agnostic |
| localStorage persistence | Manual JSON.parse/stringify | Zustand persist middleware | Handles serialization, versioning, migration, partial state, rehydration |
| Currency formatting | Manual toFixed() / regex | Intl.NumberFormat | Handles locale, currency symbols, edge cases; no custom code needed |
| Loading skeletons | Custom pulse animations | shadcn Skeleton component | Already styled to match theme; consistent look |

**Key insight:** The shadcn/ui ecosystem already provides official guides/components for data tables (TanStack Table), charts (Recharts), and search (cmdk). Building outside this ecosystem means losing theme integration and fighting component styling. Stay in the ecosystem.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Waterfall API Requests
**What goes wrong:** Pages load slowly because requests happen sequentially (fetch team → then fetch roster → then fetch cap)
**Why it happens:** Nested useQuery calls that depend on previous results
**How to avoid:** Use parallel queries where possible; prefetch on hover/navigation with queryClient.prefetchQuery
**Warning signs:** Loading spinners appearing one after another; slow page transitions

### Pitfall 2: Stale Closures in Table Column Definitions
**What goes wrong:** Table column actions (buttons, links) reference stale state
**Why it happens:** Column definitions memoized with useMemo but callbacks capture old state
**How to avoid:** Use accessorFn and cell render functions that read from row data, not closed-over state; keep column defs pure
**Warning signs:** Click handlers showing wrong data; actions targeting wrong rows

### Pitfall 3: TanStack Table Re-renders on Every State Change
**What goes wrong:** Entire table re-renders when sort/filter changes, causing lag
**Why it happens:** Large table data passed as prop without memoization
**How to avoid:** Memoize data with useMemo; keep column definitions stable with useMemo; avoid inline function definitions in column defs
**Warning signs:** Typing in filter input feels sluggish; sort click has visible delay

### Pitfall 4: shadcn Chart Not Responding to Dark Mode
**What goes wrong:** Charts look wrong or invisible in dark mode
**Why it happens:** Using hardcoded colors instead of CSS variables from ChartConfig
**How to avoid:** Always use `var(--color-{key})` pattern with ChartConfig; let shadcn handle theme switching
**Warning signs:** Charts look fine in light mode but broken in dark mode

### Pitfall 5: Zustand Persist Hydration Mismatch
**What goes wrong:** Initial render shows default state, then "jumps" to persisted state
**Why it happens:** localStorage is synchronous but React may render before hydration completes
**How to avoid:** Use Zustand's onRehydrateStorage callback; show skeleton during hydration if needed; Set objects don't serialize natively — use Array transform
**Warning signs:** Flash of empty bookmarks/notes on page load; Set values lost after refresh

### Pitfall 6: Over-fetching with Bundled Endpoint
**What goes wrong:** Every player list item triggers the heavy /all endpoint
**Why it happens:** Using the bundled endpoint for list views instead of just detail pages
**How to avoid:** Use lightweight list endpoints for roster/search; reserve /all for PlayerDetailPage only
**Warning signs:** Slow roster page loads; excessive network traffic on table views
</common_pitfalls>

<code_examples>
## Code Examples

Verified patterns from official sources:

### TanStack Query Provider Setup
```typescript
// main.tsx — wrap app with QueryClientProvider
// Source: TanStack Query quick-start docs
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,  // 5 min default for contract data
      retry: 1,
    },
  },
})

// In render:
<QueryClientProvider client={queryClient}>
  <BrowserRouter>
    <App />
  </BrowserRouter>
</QueryClientProvider>
```

### API Client with Typed Fetch
```typescript
// api/client.ts
const BASE_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

async function fetchApi<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`)
  if (!res.ok) throw new Error(`API error: ${res.status}`)
  return res.json()
}

export const api = {
  get: <T>(path: string) => fetchApi<T>(path),
}
```

### Salary Formatting Utility
```typescript
// lib/format.ts
const usd = new Intl.NumberFormat('en-US', {
  style: 'currency', currency: 'USD',
  minimumFractionDigits: 0, maximumFractionDigits: 0,
})

// Salaries stored in millions (0.01 = $10k)
export function formatSalary(millions: number): string {
  return usd.format(millions * 1_000_000)
}

export function formatCapPercent(pct: number): string {
  return `${(pct * 100).toFixed(1)}%`
}
```

### Contract Comparison View (Side-by-Side Cards)
```typescript
// components/player/ContractComparison.tsx — key Phase 8 feature
// Uses shadcn Card + Tabs for the comparison layout
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

// Data comes from the bundled /players/{id}/all endpoint
export function ContractComparison({ tools }) {
  const sections = [
    { key: 'extensions', title: 'Extensions', data: tools.extensions },
    { key: 'tags', title: 'Franchise/Transition Tags', data: tools.tags },
    { key: 'tenders', title: 'RFA/ERFA Tenders', data: tools.tenders },
    { key: 'buyout', title: 'Buyout', data: tools.buyout },
  ].filter(s => s.data) // Only show applicable tools

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {sections.map(section => (
        <Card key={section.key}>
          <CardHeader>
            <CardTitle>{section.title}</CardTitle>
          </CardHeader>
          <CardContent>
            {/* Render section-specific details */}
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
```

### nuqs for Shareable Table Filters
```typescript
// Use in RosterPage to persist filter/sort in URL
import { useQueryState, parseAsString, parseAsInteger } from 'nuqs'

export function useRosterFilters() {
  const [position, setPosition] = useQueryState('pos', parseAsString)
  const [team, setTeam] = useQueryState('team', parseAsInteger)
  const [sort, setSort] = useQueryState('sort', parseAsString.withDefault('name'))

  return { position, setPosition, team, setTeam, sort, setSort }
}
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| useEffect + useState for fetching | TanStack Query v5 | 2023+ | Eliminates boilerplate; automatic caching, dedup, retry |
| react-table v7 (hooks) | TanStack Table v8 (headless) | 2022+ | Framework-agnostic core; TypeScript-first; better API |
| Custom chart wrappers | shadcn/ui Chart (Recharts) | 2024+ | Theme-integrated charts; no custom styling needed |
| cmdk standalone | shadcn/ui Command (cmdk bundled) | 2024+ | Pre-styled, theme-integrated command palette |
| Manual URLSearchParams | nuqs | 2024+ | Type-safe URL state; used by Vercel, Sentry, Supabase |
| Redux Toolkit for everything | Zustand for client + TanStack Query for server | 2023+ | Separation of concerns; less boilerplate; smaller bundle |
| Recharts v2 | Recharts v3 (coming) | In progress | shadcn/ui tracking v3 upgrade; v2 works fine now |

**New tools/patterns to consider:**
- **shadcn/ui v4 with Base UI:** Current scaffold already uses this; components use render props instead of Radix asChild
- **nuqs:** Type-safe URL state becoming standard for shareable views; tiny bundle (~5.5KB)
- **TanStack Query + prefetchQuery:** Prefetch on hover/navigation for instant page transitions

**Deprecated/outdated:**
- **react-table v7:** Replaced by TanStack Table v8 with different API
- **SWR for complex apps:** TanStack Query has richer mutation/invalidation story
- **Chart.js with React:** Recharts is more React-native; Chart.js requires wrapper
- **Redux for local UI state:** Overkill; Zustand or React state sufficient
</sota_updates>

<open_questions>
## Open Questions

1. **Recharts v2 vs v3 timing**
   - What we know: shadcn/ui currently uses Recharts v2; v3 upgrade is on their roadmap
   - What's unclear: When v3 will officially land in shadcn/ui
   - Recommendation: Use v2 now (what shadcn installs); v3 migration will be handled by shadcn CLI when ready

2. **Scenario modeling complexity**
   - What we know: User wants "what if I extend this player?" cap preview
   - What's unclear: Whether this requires a server-side endpoint or can be computed client-side from existing data
   - Recommendation: Start with client-side computation using existing contract tool results + cap data; add server endpoint only if computation is too complex for the client

3. **Light actions persistence scope**
   - What we know: Flags, notes, bookmarks should persist but don't need server storage
   - What's unclear: Whether GMs want these shared across devices or local-only is fine
   - Recommendation: Start with localStorage (Zustand persist); can upgrade to server-persisted later if needed
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- /tanstack/query — Quick start, useQuery, useMutation, QueryClientProvider setup
- /websites/tanstack_table — Column definitions, sorting, filtering, pagination patterns
- /recharts/recharts — BarChart, ComposedChart, ResponsiveContainer, React 16.8-19 support confirmed
- shadcn/ui official docs — Chart component (Recharts), Command (cmdk), Data Table guide (TanStack Table), Combobox

### Secondary (MEDIUM confidence)
- WebSearch: TanStack Table vs AG Grid comparison — verified TanStack Table is free, headless, ~30KB vs AG Grid enterprise pricing
- WebSearch: shadcn/ui charts use Recharts, v3 upgrade in progress — verified via official GitHub issue #7669
- WebSearch: nuqs by 47ng — type-safe URL state, used by Vercel/Sentry/Supabase — verified via official site and InfoQ coverage
- WebSearch: Zustand persist middleware — verified via official Zustand docs at zustand.docs.pmnd.rs

### Tertiary (LOW confidence - needs validation)
- Scenario modeling UI patterns — general dashboard best practices found; no sports-specific salary cap UI patterns in search results; will need to design based on domain knowledge
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: React 19 + Vite 7 + shadcn/ui v4 (existing scaffold)
- Ecosystem: TanStack Query, TanStack Table, Recharts, Zustand, nuqs, cmdk
- Patterns: Data fetching, headless tables, chart theming, command search, URL state, local persistence
- Pitfalls: Waterfall requests, table re-renders, chart theming, hydration mismatch, over-fetching

**Confidence breakdown:**
- Standard stack: HIGH — all libraries verified via Context7 + official docs; shadcn/ui officially integrates TanStack Table, Recharts, cmdk
- Architecture: HIGH — patterns from official TanStack/shadcn documentation and examples
- Pitfalls: HIGH — documented in official docs and confirmed via community patterns
- Code examples: HIGH — from Context7 and official quick-start guides

**Research date:** 2026-03-11
**Valid until:** 2026-04-11 (30 days — ecosystem stable; shadcn/ui v4 settled)
</metadata>

---

*Phase: 08-frontend-ui*
*Research completed: 2026-03-11*
*Ready for planning: yes*
