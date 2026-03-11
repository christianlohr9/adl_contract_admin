# Phase 7: Frontend Placeholder - Research

**Researched:** 2026-03-11
**Domain:** React/TypeScript SPA scaffold with sidebar navigation and tabbed views
**Confidence:** HIGH

<research_summary>
## Summary

Researched the modern React frontend ecosystem for building a GM-centric dashboard SPA. The standard approach uses Vite as the build tool, React 19 with TypeScript, Tailwind CSS v4 for styling, shadcn/ui for components, and React Router v7 in library mode for SPA routing.

Key finding: shadcn/ui has dedicated Sidebar and Tabs components that directly match the CONTEXT.md requirements (sidebar navigation, tabbed player detail page). These are production-quality, accessible components built on Radix UI primitives — no need to build custom sidebar or tab patterns.

**Primary recommendation:** Use Vite + React 19 + TypeScript + Tailwind v4 + shadcn/ui + React Router v7 (library mode). Start with shadcn/ui's Sidebar component for navigation shell, Tabs for player detail, and React Router nested routes with Outlet for the layout pattern.
</research_summary>

<standard_stack>
## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| react | 19.x | UI framework | Current stable, required for shadcn/ui v4 components |
| react-dom | 19.x | DOM rendering | Pairs with React 19 |
| typescript | 5.x | Type safety | Standard for React projects |
| vite | 6.x | Build tool / dev server | Fast HMR, first-class TS support, standard scaffold tool |
| tailwindcss | 4.x | Utility-first CSS | CSS-first config, 2-5x faster builds via Oxide engine |
| react-router-dom | 7.x | Client-side routing | Nested routes with Outlet, mature SPA patterns |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| shadcn/ui | latest (CLI) | Component library | Sidebar, Tabs, NavigationMenu, Button, Card, Table — all needed |
| lucide-react | latest | Icons | Ships with shadcn/ui, used for sidebar menu icons |
| @tailwindcss/vite | 4.x | Vite integration | Tailwind v4 Vite plugin (replaces PostCSS plugin) |
| clsx + tailwind-merge | latest | Class merging | Used by shadcn/ui's `cn()` utility |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| React Router v7 (library) | TanStack Router | Better type safety but +25KB bundle, more complex setup, overkill for this app's routing needs |
| React Router v7 (library) | React Router v7 (framework) | Framework mode adds SSR/file-routing features not needed for SPA placeholder |
| shadcn/ui | Ant Design / MUI | shadcn/ui is lighter, Tailwind-native, copy-paste ownership; MUI/Ant are heavier with opinionated styling |
| Tailwind v4 | Tailwind v3 | v4 is current, shadcn/ui supports it, CSS-first config is simpler; no reason to start new project on v3 |

**Installation:**
```bash
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install react-router-dom
npm install -D tailwindcss @tailwindcss/vite
npx shadcn@latest init
npx shadcn@latest add sidebar tabs navigation-menu button card
```
</standard_stack>

<architecture_patterns>
## Architecture Patterns

### Recommended Project Structure
```
frontend/
├── src/
│   ├── components/
│   │   ├── ui/              # shadcn/ui components (auto-generated)
│   │   ├── layout/          # AppSidebar, AppLayout
│   │   └── placeholders/    # Placeholder page content components
│   ├── pages/               # Route-level page components
│   │   ├── DashboardPage.tsx
│   │   ├── RosterPage.tsx
│   │   ├── PlayerDetailPage.tsx
│   │   ├── ContractToolsPage.tsx  # (or tabs within PlayerDetail)
│   │   └── SalaryCapPage.tsx
│   ├── routes.tsx            # Route definitions
│   ├── App.tsx               # Root layout with SidebarProvider + Router
│   ├── main.tsx              # Entry point
│   └── index.css             # Tailwind v4 imports + theme
├── components.json           # shadcn/ui config
├── tsconfig.json
├── vite.config.ts
└── package.json
```

### Pattern 1: Sidebar Layout with React Router Outlet
**What:** Wrap all routes in a layout component that renders shadcn/ui Sidebar + Outlet
**When to use:** Every page that needs the sidebar shell
**Example:**
```tsx
// Source: React Router docs + shadcn/ui Sidebar docs
import { Outlet } from "react-router-dom"
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/layout/AppSidebar"

export function AppLayout() {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset>
        <main className="p-6">
          <Outlet />
        </main>
      </SidebarInset>
    </SidebarProvider>
  )
}
```

### Pattern 2: Sidebar Navigation with Menu Items
**What:** Use SidebarMenu with SidebarMenuButton for navigation links
**When to use:** Main sidebar with route links
**Example:**
```tsx
// Source: shadcn/ui sidebar docs
import { Link, useLocation } from "react-router-dom"
import {
  Sidebar, SidebarContent, SidebarGroup, SidebarGroupLabel,
  SidebarGroupContent, SidebarMenu, SidebarMenuItem, SidebarMenuButton,
} from "@/components/ui/sidebar"
import { Home, Users, FileText, DollarSign } from "lucide-react"

const navItems = [
  { title: "Dashboard", url: "/", icon: Home },
  { title: "Roster", url: "/roster", icon: Users },
  { title: "Contract Tools", url: "/contracts", icon: FileText },
  { title: "Salary Cap", url: "/cap", icon: DollarSign },
]

export function AppSidebar() {
  return (
    <Sidebar>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>ADL Contract Admin</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {navItems.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild>
                    <Link to={item.url}>
                      <item.icon />
                      <span>{item.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  )
}
```

### Pattern 3: Tabbed Player Detail Page
**What:** Use shadcn/ui Tabs for contract tools on player detail
**When to use:** Player detail page with multiple contract tool views
**Example:**
```tsx
// Source: shadcn/ui Tabs docs
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

export function PlayerDetailPage() {
  return (
    <div>
      <h1>Player Name</h1>
      <Tabs defaultValue="extensions">
        <TabsList>
          <TabsTrigger value="extensions">Extensions</TabsTrigger>
          <TabsTrigger value="tags">Tags</TabsTrigger>
          <TabsTrigger value="tenders">Tenders</TabsTrigger>
          <TabsTrigger value="buyout">Buyout</TabsTrigger>
          <TabsTrigger value="5yo">5YO</TabsTrigger>
          <TabsTrigger value="ppe">PPE</TabsTrigger>
        </TabsList>
        <TabsContent value="extensions">Extension tools placeholder</TabsContent>
        <TabsContent value="tags">Tag tools placeholder</TabsContent>
        {/* ... */}
      </Tabs>
    </div>
  )
}
```

### Pattern 4: React Router Nested Routes
**What:** Define routes with a layout wrapper for consistent sidebar
**When to use:** Route configuration
**Example:**
```tsx
// Source: React Router v7 docs
import { BrowserRouter, Routes, Route } from "react-router-dom"
import { AppLayout } from "@/components/layout/AppLayout"

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route index element={<DashboardPage />} />
          <Route path="roster" element={<RosterPage />} />
          <Route path="roster/:playerId" element={<PlayerDetailPage />} />
          <Route path="cap" element={<SalaryCapPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
```

### Anti-Patterns to Avoid
- **Building custom sidebar from scratch:** shadcn/ui Sidebar handles collapsible, responsive, icon-mode, keyboard nav — don't reinvent
- **Using React Router framework mode for a simple SPA:** Adds SSR complexity, file-based routing conventions; library mode is simpler and sufficient
- **Importing all shadcn/ui components upfront:** Add components via CLI as needed; they're copy-pasted into your project, not a monolithic bundle
- **Tailwind v4 with tailwind.config.js:** v4 uses CSS-first @theme directive; don't create a JS config file
</architecture_patterns>

<dont_hand_roll>
## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Sidebar navigation | Custom div + CSS sidebar | shadcn/ui `<Sidebar>` | Handles collapsible, responsive, icon-mode, mobile overlay, keyboard shortcuts, accessibility |
| Tabbed interface | Custom tab state + buttons | shadcn/ui `<Tabs>` | Built on Radix UI, accessible, keyboard navigable, ARIA-compliant |
| Class name merging | Manual string concatenation | shadcn/ui `cn()` (clsx + tailwind-merge) | Handles Tailwind class conflicts correctly |
| Icon set | Custom SVGs or icon fonts | lucide-react | Tree-shakable, consistent style, ships with shadcn/ui |
| Layout shell | Custom flexbox sidebar+content | `SidebarProvider` + `SidebarInset` | Handles responsive breakpoints, collapse state, CSS transitions |

**Key insight:** shadcn/ui provides the exact UI patterns needed (sidebar, tabs, navigation) as accessible, tested components. The value of Phase 7 is getting the routing and page structure right, not building UI primitives.
</dont_hand_roll>

<common_pitfalls>
## Common Pitfalls

### Pitfall 1: Tailwind v4 Configuration Confusion
**What goes wrong:** Trying to use `tailwind.config.js` or `@tailwind` directives (v3 patterns)
**Why it happens:** Most tutorials and Claude's training data reference v3 patterns
**How to avoid:** Use CSS-first config: `@import "tailwindcss"` in CSS, `@theme` directive for customization, `@tailwindcss/vite` plugin in vite.config.ts (not PostCSS)
**Warning signs:** "Unknown at rule @tailwind" errors, config file not being read

### Pitfall 2: shadcn/ui Path Alias Misconfiguration
**What goes wrong:** Components import from `@/components/ui/...` but path alias isn't configured
**Why it happens:** Vite needs both tsconfig paths AND vite resolve alias configured
**How to avoid:** Run `npx shadcn@latest init` which configures both, or manually set `@` alias in both tsconfig.json and vite.config.ts
**Warning signs:** "Cannot find module '@/components/...'" errors

### Pitfall 3: React Router v7 Import Confusion
**What goes wrong:** Importing from `react-router` instead of `react-router-dom` in library mode, or using framework-mode APIs
**Why it happens:** v7 docs mix library and framework mode examples; framework mode uses `react-router` package, library mode uses `react-router-dom`
**How to avoid:** In library mode, always import from `react-router-dom`. Don't use `@react-router/dev/routes` — that's framework mode.
**Warning signs:** Missing exports, unexpected SSR behavior

### Pitfall 4: Over-Building Placeholder Pages
**What goes wrong:** Building real components/data fetching in "placeholder" phase
**Why it happens:** Temptation to start Phase 8 work early
**How to avoid:** Placeholder pages should have: route, position in nav, heading, description of what will go there. No API calls, no real data, no complex components.
**Warning signs:** Pages that fetch data, complex state management in Phase 7

### Pitfall 5: Tailwind v4 Default Changes Breaking Layout
**What goes wrong:** Border colors, ring widths, gradient utilities behave differently than expected
**Why it happens:** v4 changed defaults: border color → currentColor (was gray), ring width → 1px (was 3px), gradient syntax changed
**How to avoid:** Be explicit with border colors (`border-gray-200`), test visual output early
**Warning signs:** Borders appearing darker than expected, rings looking different
</common_pitfalls>

<code_examples>
## Code Examples

Verified patterns from official sources:

### Vite Config with Tailwind v4
```typescript
// Source: Tailwind CSS v4 docs + Vite integration
import { defineConfig } from "vite"
import react from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"
import path from "path"

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
})
```

### Tailwind v4 CSS Entry Point
```css
/* Source: Tailwind v4 docs — CSS-first config */
@import "tailwindcss";

@theme {
  --color-primary: oklch(0.55 0.2 250);
  --color-sidebar: oklch(0.97 0.01 250);
  --font-sans: "Inter", sans-serif;
}
```

### shadcn/ui Sidebar with Collapsible Groups
```tsx
// Source: shadcn/ui sidebar component docs
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import {
  Sidebar, SidebarContent, SidebarGroup, SidebarGroupLabel,
  SidebarGroupContent, SidebarMenu, SidebarMenuItem, SidebarMenuButton,
} from "@/components/ui/sidebar"
import { ChevronDown } from "lucide-react"

export function AppSidebar() {
  return (
    <Sidebar collapsible="icon">
      <SidebarContent>
        {/* Fixed group */}
        <SidebarGroup>
          <SidebarGroupLabel>Main</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>{/* menu items */}</SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>

        {/* Collapsible group */}
        <Collapsible defaultOpen className="group/collapsible">
          <SidebarGroup>
            <SidebarGroupLabel asChild>
              <CollapsibleTrigger>
                Contract Tools
                <ChevronDown className="ml-auto transition-transform group-data-[state=open]/collapsible:rotate-180" />
              </CollapsibleTrigger>
            </SidebarGroupLabel>
            <CollapsibleContent>
              <SidebarGroupContent>
                <SidebarMenu>{/* tool menu items */}</SidebarMenu>
              </SidebarGroupContent>
            </CollapsibleContent>
          </SidebarGroup>
        </Collapsible>
      </SidebarContent>
    </Sidebar>
  )
}
```

### React Router v7 Library Mode Setup
```tsx
// Source: React Router v7 docs — library mode
import { BrowserRouter, Routes, Route, Link, Outlet } from "react-router-dom"

// Layout with sidebar + content area
function RootLayout() {
  return (
    <div className="flex min-h-screen">
      <nav className="w-64 border-r">{/* sidebar */}</nav>
      <main className="flex-1">
        <Outlet />
      </main>
    </div>
  )
}

// Route config
function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<RootLayout />}>
          <Route index element={<Dashboard />} />
          <Route path="roster" element={<Roster />} />
          <Route path="roster/:playerId" element={<PlayerDetail />} />
          <Route path="cap" element={<SalaryCap />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
```
</code_examples>

<sota_updates>
## State of the Art (2025-2026)

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Create React App (CRA) | Vite | 2023+ (CRA deprecated) | Vite is the standard React scaffold tool |
| Tailwind v3 (JS config) | Tailwind v4 (CSS-first @theme) | Jan 2025 | No more tailwind.config.js, use @import + @theme |
| Tailwind PostCSS plugin | @tailwindcss/vite plugin | Jan 2025 | Dedicated Vite plugin, not PostCSS |
| HSL color system | OKLCH color system | Tailwind v4 | Better perceptual uniformity, shadcn/ui converted |
| React 18 | React 19 | 2024 | forwardRef removed, new hooks, shadcn/ui updated |
| React Router v6 | React Router v7 | Late 2024 | Library mode (SPA) vs Framework mode (SSR), same API surface for library mode |
| shadcn/ui + Tailwind v3 | shadcn/ui + Tailwind v4 | Early 2025 | All components updated, data-slot attributes, OKLCH colors |

**New tools/patterns to consider:**
- **shadcn/ui CLI (`npx shadcn@latest`):** Now handles init + component installation, configures Tailwind v4 automatically
- **React Router v7 library mode:** Simplest SPA setup, same familiar API from v6
- **Tailwind v4 Oxide engine:** 2-5x faster builds via Rust engine

**Deprecated/outdated:**
- **Create React App:** Officially deprecated, do not use
- **tailwind.config.js:** v4 uses CSS-first config; JS config only for v3 compat
- **@tailwind base/components/utilities:** v4 uses `@import "tailwindcss"` instead
- **React Router v6 data APIs (createBrowserRouter):** Still works but v7 library mode with `<BrowserRouter>` is simpler for SPAs without loaders
</sota_updates>

<open_questions>
## Open Questions

1. **Frontend directory placement**
   - What we know: Backend is in `src/app/`. Need a `frontend/` directory.
   - What's unclear: Whether it should be `frontend/` at project root or `src/frontend/`
   - Recommendation: Use `frontend/` at project root — standard monorepo convention, separate package.json, clear backend/frontend separation

2. **API proxy in development**
   - What we know: Backend runs on a port (likely 8000), frontend dev server on another (5173)
   - What's unclear: Whether to configure Vite proxy now or defer to Phase 8
   - Recommendation: Defer — Phase 7 has no API calls. Add Vite proxy config in Phase 8 when API integration begins.

3. **Node.js version requirement**
   - What we know: Tailwind v4 upgrade tool requires Node.js 20+. Modern Vite requires Node 18+.
   - What's unclear: What Node version is installed on the development machine
   - Recommendation: Verify Node version during scaffold; target Node 20+ for full compatibility
</open_questions>

<sources>
## Sources

### Primary (HIGH confidence)
- Context7 /shadcn/ui — sidebar component, tabs component, navigation menu, Vite setup
- Context7 /websites/reactrouter — nested routes, Outlet, layout pattern, v7 SPA setup
- [Tailwind CSS v4 Upgrade Guide](https://tailwindcss.com/docs/upgrade-guide) — breaking changes, CSS-first config
- [shadcn/ui Tailwind v4 docs](https://ui.shadcn.com/docs/tailwind-v4) — v4 support, OKLCH colors
- [shadcn/ui Vite installation](https://ui.shadcn.com/docs/installation/vite) — setup steps
- [React Router SPA How-To](https://reactrouter.com/how-to/spa) — library mode SPA setup
- [React Router Modes](https://reactrouter.com/start/modes) — library vs framework mode

### Secondary (MEDIUM confidence)
- [TanStack Router vs React Router v7 comparison](https://medium.com/ekino-france/tanstack-router-vs-react-router-v7-32dddc4fcd58) — verified TanStack Router tradeoffs against official docs
- [Vite Getting Started](https://vite.dev/guide/) — current scaffold command
- [React 19 + Vite + Tailwind v4 + shadcn/ui starter template](https://dev.to/molly_1024/the-ultimate-react-19-vite-tailwind-css-v4-shadcn-ui-react-router-v7-starter-template-113p) — verified stack compatibility

### Tertiary (LOW confidence - needs validation)
- None — all findings verified against official documentation
</sources>

<metadata>
## Metadata

**Research scope:**
- Core technology: React 19 + TypeScript + Vite
- Ecosystem: shadcn/ui, Tailwind CSS v4, React Router v7, lucide-react
- Patterns: Sidebar layout, nested routes, tabbed views, placeholder pages
- Pitfalls: Tailwind v4 config, path aliases, React Router import modes

**Confidence breakdown:**
- Standard stack: HIGH — all libraries are current stable releases, verified via Context7 and official docs
- Architecture: HIGH — patterns directly from shadcn/ui docs and React Router tutorials
- Pitfalls: HIGH — documented breaking changes in Tailwind v4, known path alias issues
- Code examples: HIGH — from Context7 (shadcn/ui, React Router) and official documentation

**Research date:** 2026-03-11
**Valid until:** 2026-04-11 (30 days — React ecosystem stable, no major releases expected)
</metadata>

---

*Phase: 07-frontend-placeholder*
*Research completed: 2026-03-11*
*Ready for planning: yes*
