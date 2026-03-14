# Frontend Style Guide

> Source of truth for all frontend decisions in the Next.js dashboard (`frontend/`).

---

## Project

Build a corporate, clean, modern frontend.
The UI must feel professional, trustworthy, minimal, and consistent.
Always support responsive design, light mode, and dark mode.

---

## Stack

| Tool | Purpose |
|------|---------|
| Next.js (App Router) | Routing, SSR, page shell |
| TypeScript | Type safety across all components |
| Tailwind CSS | Utility-first styling |
| shadcn/ui | Accessible, composable base components |
| lucide-react | Icon set |
| motion | Subtle animations (framer-motion) |
| React Hook Form | Form state management |
| Zod | Schema validation (forms + API payloads) |
| TanStack Query | Server state, caching, refetch |
| Recharts | Charts (line, bar, pie/donut) |
| next-themes | Light/dark mode toggle |

---

## Style

The UI should feel:
- Corporate
- Modern
- Clean
- Trustworthy
- Medium-high formality

**Avoid:**
- Generic layouts (cookie-cutter dashboard templates)
- Playful styling (rounded bubbles, emoji-heavy UI)
- Heavy shadows or glow effects
- Too many accent colors
- Unnecessary animation or motion
- Inconsistent spacing or components

---

## Typography & Shape

- **Font:** Inter (Google Fonts)
- Keep text highly legible — sufficient contrast, appropriate size
- Use soft rounded corners (`rounded-md`, `rounded-lg`)
- Use subtle borders and soft shadows only (`border border-border`, `shadow-sm`)
- Use subtle motion only — no bouncing, no attention-seeking transitions

---

## Theme

Always use theme-aware styles via CSS variables. Do not hardcode one-off colors in components.
Use Tailwind utility classes that map to CSS variables (e.g. `text-foreground`, `bg-surface`, `border-border`).

### Light Mode
| Token | Value |
|-------|-------|
| background | `#F8FAFC` |
| surface | `#FFFFFF` |
| foreground | `#0F172A` |
| primary | `#1D4ED8` |
| border | `#CBD5E1` |
| muted | `#64748B` |
| success | `#16A34A` |
| warning | `#D97706` |
| danger | `#DC2626` |

### Dark Mode
| Token | Value |
|-------|-------|
| background | `#0B1220` |
| surface | `#111827` |
| foreground | `#E5E7EB` |
| primary | `#60A5FA` |
| border | `#334155` |
| muted | `#94A3B8` |
| success | `#22C55E` |
| warning | `#F59E0B` |
| danger | `#EF4444` |

---

## Project Structure

```
frontend/
  src/
    app/                    ← Next.js App Router pages and layouts
    components/
      ui/                   ← Shared, reusable UI atoms (KpiCard, DeltaBadge, etc.)
      layout/               ← Layout pieces: Topbar, Footer, sidebar
    features/
      dashboard/            ← Dashboard-specific components and hooks
      advisor/              ← Advisor page components
      quality/              ← Quality page components
      assets/               ← Assets page components
      history/              ← History page components
    lib/
      api.ts                ← API client functions (one per endpoint)
      formatters.ts         ← Number/date formatting utilities
    hooks/                  ← Reusable React hooks (TanStack Query wrappers)
    types/                  ← Shared TypeScript type definitions
```

---

## Rules

- **Reuse first.** Check existing components before creating a new one.
- **Tailwind first.** Avoid inline styles and one-off CSS classes.
- **Small components.** Each component does one thing, accepts typed props.
- **No extra libraries** without a strong reason and team discussion.
- **React Hook Form + Zod** for all forms.
- **TanStack Query** for all server state (no local fetch in components).
- **Always handle:** loading, empty, error, and success states.
- **Mobile and desktop from the start.** Never "fix mobile later".
- **Light and dark mode in every component.** No hardcoded colors.

---

## Done Means

A feature is complete only if:

- [ ] Builds successfully (`npm run build` exits 0, no TS errors)
- [ ] TypeScript is clean — no `any`, no suppressed errors
- [ ] Responsive — works on 375px and 1280px viewports
- [ ] Light mode looks correct
- [ ] Dark mode looks correct
- [ ] Matches project style (corporate, clean, Inter font)
- [ ] Loading state shows skeleton/spinner
- [ ] Empty state shows a message
- [ ] Error state shows an error with optional retry
- [ ] No obvious unused code remains

---

## Task Template

Use this format for every task:

```
Task: [short name]

Goal:
- [what to build or change]

Context:
- [route, feature, files, components to reuse]

Constraints:
- keep existing style and stack
- no unnecessary libraries
- responsive
- light/dark mode support
- clean TypeScript

Done when:
- builds successfully
- matches project style
- works on mobile and desktop
- handles relevant UI states
```

---

## Priority

When in doubt:

1. **Clarity** — the user understands what they're looking at
2. **Consistency** — same patterns across all pages
3. **Maintainability** — future devs can extend it easily
4. **Accessibility** — keyboard navigation, ARIA labels, color contrast
5. **User experience** — fast, predictable, helpful

---

## API Contract

The backend exposes a REST API at `http://web:8000/api/...` (Docker service name).
Next.js proxies `/api/:path*` → `http://web:8000/api/:path*` via `next.config.ts` rewrites.

All frontend API calls go through `src/lib/api.ts` typed functions.
All server state is managed by TanStack Query hooks in `src/hooks/`.

See the API documentation for response shapes:
- `/api/latest` — latest portfolio snapshot + assets
- `/api/returns` — multi-period return blocks
- `/api/snapshots` — portfolio time series
- `/api/allocation` — allocation breakdown
- `/api/assets/performance` — per-asset performance by period
- `/api/advisor/latest` — latest advisor briefing
- `/api/cashflows/manual` — manual cashflow adjustments (CRUD)
- `/api/cashflows/auto` — auto-detected cashflows
- `/api/quality` — data quality health rows
- `/api/reconciliation/open` — open reconciliation proposals
- `/api/kpi/monthly-vs-inflation` — monthly KPI vs inflation
- `/api/compare/inflation/series` — portfolio vs inflation indexed series
- `/api/compare/inflation/annual` — annual comparison

---

## Chart Guidelines

Use **Recharts** for all charts.

- Always use `<ResponsiveContainer width="100%" height={height}>` — never fixed widths
- Use `stroke="currentColor"` or theme color variables, not hardcoded hex
- Custom tooltip: use `bg-surface border border-border rounded-md shadow-sm p-2 text-sm`
- Axis labels: `text-muted-foreground text-xs`
- Line charts: subtle gradient fill beneath the line (low opacity)
- Bar charts: use `success` color for positive values, `danger` for negative
- Pie/donut: use shadcn `Card` for the legend, not inline HTML
- No animation on data updates — only on initial mount if needed
