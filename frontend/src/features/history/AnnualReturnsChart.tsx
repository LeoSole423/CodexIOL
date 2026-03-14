"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtPct } from "@/lib/formatters";
import type { AnnualComparisonRow } from "@/types/inflation";

interface AnnualReturnsChartProps {
  rows: AnnualComparisonRow[];
  height?: number;
}

export function AnnualReturnsChart({ rows, height = 260 }: AnnualReturnsChartProps) {
  if (rows.length === 0) return null;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart
        data={rows}
        margin={{ top: 4, right: 4, left: 0, bottom: 0 }}
        barCategoryGap="25%"
        barGap={2}
      >
        <CartesianGrid
          strokeDasharray="3 3"
          stroke="hsl(var(--border))"
          strokeOpacity={0.5}
          vertical={false}
        />
        <XAxis
          dataKey="label"
          tick={{ fontSize: 11, fill: "hsl(var(--muted))" }}
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "hsl(var(--muted))" }}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v: number) => `${v.toFixed(0)}%`}
          width={44}
        />
        <Tooltip
          content={({ active, payload, label }) => {
            if (!active || !payload?.length) return null;
            return (
              <div className="rounded-md border border-border bg-surface shadow-sm px-3 py-2 text-xs space-y-1">
                <p className="text-muted-foreground font-medium">{label as string}</p>
                {payload.map((p) => (
                  <p key={p.dataKey as string} style={{ color: p.color }} className="font-medium">
                    {p.name}:{" "}
                    {typeof p.value === "number" ? fmtPct(p.value) : "—"}
                  </p>
                ))}
              </div>
            );
          }}
        />
        <Legend
          wrapperStyle={{ fontSize: "11px", paddingTop: "8px" }}
          formatter={(value: string) =>
            value === "portfolio_pct" ? "Portfolio" : "Inflación (IPC)"
          }
        />
        <Bar dataKey="portfolio_pct" name="portfolio_pct" radius={[3, 3, 0, 0]}>
          {rows.map((row, i) => (
            <Cell
              key={i}
              fill={
                (row.portfolio_pct ?? 0) >= 0
                  ? "hsl(var(--primary))"
                  : "hsl(var(--danger))"
              }
            />
          ))}
        </Bar>
        <Bar
          dataKey="inflation_pct"
          name="inflation_pct"
          fill="hsl(var(--warning))"
          fillOpacity={0.6}
          radius={[3, 3, 0, 0]}
        />
      </BarChart>
    </ResponsiveContainer>
  );
}
