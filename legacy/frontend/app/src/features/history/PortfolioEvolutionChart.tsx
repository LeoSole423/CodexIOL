"use client";

import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtARS, fmtDate } from "@/lib/formatters";
import type { SnapshotPoint } from "@/types/returns";

interface PortfolioEvolutionChartProps {
  data: SnapshotPoint[];
  height?: number;
}

export function PortfolioEvolutionChart({
  data,
  height = 280,
}: PortfolioEvolutionChartProps) {
  if (data.length === 0) return null;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <AreaChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="portfolioGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="hsl(var(--primary))" stopOpacity={0.2} />
            <stop offset="100%" stopColor="hsl(var(--primary))" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid
          strokeDasharray="3 3"
          stroke="hsl(var(--border))"
          strokeOpacity={0.5}
          vertical={false}
        />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 11, fill: "hsl(var(--muted))" }}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v: string) => {
            const d = new Date(v + "T12:00:00");
            return d.toLocaleDateString("es-AR", { month: "short", year: "2-digit" });
          }}
          interval="preserveStartEnd"
          minTickGap={40}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "hsl(var(--muted))" }}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v: number) => {
            if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
            if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`;
            return `$${v}`;
          }}
          width={60}
        />
        <Tooltip
          content={({ active, payload }) => {
            if (!active || !payload?.length) return null;
            const d = payload[0]?.payload as SnapshotPoint;
            return (
              <div className="rounded-md border border-border bg-surface shadow-sm px-3 py-2 text-xs">
                <p className="text-muted-foreground mb-1">{fmtDate(d.date)}</p>
                <p className="font-semibold text-foreground">{fmtARS(d.total_value)}</p>
              </div>
            );
          }}
        />
        <Area
          type="monotone"
          dataKey="total_value"
          stroke="hsl(var(--primary))"
          strokeWidth={1.5}
          fill="url(#portfolioGrad)"
          dot={false}
          activeDot={{ r: 3, strokeWidth: 0 }}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
