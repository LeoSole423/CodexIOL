"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fmtDate, fmtPct } from "@/lib/formatters";
import type { InflationSeriesComparison } from "@/types/inflation";

interface InflationComparisonChartProps {
  data: InflationSeriesComparison;
  height?: number;
}

export function InflationComparisonChart({
  data,
  height = 260,
}: InflationComparisonChartProps) {
  const chartData = data.labels.map((label, i) => ({
    date: label,
    portfolio: data.portfolio_index[i],
    inflation: data.inflation_index[i],
  }));

  if (chartData.length === 0) return null;

  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart data={chartData} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
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
          minTickGap={50}
        />
        <YAxis
          tick={{ fontSize: 11, fill: "hsl(var(--muted))" }}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v: number) => `${v.toFixed(0)}`}
          width={44}
        />
        <Tooltip
          content={({ active, payload, label }) => {
            if (!active || !payload?.length) return null;
            return (
              <div className="rounded-md border border-border bg-surface shadow-sm px-3 py-2 text-xs space-y-1">
                <p className="text-muted-foreground">{fmtDate(label as string)}</p>
                {payload.map((p) => (
                  <p key={p.dataKey as string} style={{ color: p.color }} className="font-medium">
                    {p.name}: {typeof p.value === "number" ? p.value.toFixed(1) : "—"}
                  </p>
                ))}
              </div>
            );
          }}
        />
        <Legend
          wrapperStyle={{ fontSize: "11px", paddingTop: "8px" }}
          formatter={(value: string) =>
            value === "portfolio" ? "Portfolio" : "Inflación (IPC)"
          }
        />
        <Line
          type="monotone"
          dataKey="portfolio"
          name="portfolio"
          stroke="hsl(var(--primary))"
          strokeWidth={1.5}
          dot={false}
          activeDot={{ r: 3 }}
          connectNulls
        />
        <Line
          type="monotone"
          dataKey="inflation"
          name="inflation"
          stroke="hsl(var(--warning))"
          strokeWidth={1.5}
          strokeDasharray="4 2"
          dot={false}
          activeDot={{ r: 3 }}
          connectNulls
        />
      </LineChart>
    </ResponsiveContainer>
  );
}
