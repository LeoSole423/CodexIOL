"use client";

import { useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import { fmtARS, fmtDate } from "@/lib/formatters";
import { useSnapshots } from "@/hooks/useReturns";

type RangeDays = 30 | 90 | 365 | 0;
type EvolutionMode = "raw" | "market";

function daysAgo(days: number): string {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().split("T")[0];
}

export function EvolutionSection() {
  const [range, setRange] = useState<RangeDays>(90);
  const [mode, setMode] = useState<EvolutionMode>("raw");

  const params =
    range === 0
      ? { mode }
      : { from: daysAgo(range), mode };

  const { data, isLoading } = useSnapshots(params);

  const chartData = (data ?? []).map((p) => ({
    date: p.date,
    value: p.total_value,
  }));

  return (
    <Card>
      <CardHeader className="flex flex-col sm:flex-row sm:items-center gap-3 pb-3">
        <div className="flex-1">
          <p className="text-sm font-semibold text-foreground">Evolución del portfolio</p>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <Tabs
            value={String(range)}
            onValueChange={(v) => setRange(Number(v) as RangeDays)}
          >
            <TabsList>
              {([30, 90, 365, 0] as RangeDays[]).map((r) => (
                <TabsTrigger key={r} value={String(r)}>
                  {r === 0 ? "Todo" : r === 365 ? "1A" : r === 90 ? "3M" : "1M"}
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
          <Tabs value={mode} onValueChange={(v) => setMode(v as EvolutionMode)}>
            <TabsList>
              <TabsTrigger value="raw">Mercado</TabsTrigger>
              <TabsTrigger value="market">Ajustado</TabsTrigger>
            </TabsList>
          </Tabs>
        </div>
      </CardHeader>
      <CardContent className="pt-0">
        {isLoading ? (
          <Skeleton className="w-full h-[220px]" />
        ) : (
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={chartData} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="evoGrad" x1="0" y1="0" x2="0" y2="1">
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
                  return range <= 30
                    ? d.toLocaleDateString("es-AR", { day: "2-digit", month: "short" })
                    : d.toLocaleDateString("es-AR", { month: "short", year: "2-digit" });
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
                  const d = payload[0]?.payload as { date: string; value: number };
                  return (
                    <div className="rounded-md border border-border bg-surface shadow-sm px-3 py-2 text-xs">
                      <p className="text-muted-foreground mb-1">{fmtDate(d.date)}</p>
                      <p className="font-semibold text-foreground">{fmtARS(d.value)}</p>
                    </div>
                  );
                }}
              />
              <Area
                type="monotone"
                dataKey="value"
                stroke="hsl(var(--primary))"
                strokeWidth={1.5}
                fill="url(#evoGrad)"
                dot={false}
                activeDot={{ r: 3, strokeWidth: 0 }}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </CardContent>
    </Card>
  );
}
