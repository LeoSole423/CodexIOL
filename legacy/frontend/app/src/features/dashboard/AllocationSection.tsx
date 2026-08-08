"use client";

import { useState } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { fmtPct } from "@/lib/formatters";
import { useAllocation } from "@/hooks/usePortfolio";
import type { AllocationGroupBy } from "@/types/portfolio";

const COLORS = [
  "hsl(var(--primary))",
  "hsl(var(--success))",
  "hsl(var(--warning))",
  "hsl(var(--danger))",
  "#8b5cf6",
  "#06b6d4",
  "#f97316",
  "#ec4899",
];

export function AllocationSection() {
  const [groupBy, setGroupBy] = useState<AllocationGroupBy>("type");
  const [includeCash, setIncludeCash] = useState<0 | 1>(0);

  const { data, isLoading } = useAllocation({ group_by: groupBy, include_cash: includeCash });

  const total = data?.reduce((s, d) => s + d.value, 0) ?? 0;

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between gap-2 pb-3">
        <p className="text-sm font-semibold text-foreground">Asignación</p>
        <div className="flex items-center gap-2">
          <Select
            value={groupBy}
            onValueChange={(v) => setGroupBy(v as AllocationGroupBy)}
          >
            <SelectTrigger className="w-28 h-7 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="symbol">Símbolo</SelectItem>
              <SelectItem value="type">Tipo</SelectItem>
              <SelectItem value="market">Mercado</SelectItem>
              <SelectItem value="currency">Moneda</SelectItem>
            </SelectContent>
          </Select>
          <button
            className="text-xs text-muted-foreground hover:text-foreground transition-colors"
            onClick={() => setIncludeCash((c) => (c === 0 ? 1 : 0))}
          >
            {includeCash ? "Sin caja" : "+ Caja"}
          </button>
        </div>
      </CardHeader>
      <CardContent className="pt-0">
        {isLoading ? (
          <Skeleton className="w-full h-48" />
        ) : (
          <div className="flex flex-col items-center gap-4">
            <ResponsiveContainer width="100%" height={180}>
              <PieChart>
                <Pie
                  data={data ?? []}
                  dataKey="value"
                  nameKey="key"
                  cx="50%"
                  cy="50%"
                  innerRadius={50}
                  outerRadius={80}
                  paddingAngle={2}
                >
                  {(data ?? []).map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null;
                    const d = payload[0]?.payload as { key: string; value: number };
                    return (
                      <div className="rounded-md border border-border bg-surface shadow-sm px-3 py-2 text-xs">
                        <p className="font-medium text-foreground">{d.key}</p>
                        <p className="text-muted-foreground">
                          {fmtPct(total > 0 ? (d.value / total) * 100 : 0)}
                        </p>
                      </div>
                    );
                  }}
                />
              </PieChart>
            </ResponsiveContainer>

            {/* Legend */}
            <div className="w-full space-y-1.5">
              {(data ?? []).slice(0, 8).map((item, i) => {
                const pct = total > 0 ? (item.value / total) * 100 : 0;
                return (
                  <div key={item.key} className="flex items-center gap-2">
                    <div
                      className="h-2 w-2 rounded-full shrink-0"
                      style={{ backgroundColor: COLORS[i % COLORS.length] }}
                    />
                    <span className="text-xs text-muted-foreground flex-1 truncate">
                      {item.key}
                    </span>
                    <span className="text-xs font-medium text-foreground">
                      {fmtPct(pct)}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
