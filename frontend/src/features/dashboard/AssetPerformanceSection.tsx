"use client";

import { useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { DeltaBadge } from "@/components/ui/DeltaBadge";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtARS, fmtPct } from "@/lib/formatters";
import { useAssetPerformance } from "@/hooks/usePortfolio";
import type { AssetPeriod } from "@/types/portfolio";

const now = new Date();
const CURRENT_YEAR = now.getFullYear();
const CURRENT_MONTH = now.getMonth() + 1;

export function AssetPerformanceSection() {
  const [period, setPeriod] = useState<AssetPeriod>("daily");
  const [month, setMonth] = useState(CURRENT_MONTH);
  const [year, setYear] = useState(CURRENT_YEAR);

  const { data, isLoading } = useAssetPerformance(
    period === "monthly"
      ? { period, month, year }
      : period === "yearly"
        ? { period, year }
        : { period }
  );

  function prevPeriod() {
    if (period === "monthly") {
      if (month === 1) { setMonth(12); setYear((y) => y - 1); }
      else setMonth((m) => m - 1);
    } else if (period === "yearly") {
      setYear((y) => y - 1);
    }
  }

  function nextPeriod() {
    if (period === "monthly") {
      if (month === 12) { setMonth(1); setYear((y) => y + 1); }
      else setMonth((m) => m + 1);
    } else if (period === "yearly") {
      setYear((y) => y + 1);
    }
  }

  const MONTH_NAMES = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];

  const periodLabel =
    period === "monthly"
      ? `${MONTH_NAMES[month - 1]} ${year}`
      : period === "yearly"
        ? String(year)
        : null;

  return (
    <Card>
      <CardHeader className="flex flex-col sm:flex-row sm:items-center gap-3 pb-3">
        <p className="text-sm font-semibold text-foreground">Rendimiento por activo</p>
        <div className="flex items-center gap-2 flex-wrap ml-auto">
          {(period === "monthly" || period === "yearly") && (
            <div className="flex items-center gap-1">
              <Button variant="ghost" size="icon" className="h-7 w-7" onClick={prevPeriod}>
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="text-xs font-medium text-foreground w-20 text-center">
                {periodLabel}
              </span>
              <Button variant="ghost" size="icon" className="h-7 w-7" onClick={nextPeriod}>
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          )}
          <Tabs value={period} onValueChange={(v) => setPeriod(v as AssetPeriod)}>
            <TabsList>
              <TabsTrigger value="daily">Día</TabsTrigger>
              <TabsTrigger value="weekly">Sem</TabsTrigger>
              <TabsTrigger value="monthly">Mes</TabsTrigger>
              <TabsTrigger value="yearly">Año</TabsTrigger>
              <TabsTrigger value="accumulated">Total</TabsTrigger>
            </TabsList>
          </Tabs>
        </div>
      </CardHeader>
      <CardContent className="pt-0">
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : !data?.rows?.length ? (
          <EmptyState message="Sin datos para el período seleccionado" />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-2 text-xs font-medium text-muted-foreground">Símbolo</th>
                  <th className="text-left py-2 text-xs font-medium text-muted-foreground hidden sm:table-cell">Descripción</th>
                  <th className="text-right py-2 text-xs font-medium text-muted-foreground">Valor</th>
                  <th className="text-right py-2 text-xs font-medium text-muted-foreground">Var. período</th>
                  <th className="text-right py-2 text-xs font-medium text-muted-foreground">Peso</th>
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row) => (
                  <tr
                    key={row.symbol}
                    className="border-b border-border last:border-0 hover:bg-muted/5 transition-colors"
                  >
                    <td className="py-2.5 font-mono text-xs font-semibold text-foreground whitespace-nowrap pr-2">
                      {row.symbol}
                    </td>
                    <td className="py-2.5 text-xs text-muted-foreground max-w-[140px] truncate hidden sm:table-cell">
                      {row.description}
                    </td>
                    <td className="py-2.5 text-right text-xs font-medium text-foreground whitespace-nowrap">
                      {fmtARS(row.total_value)}
                    </td>
                    <td className="py-2.5 text-right whitespace-nowrap">
                      <DeltaBadge value={row.selected_pct} format="pct" />
                    </td>
                    <td className="py-2.5 text-right text-xs text-muted-foreground whitespace-nowrap">
                      {fmtPct(row.weight_pct)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
