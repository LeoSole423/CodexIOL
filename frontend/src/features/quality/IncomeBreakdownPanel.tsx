"use client";

import { useState } from "react";
import { TrendingUp } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtARS } from "@/lib/formatters";
import { useIncomeSummary } from "@/hooks/usePortfolio";
import type { IncomeSummaryByKind, IncomeSummaryBySymbol } from "@/types/quality";

const KIND_LABELS: Record<string, string> = {
  dividend_income: "Dividendos",
  coupon_income: "Cupones",
  bond_amortization_income: "Amortizaciones",
};

const KIND_COLORS: Record<string, string> = {
  dividend_income: "bg-emerald-500/20 text-emerald-700 dark:text-emerald-400",
  coupon_income: "bg-blue-500/20 text-blue-700 dark:text-blue-400",
  bond_amortization_income: "bg-amber-500/20 text-amber-700 dark:text-amber-400",
};

function KindBadge({ kind }: { kind: string }) {
  const label = KIND_LABELS[kind] ?? kind;
  const color = KIND_COLORS[kind] ?? "bg-muted/20 text-muted-foreground";
  return (
    <span className={`text-xs font-medium px-2 py-0.5 rounded-md ${color}`}>
      {label}
    </span>
  );
}

function KindSummaryRow({ item }: { item: IncomeSummaryByKind }) {
  return (
    <div className="flex items-center gap-3 py-2 border-b border-border last:border-0">
      <KindBadge kind={item.kind} />
      <span className="text-xs text-muted-foreground flex-1">{item.count} mov.</span>
      <span className="text-xs font-semibold text-foreground tabular-nums">
        {fmtARS(item.total_ars)}
      </span>
    </div>
  );
}

function SymbolRow({ item }: { item: IncomeSummaryBySymbol }) {
  return (
    <div className="flex items-center gap-3 py-1.5 border-b border-border last:border-0">
      <span className="text-xs font-mono font-medium text-foreground w-16 shrink-0">
        {item.symbol}
      </span>
      <KindBadge kind={item.kind} />
      <span className="text-xs text-muted-foreground flex-1">{item.count}×</span>
      <span className="text-xs font-semibold text-foreground tabular-nums">
        {fmtARS(item.total_ars)}
      </span>
    </div>
  );
}

const DAYS_OPTIONS = [30, 60, 90, 180] as const;
type DaysOption = (typeof DAYS_OPTIONS)[number];

export function IncomeBreakdownPanel() {
  const [days, setDays] = useState<DaysOption>(60);
  const { data, isLoading } = useIncomeSummary(days);

  const hasData = (data?.by_kind.length ?? 0) > 0;

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
            <p className="text-sm font-semibold text-foreground">Ingresos importados</p>
          </div>
          <div className="flex gap-1">
            {DAYS_OPTIONS.map((d) => (
              <button
                key={d}
                onClick={() => setDays(d)}
                className={`text-xs px-2 py-0.5 rounded-md transition-colors ${
                  days === d
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:bg-muted/20"
                }`}
              >
                {d}d
              </button>
            ))}
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Dividendos, cupones y amortizaciones detectados desde órdenes IOL
        </p>
      </CardHeader>
      <CardContent className="pt-0 space-y-4">
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-8 w-full" />
            ))}
          </div>
        ) : !hasData ? (
          <EmptyState message="Sin ingresos registrados en el período" />
        ) : (
          <>
            {/* Total */}
            <div className="flex items-center justify-between rounded-md bg-muted/10 px-3 py-2">
              <span className="text-xs font-medium text-muted-foreground">Total {days}d</span>
              <span className="text-sm font-bold text-foreground tabular-nums">
                {fmtARS(data?.total_ars ?? 0)}
              </span>
            </div>

            {/* By kind */}
            <div>
              <p className="text-xs font-medium text-muted-foreground mb-1">Por tipo</p>
              <div className="rounded-md border border-border overflow-hidden px-3">
                {(data?.by_kind ?? []).map((item) => (
                  <KindSummaryRow key={item.kind} item={item} />
                ))}
              </div>
            </div>

            {/* By symbol */}
            {(data?.by_symbol.length ?? 0) > 0 && (
              <div>
                <p className="text-xs font-medium text-muted-foreground mb-1">Por símbolo</p>
                <div className="rounded-md border border-border overflow-hidden px-3 max-h-48 overflow-y-auto">
                  {(data?.by_symbol ?? []).map((item, i) => (
                    <SymbolRow key={`${item.symbol}-${item.kind}-${i}`} item={item} />
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}
