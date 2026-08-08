"use client";

import { KpiCard } from "@/components/ui/KpiCard";
import { fmtARS, fmtPct, fmtDelta } from "@/lib/formatters";
import type { LatestResponse } from "@/types/portfolio";
import type { ReturnsResponse } from "@/types/returns";
import type { MonthlyKPI } from "@/types/inflation";

interface KpiStripProps {
  latest: LatestResponse | undefined;
  returns: ReturnsResponse | undefined;
  monthlyKpi: MonthlyKPI | undefined;
  loading?: boolean;
}

export function KpiStrip({ latest, returns, monthlyKpi, loading }: KpiStripProps) {
  const snapshot = latest?.snapshot;
  const daily = returns?.daily;
  const monthly = returns?.monthly;
  const inception = returns?.inception;

  const cashPct =
    snapshot && snapshot.total_value
      ? ((snapshot.cash_total_ars ?? 0) / snapshot.total_value) * 100
      : null;

  return (
    <div className="space-y-3">
      {/* Primary KPIs */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <KpiCard
          label="Portfolio total"
          value={fmtARS(snapshot?.total_value ?? null)}
          subvalue={snapshot?.snapshot_date}
          loading={loading}
        />
        <KpiCard
          label="Retorno diario"
          value={fmtPct(daily?.pct ?? null)}
          delta={fmtDelta(daily?.delta ?? null)}
          trend={
            daily?.pct == null ? null : daily.pct > 0 ? "up" : daily.pct < 0 ? "down" : "flat"
          }
          loading={loading}
        />
        <KpiCard
          label="Retorno mensual"
          value={fmtPct(monthly?.pct ?? null)}
          delta={fmtDelta(monthly?.delta ?? null)}
          trend={
            monthly?.pct == null
              ? null
              : monthly.pct > 0
                ? "up"
                : monthly.pct < 0
                  ? "down"
                  : "flat"
          }
          loading={loading}
        />
        <KpiCard
          label="Desde inicio"
          value={fmtPct(inception?.pct ?? null)}
          subvalue={inception?.from ? `desde ${inception.from}` : undefined}
          trend={
            inception?.pct == null
              ? null
              : inception.pct > 0
                ? "up"
                : inception.pct < 0
                  ? "down"
                  : "flat"
          }
          loading={loading}
        />
      </div>

      {/* Secondary KPIs */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <KpiCard
          label="Liquidez"
          value={cashPct != null ? fmtPct(cashPct) : "—"}
          subvalue={
            snapshot
              ? [
                  snapshot.cash_disponible_ars != null
                    ? `ARS ${fmtARS(snapshot.cash_disponible_ars)}`
                    : null,
                  snapshot.cash_disponible_usd != null
                    ? `USD ${snapshot.cash_disponible_usd.toFixed(2)}`
                    : null,
                ]
                  .filter(Boolean)
                  .join(" · ") || undefined
              : undefined
          }
          loading={loading}
        />
        <KpiCard
          label="vs Inflación (mes)"
          value={
            monthlyKpi?.real_vs_inflation_pct != null
              ? fmtPct(monthlyKpi.real_vs_inflation_pct)
              : "—"
          }
          subvalue={
            monthlyKpi?.beats_inflation != null
              ? monthlyKpi.beats_inflation
                ? "Supera inflación"
                : "Por debajo de inflación"
              : undefined
          }
          trend={
            monthlyKpi?.real_vs_inflation_pct == null
              ? null
              : monthlyKpi.real_vs_inflation_pct > 0
                ? "up"
                : monthlyKpi.real_vs_inflation_pct < 0
                  ? "down"
                  : "flat"
          }
          loading={loading}
        />
        <KpiCard
          label="Semana"
          value={fmtPct(returns?.weekly?.pct ?? null)}
          delta={fmtDelta(returns?.weekly?.delta ?? null)}
          trend={
            returns?.weekly?.pct == null
              ? null
              : returns.weekly.pct > 0
                ? "up"
                : returns.weekly.pct < 0
                  ? "down"
                  : "flat"
          }
          loading={loading}
        />
      </div>
    </div>
  );
}
