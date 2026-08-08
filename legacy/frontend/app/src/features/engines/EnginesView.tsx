"use client";

import { useState } from "react";
import { RefreshCw, TrendingUp, Globe, Users } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/ui/ErrorState";
import { SectionHeader } from "@/components/ui/SectionHeader";
import { useEngines, type AccuracyRow } from "./useEngines";
import { cn } from "@/lib/utils";

// ── Regime card ───────────────────────────────────────────────────────────────

function RegimeCard({ signal }: { signal: Record<string, unknown> | null }) {
  if (!signal) return (
    <Card>
      <CardContent className="pt-5">
        <p className="text-sm text-muted-foreground">Sin señal de régimen. Ejecutá: <code>iol engines regime run</code></p>
      </CardContent>
    </Card>
  );

  const regime = String(signal.regime ?? "").toLowerCase();
  const regimeColor =
    regime === "bull" ? "text-success" :
    regime === "bear" || regime === "crisis" ? "text-destructive" :
    "text-warning";
  const adj = Number(signal.defensive_weight_adjustment ?? 0);

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <TrendingUp className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium text-muted-foreground">Régimen de Mercado</span>
          <span className="text-xs text-muted-foreground ml-auto">{String(signal.as_of ?? "")}</span>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex items-baseline gap-3">
          <span className={cn("text-2xl font-bold uppercase", regimeColor)}>
            {String(signal.regime ?? "—")}
          </span>
          <span className="text-sm text-muted-foreground">
            score {Number(signal.regime_score ?? 0).toFixed(1)}/100
          </span>
          <Badge variant="secondary" className="ml-auto">
            {Number(signal.confidence ?? 0).toLocaleString(undefined, { style: "percent" })} confianza
          </Badge>
        </div>
        <div className="grid grid-cols-2 gap-3 text-sm">
          <div>
            <p className="text-muted-foreground text-xs">Amplitud</p>
            <p className="font-medium">{Number(signal.breadth_score ?? 0).toFixed(1)}% &gt; MA50</p>
          </div>
          <div>
            <p className="text-muted-foreground text-xs">Volatilidad</p>
            <p className="font-medium capitalize">{String(signal.volatility_regime ?? "—")}</p>
          </div>
          <div>
            <p className="text-muted-foreground text-xs">Ajuste equity</p>
            <p className={cn("font-medium", adj < 0 ? "text-destructive" : "text-success")}>
              {adj >= 0 ? "+" : ""}{(adj * 100).toFixed(0)}%
            </p>
          </div>
          <div>
            <p className="text-muted-foreground text-xs">Favorecidos</p>
            <p className="font-medium text-xs">{(signal.favored_asset_classes as string[] ?? []).join(", ") || "—"}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

// ── Macro card ────────────────────────────────────────────────────────────────

function MacroCard({ signal }: { signal: Record<string, unknown> | null }) {
  if (!signal) return (
    <Card>
      <CardContent className="pt-5">
        <p className="text-sm text-muted-foreground">Sin señal macro. Ejecutá: <code>iol engines macro run</code></p>
      </CardContent>
    </Card>
  );

  const stress = Number(signal.argentina_macro_stress ?? 50);
  const riskOn = Number(signal.global_risk_on ?? 50);
  const stressColor = stress < 30 ? "text-success" : stress < 60 ? "text-warning" : "text-destructive";
  const riskColor = riskOn >= 60 ? "text-success" : riskOn >= 40 ? "text-warning" : "text-destructive";

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <Globe className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium text-muted-foreground">Momentum Macro</span>
          <span className="text-xs text-muted-foreground ml-auto">{String(signal.as_of ?? "")}</span>
        </div>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <p className="text-xs text-muted-foreground">Estrés AR</p>
            <p className={cn("text-2xl font-bold", stressColor)}>{stress.toFixed(0)}<span className="text-sm font-normal text-muted-foreground">/100</span></p>
            <p className="text-xs text-muted-foreground">mayor = peor</p>
          </div>
          <div>
            <p className="text-xs text-muted-foreground">Global Risk-On</p>
            <p className={cn("text-2xl font-bold", riskColor)}>{riskOn.toFixed(0)}<span className="text-sm font-normal text-muted-foreground">/100</span></p>
            <p className="text-xs text-muted-foreground">mayor = mejor</p>
          </div>
        </div>
        <div className="grid grid-cols-3 gap-3 text-sm border-t pt-3">
          {signal.bcra_rate_pct != null && (
            <div>
              <p className="text-muted-foreground text-xs">BCRA TNA</p>
              <p className="font-medium">{Number(signal.bcra_rate_pct).toFixed(1)}%</p>
            </div>
          )}
          {signal.usd_ars_official != null && (
            <div>
              <p className="text-muted-foreground text-xs">USD/ARS ofic.</p>
              <p className="font-medium">{Number(signal.usd_ars_official).toFixed(2)}</p>
            </div>
          )}
          {signal.fed_rate_pct != null && (
            <div>
              <p className="text-muted-foreground text-xs">Fed rate</p>
              <p className="font-medium">{Number(signal.fed_rate_pct).toFixed(2)}%</p>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

// ── Smart Money table ─────────────────────────────────────────────────────────

function SmartMoneyTable({ signals }: { signals: Record<string, unknown>[] }) {
  if (!signals.length) return (
    <Card>
      <CardContent className="pt-5">
        <p className="text-sm text-muted-foreground">Sin señales institucionales. Ejecutá: <code>iol engines smart-money run</code></p>
      </CardContent>
    </Card>
  );

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <Users className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium text-muted-foreground">Smart Money — Señales 13F ({signals.length})</span>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="px-4 py-2 text-left">Símbolo</th>
                <th className="px-4 py-2 text-left">Dirección</th>
                <th className="px-4 py-2 text-right">Convicción</th>
                <th className="px-4 py-2 text-left hidden md:table-cell">Agregado por</th>
                <th className="px-4 py-2 text-left hidden lg:table-cell">Fecha 13F</th>
              </tr>
            </thead>
            <tbody>
              {signals.map((s, i) => {
                const dir = String(s.net_institutional_direction ?? "");
                const dirColor = dir === "accumulate" ? "text-success" : dir === "distribute" ? "text-destructive" : "text-muted-foreground";
                const icon = dir === "accumulate" ? "▲" : dir === "distribute" ? "▼" : "─";
                return (
                  <tr key={i} className="border-b last:border-0 hover:bg-accent/40 transition-colors">
                    <td className="px-4 py-2 font-mono font-semibold">{String(s.symbol ?? "")}</td>
                    <td className={cn("px-4 py-2 capitalize", dirColor)}>{icon} {dir}</td>
                    <td className="px-4 py-2 text-right">{Number(s.conviction_score ?? 0).toFixed(0)}/100</td>
                    <td className="px-4 py-2 hidden md:table-cell text-xs text-muted-foreground">
                      {(s.top_holders_added as string[] ?? []).slice(0, 2).join(", ") || "—"}
                    </td>
                    <td className="px-4 py-2 hidden lg:table-cell text-xs text-muted-foreground">{String(s.latest_13f_date ?? "—")}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

// ── Signal Accuracy card ──────────────────────────────────────────────────────

const ENGINE_LABELS: Record<string, string> = {
  regime: "Régimen",
  macro: "Macro",
  smart_money: "Smart Money",
  strategy: "Estrategia",
};

function SignalAccuracyCard({ rows }: { rows: AccuracyRow[] }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <TrendingUp className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium text-muted-foreground">Precisión de Señales</span>
          <span className="text-xs text-muted-foreground ml-auto">últimos 90 días</span>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-xs text-muted-foreground">
              <th className="px-4 py-2 text-left">Motor</th>
              <th className="px-4 py-2 text-right">Hit Rate</th>
              <th className="px-4 py-2 text-right">Hits</th>
              <th className="px-4 py-2 text-right">Evaluadas</th>
              <th className="px-4 py-2 text-right">Pendientes</th>
              <th className="px-4 py-2 text-left hidden sm:table-cell">Última eval.</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const hr = r.hit_rate_pct;
              const hrColor =
                hr === null ? "text-muted-foreground" :
                hr >= 65 ? "text-success" :
                hr >= 50 ? "text-warning" : "text-destructive";
              return (
                <tr key={r.engine} className="border-b last:border-0">
                  <td className="px-4 py-2 font-medium capitalize">
                    {ENGINE_LABELS[r.engine] ?? r.engine}
                  </td>
                  <td className={cn("px-4 py-2 text-right font-mono font-semibold", hrColor)}>
                    {hr !== null ? `${hr.toFixed(1)}%` : "—"}
                  </td>
                  <td className="px-4 py-2 text-right text-muted-foreground">{r.hits}</td>
                  <td className="px-4 py-2 text-right text-muted-foreground">{r.evaluated}</td>
                  <td className="px-4 py-2 text-right text-muted-foreground">{r.pending}</td>
                  <td className="px-4 py-2 hidden sm:table-cell text-xs text-muted-foreground">
                    {r.last_eval_date ?? "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        <p className="px-4 pb-3 pt-1 text-xs text-muted-foreground">
          Verde ≥65% · Amarillo 50-65% · Rojo &lt;50% · Pendientes: aún no hay retornos suficientes
        </p>
      </CardContent>
    </Card>
  );
}

// ── Main view ─────────────────────────────────────────────────────────────────

export function EnginesView() {
  const { regime, macro, smartMoney, accuracy, loading, error, refresh, runAll, running } = useEngines();

  return (
    <main className="mx-auto max-w-screen-2xl px-4 sm:px-6 py-6 space-y-6">
      <div className="flex items-center justify-between">
        <SectionHeader
          title="Motores de Análisis"
          description="Señales de régimen, macro y smart money actualizadas"
        />
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
            <RefreshCw className={cn("h-4 w-4 mr-1.5", loading && "animate-spin")} />
            Recargar
          </Button>
          <Button size="sm" onClick={runAll} disabled={running}>
            {running ? "Ejecutando…" : "Ejecutar motores"}
          </Button>
        </div>
      </div>

      {error && <ErrorState message={error} />}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {loading ? (
          <>
            <Skeleton className="h-44 rounded-xl" />
            <Skeleton className="h-44 rounded-xl" />
          </>
        ) : (
          <>
            <RegimeCard signal={regime} />
            <MacroCard signal={macro} />
          </>
        )}
      </div>

      {loading ? (
        <Skeleton className="h-64 rounded-xl" />
      ) : (
        <SmartMoneyTable signals={smartMoney} />
      )}

      {loading ? (
        <Skeleton className="h-48 rounded-xl" />
      ) : (
        <SignalAccuracyCard rows={accuracy} />
      )}
    </main>
  );
}
