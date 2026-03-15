"use client";

import React, { useEffect, useState } from "react";
import { Play, List, GitCompare, TrendingUp, Zap, BarChart2, Calendar } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { SectionHeader } from "@/components/ui/SectionHeader";
import { ErrorState } from "@/components/ui/ErrorState";
import { EmptyState } from "@/components/ui/EmptyState";
import { BotType, RunParams, RunSummary, useSimulate } from "./useSimulate";
import { cn } from "@/lib/utils";

// ── Bot presets per type ───────────────────────────────────────────────────────

const PRESETS: Record<BotType, { value: string; label: string; description: string }[]> = {
  daily: [
    { value: "conservative", label: "Conservative", description: "Alta preservación, influencia plena del régimen" },
    { value: "balanced",     label: "Balanced",     description: "Equilibrado — preset por defecto" },
    { value: "growth",       label: "Growth",       description: "Alto momentum, posiciones grandes" },
  ],
  swing: [
    { value: "swing-conservative", label: "Conservative", description: "Hold 5-10d · Stop 3% · Score ≥ 60" },
    { value: "swing-balanced",     label: "Balanced",     description: "Hold 3-7d · Stop 4% · Score ≥ 50" },
    { value: "swing-aggressive",   label: "Aggressive",   description: "Hold 3-5d · Stop 5% · Score ≥ 40" },
  ],
  event: [
    { value: "event-defensive",     label: "Defensive",     description: "Reduce exposición en señales negativas · 5d cooldown" },
    { value: "event-opportunistic", label: "Opportunistic", description: "Compra agresivo en señales positivas · 3d cooldown" },
    { value: "event-adaptive",      label: "Adaptive",      description: "Respuesta balanceada a todos los eventos · 2d cooldown" },
  ],
};

// ── Run form ──────────────────────────────────────────────────────────────────

function RunForm({ botType, onRun }: { botType: BotType; onRun: (p: RunParams) => void }) {
  const presets = PRESETS[botType];
  const [bot, setBot] = useState(presets[1].value);
  const [dateFrom, setDateFrom] = useState("2025-01-01");
  const [dateTo, setDateTo] = useState("2026-03-15");
  const [cash, setCash] = useState("1000000");

  // Reset selected bot when type changes
  useEffect(() => { setBot(PRESETS[botType][1].value); }, [botType]);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    onRun({ bot, date_from: dateFrom, date_to: dateTo, initial_cash_ars: Number(cash) });
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <Play className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Nueva simulación</span>
        </div>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="text-xs text-muted-foreground font-medium mb-2 block">Bot preset</label>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
              {presets.map((p) => (
                <button
                  key={p.value}
                  type="button"
                  onClick={() => setBot(p.value)}
                  className={cn(
                    "text-left p-3 rounded-lg border text-sm transition-colors",
                    bot === p.value
                      ? "border-primary bg-primary/5 text-foreground"
                      : "border-border text-muted-foreground hover:border-border/80 hover:bg-accent"
                  )}
                >
                  <p className="font-semibold">{p.label}</p>
                  <p className="text-xs mt-0.5 text-muted-foreground">{p.description}</p>
                </button>
              ))}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-muted-foreground font-medium mb-1 block">Desde</label>
              <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)}
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm" required />
            </div>
            <div>
              <label className="text-xs text-muted-foreground font-medium mb-1 block">Hasta</label>
              <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)}
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm" required />
            </div>
          </div>

          <div>
            <label className="text-xs text-muted-foreground font-medium mb-1 block">Capital inicial (ARS)</label>
            <input type="number" value={cash} onChange={(e) => setCash(e.target.value)}
              min={10000} step={10000}
              className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm" required />
          </div>

          <Button type="submit" className="w-full sm:w-auto">
            <Play className="h-4 w-4 mr-1.5" />
            Ejecutar backtest
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

// ── Run list ──────────────────────────────────────────────────────────────────

function RunRow({
  run, botType, selected, viewing, onToggle, onView,
}: {
  run: RunSummary; botType: BotType;
  selected: boolean; viewing: boolean;
  onToggle: () => void; onView: () => void;
}) {
  const ret = run.total_return_pct;
  const retColor = ret == null ? "" : ret >= 0 ? "text-success" : "text-destructive";
  const isRunning = run.status === "running";
  const statusVariant: "success" | "secondary" | "destructive" =
    run.status === "done" ? "success" : run.status === "error" ? "destructive" : "secondary";
  const status = run.status ?? "done";

  return (
    <tr
      className={cn("border-b last:border-0 cursor-pointer hover:bg-accent/40 transition-colors", viewing && "bg-primary/5")}
      onClick={onView}
    >
      <td className="px-3 py-2">
        <input type="checkbox" checked={selected} onChange={onToggle} onClick={(e) => e.stopPropagation()} />
      </td>
      <td className="px-3 py-2 text-sm text-muted-foreground">#{run.id}</td>
      <td className="px-3 py-2 text-sm font-medium capitalize">{run.bot_name}</td>
      <td className="px-3 py-2 text-sm text-muted-foreground">{run.date_from} to {run.date_to}</td>
      <td className="px-3 py-2">
        {isRunning ? (
          <span className="flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse" />
            <span className="text-xs text-emerald-500 font-medium">running</span>
          </span>
        ) : (
          <Badge variant={statusVariant}>{status}</Badge>
        )}
      </td>
      <td className={cn("px-3 py-2 text-sm text-right font-medium", retColor)}>
        {ret != null ? `${ret >= 0 ? "+" : ""}${ret.toFixed(1)}%` : "-"}
      </td>
      <td className="px-3 py-2 text-sm text-right text-muted-foreground">
        {run.sharpe_ratio != null ? run.sharpe_ratio.toFixed(2) : "-"}
      </td>
      <td className="px-3 py-2 text-sm text-right text-muted-foreground">
        {run.max_drawdown_pct != null ? `${run.max_drawdown_pct.toFixed(1)}%` : "-"}
      </td>
      {botType === "swing" && (
        <td className="px-3 py-2 text-sm text-right text-muted-foreground">
          {run.avg_hold_days != null ? `${run.avg_hold_days.toFixed(1)}d` : "-"}
        </td>
      )}
      {botType === "event" && (
        <td className="px-3 py-2 text-sm text-right text-muted-foreground">
          {run.total_events_triggered ?? "-"}
        </td>
      )}
      <td className="px-3 py-2 text-sm text-right text-muted-foreground">
        {run.total_trades ?? "-"}
      </td>
      <td className="px-3 py-2 text-sm text-right">
        {run.final_value_ars != null
          ? run.final_value_ars.toLocaleString("es-AR", { maximumFractionDigits: 0 })
          : "-"}
      </td>
    </tr>
  );
}

function RunsList({
  runs, botType, loading, selectedIds, viewId, onToggle, onView,
}: {
  runs: RunSummary[]; botType: BotType; loading: boolean;
  selectedIds: Set<number>; viewId: number | null;
  onToggle: (id: number) => void; onView: (id: number) => void;
}) {
  if (loading) return <Skeleton className="h-48 rounded-xl" />;
  if (!runs.length) return <EmptyState message="No hay simulaciones. Ejecuta una desde la pestaña Ejecutar." />;

  return (
    <Card>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="px-3 py-2 w-8"></th>
                <th className="px-3 py-2 text-left">ID</th>
                <th className="px-3 py-2 text-left">Bot</th>
                <th className="px-3 py-2 text-left">Periodo</th>
                <th className="px-3 py-2 text-left">Estado</th>
                <th className="px-3 py-2 text-right">Retorno</th>
                <th className="px-3 py-2 text-right">Sharpe</th>
                <th className="px-3 py-2 text-right">MaxDD</th>
                {botType === "swing" && <th className="px-3 py-2 text-right">AvgHold</th>}
                {botType === "event" && <th className="px-3 py-2 text-right">Eventos</th>}
                <th className="px-3 py-2 text-right">Trades</th>
                <th className="px-3 py-2 text-right">Final ARS</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <RunRow key={r.id} run={r} botType={botType}
                  selected={selectedIds.has(r.id)} viewing={viewId === r.id}
                  onToggle={() => onToggle(r.id)} onView={() => onView(r.id)} />
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

// ── Equity curve ──────────────────────────────────────────────────────────────

type CurvePoint = { date: string; value: number };

function EquityCurvePanel({
  runId, loadRunDetail,
}: {
  runId: number;
  loadRunDetail: (id: number) => Promise<Record<string, unknown>>;
}) {
  const [data, setData] = useState<CurvePoint[]>([]);
  const [fetching, setFetching] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    setFetching(true);
    setFetchError(null);
    loadRunDetail(runId)
      .then((detail) => {
        let curve: CurvePoint[] = [];
        const metricsRaw = detail.metrics_json ?? detail.metrics;
        const metrics: Record<string, unknown> =
          typeof metricsRaw === "string"
            ? JSON.parse(metricsRaw)
            : (metricsRaw as Record<string, unknown>) ?? {};
        const ec = metrics.equity_curve;
        if (Array.isArray(ec)) {
          curve = (ec as Array<[string, number]>).map(([d, v]) => ({ date: d, value: v }));
        }
        setData(curve);
      })
      .catch((err) => setFetchError(err instanceof Error ? err.message : "Error cargando curva"))
      .finally(() => setFetching(false));
  }, [runId, loadRunDetail]);

  if (fetching) return <Skeleton className="h-48 rounded-xl" />;
  if (fetchError) return <ErrorState message={fetchError} />;
  if (!data.length) return (
    <Card>
      <CardContent className="py-8 text-center text-sm text-muted-foreground">
        Sin datos de curva para este run.
      </CardContent>
    </Card>
  );

  const fmt = (v: number) => v.toLocaleString("es-AR", { maximumFractionDigits: 0 });

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <TrendingUp className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Equity curve — Run #{runId}</span>
        </div>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={data} margin={{ top: 4, right: 8, bottom: 4, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis dataKey="date" tick={{ fontSize: 11 }}
              tickFormatter={(d: string) => d.slice(5)} stroke="hsl(var(--muted-foreground))" />
            <YAxis tick={{ fontSize: 11 }} tickFormatter={fmt}
              stroke="hsl(var(--muted-foreground))" width={80} />
            <Tooltip formatter={(v: number) => [fmt(v), "ARS"]}
              labelStyle={{ fontSize: 12 }} contentStyle={{ fontSize: 12 }} />
            <Line type="monotone" dataKey="value" stroke="hsl(var(--primary))"
              dot={false} strokeWidth={2} />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}

// ── Compare view ──────────────────────────────────────────────────────────────

type CompareResult = {
  winner: string | null;
  runs: Array<{
    run_id: number; bot_name: string; date_from: string; date_to: string;
    total_return_pct: number | null; sharpe_ratio: number | null;
    max_drawdown_pct: number | null; win_rate_pct: number | null;
    initial_value_ars: number; final_value_ars: number | null; n_days: number | null;
  }>;
};

function ComparePanel({ result }: { result: CompareResult }) {
  const metrics: Array<{ label: string; key: keyof CompareResult["runs"][0]; fmt: (v: unknown) => string }> = [
    { label: "Periodo", key: "date_from", fmt: (v) => String(v ?? "-") },
    { label: "Dias", key: "n_days", fmt: (v) => v != null ? String(v) : "-" },
    { label: "Capital inicial", key: "initial_value_ars", fmt: (v) => v != null ? Number(v).toLocaleString("es-AR", { maximumFractionDigits: 0 }) : "-" },
    { label: "Capital final", key: "final_value_ars", fmt: (v) => v != null ? Number(v).toLocaleString("es-AR", { maximumFractionDigits: 0 }) : "-" },
    { label: "Retorno total", key: "total_return_pct", fmt: (v) => v != null ? `${Number(v) >= 0 ? "+" : ""}${Number(v).toFixed(1)}%` : "-" },
    { label: "Sharpe ratio", key: "sharpe_ratio", fmt: (v) => v != null ? Number(v).toFixed(3) : "-" },
    { label: "Max drawdown", key: "max_drawdown_pct", fmt: (v) => v != null ? `${Number(v).toFixed(1)}%` : "-" },
    { label: "Win rate", key: "win_rate_pct", fmt: (v) => v != null ? `${Number(v).toFixed(1)}%` : "-" },
  ];

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <GitCompare className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Comparacion de runs</span>
          {result.winner && <Badge variant="success" className="ml-auto">Ganador: {result.winner}</Badge>}
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="px-4 py-2 text-left">Metrica</th>
                {result.runs.map((r) => (
                  <th key={r.run_id} className="px-4 py-2 text-right capitalize">
                    {r.bot_name} <span className="text-muted-foreground">(#{r.run_id})</span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {metrics.map((m) => (
                <tr key={m.label} className="border-b last:border-0">
                  <td className="px-4 py-2 text-muted-foreground">{m.label}</td>
                  {result.runs.map((r) => (
                    <td key={r.run_id} className="px-4 py-2 text-right font-medium">{m.fmt(r[m.key])}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}

// ── Plan panel (live runs) ────────────────────────────────────────────────────

type PlanEntry = { symbol: string; amount_ars: number; price?: number; reason?: string; score?: number };
type PlanExit  = { symbol: string; action?: string; reason?: string; pnl_ars?: number; return_pct?: number; trigger?: string };
type PlanJson  = {
  as_of?: string;
  regime?: string;
  regime_score?: number;
  macro_stress?: number;
  events_triggered?: number;
  entries?: PlanEntry[];
  exits?: PlanExit[];
  portfolio_value_ars?: number;
  open_positions?: string[];
};

function PlanPanel({
  runId, loadRunDetail,
}: {
  runId: number;
  loadRunDetail: (id: number) => Promise<Record<string, unknown>>;
}) {
  const [plan, setPlan] = useState<PlanJson | null>(null);
  const [fetching, setFetching] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    setFetching(true);
    setFetchError(null);
    loadRunDetail(runId)
      .then((detail) => {
        const raw = detail.plan_json;
        if (!raw) { setPlan(null); return; }
        const p: PlanJson = typeof raw === "string" ? JSON.parse(raw) : raw as PlanJson;
        setPlan(p);
      })
      .catch((err) => setFetchError(err instanceof Error ? err.message : "Error"))
      .finally(() => setFetching(false));
  }, [runId, loadRunDetail]);

  if (fetching) return <Skeleton className="h-48 rounded-xl" />;
  if (fetchError) return <ErrorState message={fetchError} />;
  if (!plan) return (
    <Card>
      <CardContent className="py-8 text-center text-sm text-muted-foreground">
        Sin plan disponible aún — ejecuta un live-step primero.
      </CardContent>
    </Card>
  );

  const regimeColor = plan.regime === "bull" ? "text-success" : plan.regime === "bear" ? "text-destructive" : "text-muted-foreground";
  const stressColor = (plan.macro_stress ?? 50) > 65 ? "text-destructive" : (plan.macro_stress ?? 50) < 35 ? "text-success" : "text-muted-foreground";
  const fmtArs = (v: number) => v.toLocaleString("es-AR", { maximumFractionDigits: 0 });

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2 flex-wrap">
          <TrendingUp className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Plan del bot — Run #{runId}</span>
          {plan.as_of && <span className="text-xs text-muted-foreground ml-auto">al {plan.as_of}</span>}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Engine signals */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div className="rounded-lg border bg-background px-3 py-2">
            <p className="text-xs text-muted-foreground mb-0.5">Régimen</p>
            <p className={cn("text-sm font-semibold capitalize", regimeColor)}>{plan.regime ?? "-"}</p>
          </div>
          <div className="rounded-lg border bg-background px-3 py-2">
            <p className="text-xs text-muted-foreground mb-0.5">Score régimen</p>
            <p className="text-sm font-semibold">{plan.regime_score != null ? plan.regime_score.toFixed(1) : "-"}</p>
          </div>
          <div className="rounded-lg border bg-background px-3 py-2">
            <p className="text-xs text-muted-foreground mb-0.5">Stress macro</p>
            <p className={cn("text-sm font-semibold", stressColor)}>{plan.macro_stress != null ? plan.macro_stress.toFixed(1) : "-"}</p>
          </div>
          <div className="rounded-lg border bg-background px-3 py-2">
            <p className="text-xs text-muted-foreground mb-0.5">Valor portafolio</p>
            <p className="text-sm font-semibold">{plan.portfolio_value_ars != null ? `ARS ${fmtArs(plan.portfolio_value_ars)}` : "-"}</p>
          </div>
        </div>

        {/* Open positions */}
        {plan.open_positions && plan.open_positions.length > 0 && (
          <div>
            <p className="text-xs text-muted-foreground font-medium mb-1.5">Posiciones abiertas ({plan.open_positions.length})</p>
            <div className="flex flex-wrap gap-1.5">
              {plan.open_positions.map((sym) => (
                <Badge key={sym} variant="secondary" className="text-xs font-mono">{sym}</Badge>
              ))}
            </div>
          </div>
        )}

        {/* Entries */}
        {plan.entries && plan.entries.length > 0 && (
          <div>
            <p className="text-xs text-success font-medium mb-1.5">Entradas del último step ({plan.entries.length})</p>
            <div className="space-y-1">
              {plan.entries.map((e, i) => (
                <div key={i} className="flex items-center justify-between text-sm rounded-md border border-success/20 bg-success/5 px-3 py-1.5">
                  <span className="font-mono font-medium">{e.symbol}</span>
                  <span className="text-muted-foreground text-xs">{e.reason}</span>
                  <span className="text-success font-medium">+ARS {fmtArs(e.amount_ars)}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Exits */}
        {plan.exits && plan.exits.length > 0 && (
          <div>
            <p className="text-xs text-muted-foreground font-medium mb-1.5">Salidas del último step ({plan.exits.length})</p>
            <div className="space-y-1">
              {plan.exits.map((e, i) => {
                const isProfit = (e.pnl_ars ?? e.return_pct ?? 0) >= 0;
                return (
                  <div key={i} className="flex items-center justify-between text-sm rounded-md border px-3 py-1.5">
                    <span className="font-mono font-medium">{e.symbol}</span>
                    <span className="text-muted-foreground text-xs">{e.reason ?? e.trigger ?? e.action}</span>
                    {e.pnl_ars != null && (
                      <span className={cn("font-medium", isProfit ? "text-success" : "text-destructive")}>
                        {isProfit ? "+" : ""}{fmtArs(e.pnl_ars)}
                        {e.return_pct != null && ` (${isProfit ? "+" : ""}${e.return_pct.toFixed(1)}%)`}
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* No activity today */}
        {(!plan.entries || plan.entries.length === 0) && (!plan.exits || plan.exits.length === 0) && (
          <p className="text-sm text-muted-foreground text-center py-2">Sin actividad en el último step.</p>
        )}
      </CardContent>
    </Card>
  );
}

// ── Bot type selector ─────────────────────────────────────────────────────────

const BOT_TYPES: { value: BotType; label: string; icon: React.ReactNode; description: string }[] = [
  { value: "daily",  label: "Daily",  icon: <Calendar className="h-4 w-4" />, description: "Bots de rebalanceo diario" },
  { value: "swing",  label: "Swing",  icon: <BarChart2 className="h-4 w-4" />, description: "Posiciones de 3-10 dias con TA" },
  { value: "event",  label: "Event",  icon: <Zap className="h-4 w-4" />, description: "Reaccion a eventos del mercado" },
];

function BotTypeSelector({ value, onChange }: { value: BotType; onChange: (t: BotType) => void }) {
  return (
    <div className="flex gap-2 flex-wrap">
      {BOT_TYPES.map((t) => (
        <button
          key={t.value}
          onClick={() => onChange(t.value)}
          className={cn(
            "flex items-center gap-2 px-4 py-2 rounded-lg border text-sm font-medium transition-colors",
            value === t.value
              ? "border-primary bg-primary/10 text-foreground"
              : "border-border text-muted-foreground hover:bg-accent"
          )}
        >
          {t.icon}
          {t.label}
          <span className="hidden sm:inline text-xs font-normal text-muted-foreground">
            {t.description}
          </span>
        </button>
      ))}
    </div>
  );
}

// ── Main view ─────────────────────────────────────────────────────────────────

export function SimulateView() {
  const [botType, setBotType] = useState<BotType>("daily");
  const { runs, loading, error, startRun, compare, compareResult, comparing, loadRunDetail } =
    useSimulate(botType);
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [viewRunId, setViewRunId] = useState<number | null>(null);
  const [tab, setTab] = useState("run");

  function handleBotTypeChange(t: BotType) {
    setBotType(t);
    setSelectedIds(new Set());
    setViewRunId(null);
  }

  function toggleId(id: number) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }

  function handleView(id: number) {
    setViewRunId((prev) => (prev === id ? null : id));
  }

  async function handleRun(params: RunParams) {
    await startRun(params);
    setTab("list");
  }

  async function handleCompare() {
    if (selectedIds.size < 2) return;
    await compare([...selectedIds]);
    setTab("compare");
  }

  return (
    <main className="mx-auto max-w-screen-2xl px-4 sm:px-6 py-6 space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-3">
        <SectionHeader
          title="Simulacion / Backtest"
          description="Paper trading con datos historicos - sin tocar el portafolio real"
        />
        {selectedIds.size >= 2 && (
          <Button size="sm" onClick={handleCompare} disabled={comparing}>
            <GitCompare className="h-4 w-4 mr-1.5" />
            Comparar {selectedIds.size} runs
          </Button>
        )}
      </div>

      <BotTypeSelector value={botType} onChange={handleBotTypeChange} />

      {error && <ErrorState message={error} />}

      <Tabs value={tab} onValueChange={setTab}>
        <TabsList>
          <TabsTrigger value="run">
            <Play className="h-4 w-4 mr-1.5" />
            Ejecutar
          </TabsTrigger>
          <TabsTrigger value="list">
            <List className="h-4 w-4 mr-1.5" />
            Historial {runs.length > 0 && `(${runs.length})`}
          </TabsTrigger>
          {runs.some((r) => r.status === "running") && (
            <TabsTrigger value="live">
              <span className="h-2 w-2 rounded-full bg-emerald-500 animate-pulse mr-1.5" />
              Live ({runs.filter((r) => r.status === "running").length})
            </TabsTrigger>
          )}
          {compareResult != null && (
            <TabsTrigger value="compare">
              <GitCompare className="h-4 w-4 mr-1.5" />
              Comparar
            </TabsTrigger>
          )}
        </TabsList>

        <TabsContent value="run" className="mt-4">
          <RunForm botType={botType} onRun={handleRun} />
        </TabsContent>

        <TabsContent value="list" className="mt-4 space-y-3">
          {selectedIds.size >= 2 && (
            <p className="text-sm text-muted-foreground">
              {selectedIds.size} runs seleccionados - hace clic en "Comparar" para ver el analisis.
            </p>
          )}
          <RunsList runs={runs} botType={botType} loading={loading}
            selectedIds={selectedIds} viewId={viewRunId}
            onToggle={toggleId} onView={handleView} />
          {viewRunId != null && (
            <EquityCurvePanel runId={viewRunId} loadRunDetail={loadRunDetail} />
          )}
        </TabsContent>

        <TabsContent value="live" className="mt-4 space-y-3">
          <RunsList runs={runs.filter((r) => r.status === "running")} botType={botType} loading={loading}
            selectedIds={selectedIds} viewId={viewRunId}
            onToggle={toggleId} onView={handleView} />
          {viewRunId != null && (
            <PlanPanel runId={viewRunId} loadRunDetail={loadRunDetail} />
          )}
        </TabsContent>

        <TabsContent value="compare" className="mt-4">
          {comparing ? (
            <Skeleton className="h-64 rounded-xl" />
          ) : compareResult ? (
            <ComparePanel result={compareResult as CompareResult} />
          ) : null}
        </TabsContent>
      </Tabs>
    </main>
  );
}
