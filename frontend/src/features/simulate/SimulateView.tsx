"use client";

import React, { useEffect, useState } from "react";
import { Play, List, GitCompare, TrendingUp } from "lucide-react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { SectionHeader } from "@/components/ui/SectionHeader";
import { ErrorState } from "@/components/ui/ErrorState";
import { EmptyState } from "@/components/ui/EmptyState";
import { useSimulate } from "./useSimulate";
import { cn } from "@/lib/utils";

// ── Run form ──────────────────────────────────────────────────────────────────

const BOT_PRESETS = [
  { value: "conservative", label: "Conservative", description: "Alta preservación, influencia plena del régimen" },
  { value: "balanced", label: "Balanced", description: "Equilibrado — preset por defecto" },
  { value: "growth", label: "Growth", description: "Alto momentum, posiciones grandes" },
];

function RunForm({ onRun }: { onRun: (params: RunParams) => void }) {
  const [bot, setBot] = useState("balanced");
  const [dateFrom, setDateFrom] = useState("2024-01-01");
  const [dateTo, setDateTo] = useState("2024-12-31");
  const [cash, setCash] = useState("1000000");

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    onRun({ bot_config: bot, date_from: dateFrom, date_to: dateTo, initial_cash_ars: Number(cash) });
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
          {/* Bot selector */}
          <div>
            <label className="text-xs text-muted-foreground font-medium mb-2 block">Bot preset</label>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
              {BOT_PRESETS.map((p) => (
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

          {/* Date range */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-xs text-muted-foreground font-medium mb-1 block">Desde</label>
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm"
                required
              />
            </div>
            <div>
              <label className="text-xs text-muted-foreground font-medium mb-1 block">Hasta</label>
              <input
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm"
                required
              />
            </div>
          </div>

          {/* Initial cash */}
          <div>
            <label className="text-xs text-muted-foreground font-medium mb-1 block">Capital inicial (ARS)</label>
            <input
              type="number"
              value={cash}
              onChange={(e) => setCash(e.target.value)}
              min={10000}
              step={10000}
              className="w-full rounded-md border border-input bg-background px-3 py-1.5 text-sm"
              required
            />
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

type RunSummary = {
  id: number;
  bot_name: string;
  date_from: string;
  date_to: string;
  status: string;
  initial_value_ars: number;
  final_value_ars: number | null;
  total_return_pct: number | null;
  sharpe_ratio: number | null;
  max_drawdown_pct: number | null;
};

type RunParams = {
  bot_config: string;
  date_from: string;
  date_to: string;
  initial_cash_ars: number;
};

function RunRow({
  run,
  selected,
  viewing,
  onToggle,
  onView,
}: {
  run: RunSummary;
  selected: boolean;
  viewing: boolean;
  onToggle: () => void;
  onView: () => void;
}) {
  const ret = run.total_return_pct;
  const retColor = ret == null ? "" : ret >= 0 ? "text-success" : "text-destructive";
  const statusVariant: "success" | "secondary" | "destructive" | "warning" =
    run.status === "done" ? "success" :
    run.status === "error" ? "destructive" :
    "secondary";

  return (
    <tr
      className={cn(
        "border-b last:border-0 cursor-pointer hover:bg-accent/40 transition-colors",
        viewing && "bg-primary/5"
      )}
      onClick={onView}
    >
      <td className="px-3 py-2">
        <input
          type="checkbox"
          checked={selected}
          onChange={onToggle}
          onClick={(e) => e.stopPropagation()}
        />
      </td>
      <td className="px-3 py-2 text-sm text-muted-foreground">#{run.id}</td>
      <td className="px-3 py-2 text-sm font-medium capitalize">{run.bot_name}</td>
      <td className="px-3 py-2 text-sm text-muted-foreground">{run.date_from} → {run.date_to}</td>
      <td className="px-3 py-2"><Badge variant={statusVariant}>{run.status}</Badge></td>
      <td className={cn("px-3 py-2 text-sm text-right font-medium", retColor)}>
        {ret != null ? `${ret >= 0 ? "+" : ""}${ret.toFixed(1)}%` : "—"}
      </td>
      <td className="px-3 py-2 text-sm text-right text-muted-foreground">
        {run.sharpe_ratio != null ? run.sharpe_ratio.toFixed(2) : "—"}
      </td>
      <td className="px-3 py-2 text-sm text-right text-muted-foreground">
        {run.max_drawdown_pct != null ? `${run.max_drawdown_pct.toFixed(1)}%` : "—"}
      </td>
      <td className="px-3 py-2 text-sm text-right">
        {run.final_value_ars != null ? run.final_value_ars.toLocaleString("es-AR", { maximumFractionDigits: 0 }) : "—"}
      </td>
    </tr>
  );
}

function RunsList({
  runs,
  loading,
  selectedIds,
  viewId,
  onToggle,
  onView,
}: {
  runs: RunSummary[];
  loading: boolean;
  selectedIds: Set<number>;
  viewId: number | null;
  onToggle: (id: number) => void;
  onView: (id: number) => void;
}) {
  if (loading) return <Skeleton className="h-48 rounded-xl" />;
  if (!runs.length) return <EmptyState message="No hay simulaciones. Ejecutá una desde la pestaña Ejecutar." />;

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
                <th className="px-3 py-2 text-left">Período</th>
                <th className="px-3 py-2 text-left">Estado</th>
                <th className="px-3 py-2 text-right">Retorno</th>
                <th className="px-3 py-2 text-right">Sharpe</th>
                <th className="px-3 py-2 text-right">MaxDD</th>
                <th className="px-3 py-2 text-right">Final ARS</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((r) => (
                <RunRow
                  key={r.id}
                  run={r}
                  selected={selectedIds.has(r.id)}
                  viewing={viewId === r.id}
                  onToggle={() => onToggle(r.id)}
                  onView={() => onView(r.id)}
                />
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
  runId,
  loadRunDetail,
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
        const raw = (detail as Record<string, unknown>);
        let curve: CurvePoint[] = [];
        // metrics_json may be a string or already parsed
        const metricsRaw = raw.metrics_json ?? raw.metrics;
        const metrics: Record<string, unknown> =
          typeof metricsRaw === "string" ? JSON.parse(metricsRaw) : (metricsRaw as Record<string, unknown>) ?? {};
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
            <XAxis
              dataKey="date"
              tick={{ fontSize: 11 }}
              tickFormatter={(d: string) => d.slice(5)}
              stroke="hsl(var(--muted-foreground))"
            />
            <YAxis
              tick={{ fontSize: 11 }}
              tickFormatter={fmt}
              stroke="hsl(var(--muted-foreground))"
              width={80}
            />
            <Tooltip
              formatter={(v: number) => [fmt(v), "ARS"]}
              labelStyle={{ fontSize: 12 }}
              contentStyle={{ fontSize: 12 }}
            />
            <Line
              type="monotone"
              dataKey="value"
              stroke="hsl(var(--primary))"
              dot={false}
              strokeWidth={2}
            />
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
    run_id: number;
    bot_name: string;
    date_from: string;
    date_to: string;
    total_return_pct: number | null;
    sharpe_ratio: number | null;
    max_drawdown_pct: number | null;
    win_rate_pct: number | null;
    initial_value_ars: number;
    final_value_ars: number | null;
    n_days: number | null;
  }>;
};

function ComparePanel({ result }: { result: CompareResult }) {
  const metrics: Array<{ label: string; key: keyof CompareResult["runs"][0]; fmt: (v: unknown) => string }> = [
    { label: "Período", key: "date_from", fmt: (v) => String(v ?? "—") },
    { label: "Días", key: "n_days", fmt: (v) => v != null ? String(v) : "—" },
    { label: "Capital inicial", key: "initial_value_ars", fmt: (v) => v != null ? Number(v).toLocaleString("es-AR", { maximumFractionDigits: 0 }) : "—" },
    { label: "Capital final", key: "final_value_ars", fmt: (v) => v != null ? Number(v).toLocaleString("es-AR", { maximumFractionDigits: 0 }) : "—" },
    { label: "Retorno total", key: "total_return_pct", fmt: (v) => v != null ? `${Number(v) >= 0 ? "+" : ""}${Number(v).toFixed(1)}%` : "—" },
    { label: "Sharpe ratio", key: "sharpe_ratio", fmt: (v) => v != null ? Number(v).toFixed(3) : "—" },
    { label: "Max drawdown", key: "max_drawdown_pct", fmt: (v) => v != null ? `${Number(v).toFixed(1)}%` : "—" },
    { label: "Win rate", key: "win_rate_pct", fmt: (v) => v != null ? `${Number(v).toFixed(1)}%` : "—" },
  ];

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center gap-2">
          <GitCompare className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Comparación de runs</span>
          {result.winner && (
            <Badge variant="success" className="ml-auto">Ganador: {result.winner}</Badge>
          )}
        </div>
      </CardHeader>
      <CardContent className="p-0">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="px-4 py-2 text-left">Métrica</th>
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
                    <td key={r.run_id} className="px-4 py-2 text-right font-medium">
                      {m.fmt(r[m.key])}
                    </td>
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

// ── Main view ─────────────────────────────────────────────────────────────────

export function SimulateView() {
  const { runs, loading, error, startRun, compare, compareResult, comparing, loadRunDetail } = useSimulate();
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [viewRunId, setViewRunId] = useState<number | null>(null);
  const [tab, setTab] = useState("run");

  function toggleId(id: number) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
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
      <div className="flex items-center justify-between">
        <SectionHeader
          title="Simulación / Backtest"
          description="Paper trading con datos históricos — sin tocar el portafolio real"
        />
        {selectedIds.size >= 2 && (
          <Button size="sm" onClick={handleCompare} disabled={comparing}>
            <GitCompare className="h-4 w-4 mr-1.5" />
            Comparar {selectedIds.size} runs
          </Button>
        )}
      </div>

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
          {compareResult && (
            <TabsTrigger value="compare">
              <GitCompare className="h-4 w-4 mr-1.5" />
              Comparar
            </TabsTrigger>
          )}
        </TabsList>

        <TabsContent value="run" className="mt-4">
          <RunForm onRun={handleRun} />
        </TabsContent>

        <TabsContent value="list" className="mt-4 space-y-3">
          {selectedIds.size >= 2 && (
            <p className="text-sm text-muted-foreground">
              {selectedIds.size} runs seleccionados — hacé clic en "Comparar" para ver el análisis.
            </p>
          )}
          <RunsList
            runs={runs}
            loading={loading}
            selectedIds={selectedIds}
            viewId={viewRunId}
            onToggle={toggleId}
            onView={handleView}
          />
          {viewRunId != null && (
            <EquityCurvePanel runId={viewRunId} loadRunDetail={loadRunDetail} />
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
