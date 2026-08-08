"use client";

import { useCallback, useEffect, useState } from "react";

export type BotType = "daily" | "swing" | "event";

export type RunSummary = {
  id: number;
  bot_name: string;
  date_from: string;
  date_to: string;
  status?: string;
  initial_value_ars: number;
  final_value_ars: number | null;
  total_return_pct: number | null;
  sharpe_ratio: number | null;
  max_drawdown_pct: number | null;
  win_rate_pct?: number | null;
  total_trades?: number | null;
  avg_hold_days?: number | null;
  total_events_triggered?: number | null;
};

export type RunParams = {
  bot: string;
  date_from: string;
  date_to: string;
  initial_cash_ars: number;
};

async function fetchJSON<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

// Normalize swing/event row (uses `initial_cash` / `final_value`) to common shape
function normalize(row: Record<string, unknown>): RunSummary {
  return {
    id: row.id as number,
    bot_name: row.bot_name as string,
    date_from: row.date_from as string,
    date_to: row.date_to as string,
    status: (row.status ?? "done") as string,
    initial_value_ars: (row.initial_value_ars ?? row.initial_cash ?? 0) as number,
    final_value_ars: (row.final_value_ars ?? row.final_value ?? null) as number | null,
    total_return_pct: row.total_return_pct as number | null,
    sharpe_ratio: row.sharpe_ratio as number | null,
    max_drawdown_pct: row.max_drawdown_pct as number | null,
    win_rate_pct: row.win_rate_pct as number | null,
    total_trades: row.total_trades as number | null,
    avg_hold_days: row.avg_hold_days as number | null,
    total_events_triggered: row.total_events_triggered as number | null,
  };
}

export function useSimulate(botType: BotType) {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [compareResult, setCompareResult] = useState<unknown>(null);
  const [comparing, setComparing] = useState(false);

  const basePath =
    botType === "daily" ? "/api/simulation" : `/api/simulation/${botType}`;

  const loadRuns = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      if (botType === "daily") {
        const data = await fetchJSON<{ runs: Record<string, unknown>[] }>(
          `${basePath}/runs?limit=50`
        );
        setRuns((data.runs ?? []).map(normalize));
      } else {
        const data = await fetchJSON<{ runs: Record<string, unknown>[] }>(
          `${basePath}/runs?limit=50`
        );
        setRuns((data.runs ?? []).map(normalize));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error cargando simulaciones");
    } finally {
      setLoading(false);
    }
  }, [basePath, botType]);

  useEffect(() => {
    setRuns([]);
    setCompareResult(null);
    loadRuns();
  }, [loadRuns]);

  const startRun = useCallback(
    async (params: RunParams) => {
      const qs = new URLSearchParams({
        date_from: params.date_from,
        date_to: params.date_to,
        initial_cash_ars: String(params.initial_cash_ars),
        ...(botType === "daily"
          ? { bot_config: params.bot }
          : { bot: params.bot }),
      });
      try {
        await fetchJSON(`${basePath}/run?${qs}`, { method: "POST" });
        setTimeout(() => loadRuns(), 500);
        setTimeout(() => loadRuns(), 10000);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Error iniciando simulación");
      }
    },
    [basePath, botType, loadRuns]
  );

  const compare = useCallback(
    async (ids: number[]) => {
      setComparing(true);
      setCompareResult(null);
      try {
        const path =
          botType === "daily"
            ? `/api/simulation/compare?run_ids=${ids.join(",")}`
            : `${basePath}/compare?run_ids=${ids.join(",")}`;
        const result = await fetchJSON(path);
        setCompareResult(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Error comparando runs");
      } finally {
        setComparing(false);
      }
    },
    [basePath, botType]
  );

  const loadRunDetail = useCallback(
    async (id: number) => {
      return fetchJSON<Record<string, unknown>>(`${basePath}/runs/${id}`);
    },
    [basePath]
  );

  return { runs, loading, error, startRun, compare, compareResult, comparing, loadRunDetail };
}
