"use client";

import { useCallback, useEffect, useState } from "react";

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

async function fetchJSON<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export function useSimulate() {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [compareResult, setCompareResult] = useState<unknown>(null);
  const [comparing, setComparing] = useState(false);

  const loadRuns = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchJSON<{ runs: RunSummary[] }>("/api/simulation/runs?limit=50");
      setRuns(data.runs ?? []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error cargando simulaciones");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadRuns(); }, [loadRuns]);

  const startRun = useCallback(async (params: RunParams) => {
    const qs = new URLSearchParams({
      bot_config: params.bot_config,
      date_from: params.date_from,
      date_to: params.date_to,
      initial_cash_ars: String(params.initial_cash_ars),
    });
    try {
      await fetchJSON(`/api/simulation/run?${qs}`, { method: "POST" });
      // Reload list after a short delay to pick up the new row
      setTimeout(() => loadRuns(), 500);
      // Poll again after the backtest likely finishes
      setTimeout(() => loadRuns(), 10000);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error iniciando simulación");
    }
  }, [loadRuns]);

  const compare = useCallback(async (ids: number[]) => {
    setComparing(true);
    setCompareResult(null);
    try {
      const result = await fetchJSON(`/api/simulation/compare?run_ids=${ids.join(",")}`);
      setCompareResult(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Error comparando runs");
    } finally {
      setComparing(false);
    }
  }, []);

  const loadRunDetail = useCallback(async (id: number) => {
    return fetchJSON<Record<string, unknown>>(`/api/simulation/runs/${id}`);
  }, []);

  return { runs, loading, error, startRun, compare, compareResult, comparing, loadRunDetail };
}
