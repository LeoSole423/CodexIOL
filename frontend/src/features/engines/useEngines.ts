"use client";

import { useCallback, useEffect, useState } from "react";

export interface AccuracyRow {
  engine: string;
  hit_rate_pct: number | null;
  hits: number;
  evaluated: number;
  pending: number;
  total: number;
  last_eval_date: string | null;
}

interface EnginesState {
  regime: Record<string, unknown> | null;
  macro: Record<string, unknown> | null;
  smartMoney: Record<string, unknown>[];
  accuracy: AccuracyRow[];
  loading: boolean;
  error: string | null;
  running: boolean;
}

async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export function useEngines() {
  const [state, setState] = useState<EnginesState>({
    regime: null,
    macro: null,
    smartMoney: [],
    accuracy: [],
    loading: true,
    error: null,
    running: false,
  });

  const load = useCallback(async () => {
    setState((s) => ({ ...s, loading: true, error: null }));
    try {
      const [regimeRes, macroRes, smRes, accRes] = await Promise.all([
        fetchJSON<{ signal: Record<string, unknown> | null }>("/api/engines/regime"),
        fetchJSON<{ signal: Record<string, unknown> | null }>("/api/engines/macro"),
        fetchJSON<{ signals: Record<string, unknown>[] }>("/api/engines/smart-money"),
        fetchJSON<{ engines: AccuracyRow[] }>("/api/engines/accuracy?update=false"),
      ]);
      setState((s) => ({
        ...s,
        regime: regimeRes.signal,
        macro: macroRes.signal,
        smartMoney: smRes.signals ?? [],
        accuracy: accRes.engines ?? [],
        loading: false,
      }));
    } catch (err) {
      setState((s) => ({
        ...s,
        loading: false,
        error: err instanceof Error ? err.message : "Error cargando motores",
      }));
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const runAll = useCallback(async () => {
    setState((s) => ({ ...s, running: true }));
    try {
      await fetch("/api/engines/run-all", { method: "POST" });
      // Poll for results after 3s
      setTimeout(() => {
        load().finally(() => setState((s) => ({ ...s, running: false })));
      }, 3000);
    } catch {
      setState((s) => ({ ...s, running: false }));
    }
  }, [load]);

  return {
    ...state,
    refresh: load,
    runAll,
  };
}
