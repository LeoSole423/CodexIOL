"use client";

import { useState, useMemo } from "react";
import { Search, ArrowUpDown, ArrowUp, ArrowDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import { DeltaBadge } from "@/components/ui/DeltaBadge";
import { EmptyState } from "@/components/ui/EmptyState";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { fmtARS, fmtPct, signClass } from "@/lib/formatters";
import type { Asset } from "@/types/portfolio";

type SortKey = "value" | "daily" | "gain";
type SortDir = "asc" | "desc";

function sortAssets(assets: Asset[], key: SortKey, dir: SortDir): Asset[] {
  return [...assets].sort((a, b) => {
    let va = 0, vb = 0;
    if (key === "value") { va = a.total_value; vb = b.total_value; }
    if (key === "daily") { va = a.daily_var_pct ?? 0; vb = b.daily_var_pct ?? 0; }
    if (key === "gain") { va = a.gain_amount ?? 0; vb = b.gain_amount ?? 0; }
    return dir === "desc" ? vb - va : va - vb;
  });
}

interface AssetTableProps {
  assets: Asset[];
  loading?: boolean;
  snapshotDate?: string | null;
}

export function AssetTable({ assets, loading, snapshotDate }: AssetTableProps) {
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("value");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const filtered = useMemo(() => {
    const q = query.toLowerCase().trim();
    const base = q
      ? assets.filter(
          (a) =>
            a.symbol.toLowerCase().includes(q) ||
            a.description.toLowerCase().includes(q) ||
            a.type.toLowerCase().includes(q) ||
            a.market.toLowerCase().includes(q)
        )
      : assets;
    return sortAssets(base, sortKey, sortDir);
  }, [assets, query, sortKey, sortDir]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  function SortIcon({ col }: { col: SortKey }) {
    if (sortKey !== col) return <ArrowUpDown className="h-3.5 w-3.5 opacity-40" />;
    return sortDir === "desc"
      ? <ArrowDown className="h-3.5 w-3.5" />
      : <ArrowUp className="h-3.5 w-3.5" />;
  }

  if (loading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-9 w-full max-w-xs" />
        <div className="rounded-lg border border-border overflow-hidden">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="flex items-center gap-4 p-3 border-b border-border last:border-0">
              <Skeleton className="h-4 w-16" />
              <Skeleton className="h-4 w-40 flex-1" />
              <Skeleton className="h-4 w-24" />
              <Skeleton className="h-4 w-16" />
              <Skeleton className="h-4 w-16" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Search + meta */}
      <div className="flex flex-col sm:flex-row sm:items-center gap-3">
        <div className="relative max-w-xs w-full">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
          <Input
            placeholder="Buscar por símbolo, tipo…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="pl-8"
          />
        </div>
        <span className="text-xs text-muted-foreground ml-auto">
          {filtered.length} activo{filtered.length !== 1 ? "s" : ""}
          {snapshotDate && (
            <> · <span className="font-mono">{snapshotDate}</span></>
          )}
        </span>
      </div>

      {/* Table */}
      <div className="rounded-lg border border-border overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/10">
              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted-foreground whitespace-nowrap">
                Símbolo
              </th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted-foreground w-full">
                Descripción
              </th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted-foreground whitespace-nowrap hidden sm:table-cell">
                Tipo / Mercado
              </th>
              <th className="text-right px-4 py-2.5 whitespace-nowrap">
                <button
                  className="inline-flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
                  onClick={() => toggleSort("value")}
                >
                  Valor <SortIcon col="value" />
                </button>
              </th>
              <th className="text-right px-4 py-2.5 whitespace-nowrap">
                <button
                  className="inline-flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
                  onClick={() => toggleSort("daily")}
                >
                  Var. día <SortIcon col="daily" />
                </button>
              </th>
              <th className="text-right px-4 py-2.5 whitespace-nowrap">
                <button
                  className="inline-flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
                  onClick={() => toggleSort("gain")}
                >
                  Ganancia <SortIcon col="gain" />
                </button>
              </th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={6}>
                  <EmptyState
                    message="Sin resultados"
                    description={query ? `No se encontraron activos para "${query}"` : "No hay activos en el portfolio"}
                  />
                </td>
              </tr>
            ) : (
              filtered.map((asset) => (
                <tr
                  key={asset.symbol}
                  className="border-b border-border last:border-0 hover:bg-muted/5 transition-colors"
                >
                  <td className="px-4 py-3 font-mono text-xs font-semibold text-foreground whitespace-nowrap">
                    {asset.symbol}
                  </td>
                  <td className="px-4 py-3 text-sm text-foreground">
                    <span className="line-clamp-1">{asset.description || "—"}</span>
                  </td>
                  <td className="px-4 py-3 text-xs text-muted-foreground whitespace-nowrap hidden sm:table-cell">
                    <span>{asset.type}</span>
                    {asset.market && (
                      <span className="ml-1 text-muted-foreground/60">· {asset.market}</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-right font-medium text-sm text-foreground whitespace-nowrap">
                    {fmtARS(asset.total_value)}
                  </td>
                  <td className="px-4 py-3 text-right whitespace-nowrap">
                    <span className={cn("text-xs font-medium", signClass(asset.daily_var_pct))}>
                      {asset.daily_var_pct != null ? fmtPct(asset.daily_var_pct) : "—"}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-right whitespace-nowrap">
                    <DeltaBadge value={asset.gain_amount} format="ars" />
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
