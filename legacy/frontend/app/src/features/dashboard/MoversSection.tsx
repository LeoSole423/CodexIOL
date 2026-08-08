"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { DeltaBadge } from "@/components/ui/DeltaBadge";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/EmptyState";
import { fmtARS } from "@/lib/formatters";
import { useMovers } from "@/hooks/usePortfolio";

export function MoversSection() {
  const [tab, setTab] = useState<"gainers" | "losers">("gainers");
  const { data, isLoading } = useMovers({ kind: "daily", limit: 5 });

  const items = tab === "gainers" ? (data?.gainers ?? []) : (data?.losers ?? []);

  return (
    <Card>
      <CardHeader className="pb-3">
        <Tabs value={tab} onValueChange={(v) => setTab(v as "gainers" | "losers")}>
          <TabsList>
            <TabsTrigger value="gainers">Ganadores</TabsTrigger>
            <TabsTrigger value="losers">Perdedores</TabsTrigger>
          </TabsList>
        </Tabs>
      </CardHeader>
      <CardContent className="pt-0">
        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : items.length === 0 ? (
          <EmptyState message="Sin datos" />
        ) : (
          <div className="space-y-1">
            {items.map((asset) => (
              <div
                key={asset.symbol}
                className="flex items-center gap-3 py-2 px-2 rounded-md hover:bg-muted/10 transition-colors"
              >
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-semibold font-mono text-foreground">{asset.symbol}</p>
                  <p className="text-xs text-muted-foreground truncate">{asset.description}</p>
                </div>
                <div className="text-right shrink-0">
                  <DeltaBadge value={asset.daily_var_pct} format="pct" />
                  <p className="text-xs text-muted-foreground mt-0.5">{fmtARS(asset.total_value)}</p>
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
