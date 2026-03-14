"use client";

import { useState } from "react";
import { AlertTriangle, Clock, TrendingUp } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/ui/ErrorState";
import { EmptyState } from "@/components/ui/EmptyState";
import { SectionHeader } from "@/components/ui/SectionHeader";
import { Button } from "@/components/ui/button";
import { AdvisorItemCard } from "./AdvisorItemCard";
import { useAdvisorLatest, useAdvisorHistory } from "@/hooks/useAdvisor";
import { humanize, fmtRelativeTime } from "@/lib/formatters";
import { cn } from "@/lib/utils";
import type { AdvisorCadence, AdvisorBriefing } from "@/types/advisor";

function HeroCard({ briefing }: { briefing: AdvisorBriefing }) {
  const items = briefing.items ?? [];
  const buys = items.filter((i) =>
    ["buy", "comprar"].includes(String(i.action ?? "").toLowerCase())
  ).length;
  const sells = items.filter((i) =>
    ["sell", "vender"].includes(String(i.action ?? "").toLowerCase())
  ).length;
  const reviews = items.filter((i) =>
    ["review", "revisar"].includes(String(i.action ?? "").toLowerCase())
  ).length;

  const statusVariant =
    briefing.status === "actionable" || briefing.decision
      ? "success"
      : briefing.status === "blocked"
        ? "danger"
        : "warning";

  return (
    <Card>
      <CardContent className="pt-5">
        <div className="flex flex-col sm:flex-row sm:items-start gap-4">
          <div className="flex-1">
            {briefing.status && (
              <Badge variant={statusVariant} className="mb-2">
                {humanize(briefing.status)}
              </Badge>
            )}
            <h2 className="text-base font-semibold text-foreground">
              {briefing.headline ?? briefing.decision ?? "Sin recomendación"}
            </h2>
          </div>
          <div className="flex gap-4 text-center">
            {buys > 0 && (
              <div>
                <p className="text-lg font-bold text-success">{buys}</p>
                <p className="text-xs text-muted-foreground">compra{buys !== 1 ? "s" : ""}</p>
              </div>
            )}
            {sells > 0 && (
              <div>
                <p className="text-lg font-bold text-danger">{sells}</p>
                <p className="text-xs text-muted-foreground">venta{sells !== 1 ? "s" : ""}</p>
              </div>
            )}
            {reviews > 0 && (
              <div>
                <p className="text-lg font-bold text-warning">{reviews}</p>
                <p className="text-xs text-muted-foreground">revisión{reviews !== 1 ? "es" : ""}</p>
              </div>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function BriefingContent({ briefing }: { briefing: AdvisorBriefing }) {
  const items = briefing.items ?? [];
  const watchlist = briefing.watchlist ?? [];
  const blockers = briefing.blockers ?? [];

  return (
    <div className="space-y-4">
      <HeroCard briefing={briefing} />

      {/* Blockers */}
      {blockers.length > 0 && (
        <Card className="border-warning/30">
          <CardHeader className="pb-2">
            <div className="flex items-center gap-2 text-warning">
              <AlertTriangle className="h-4 w-4" />
              <p className="text-sm font-medium">Bloqueadores ({blockers.length})</p>
            </div>
          </CardHeader>
          <CardContent className="pt-0">
            <ul className="space-y-1">
              {blockers.map((b, i) => (
                <li key={i} className="text-xs text-muted-foreground">• {String(b)}</li>
              ))}
            </ul>
          </CardContent>
        </Card>
      )}

      {/* Recommendations */}
      {items.length > 0 && (
        <div className="space-y-2">
          <p className="text-sm font-medium text-foreground">
            Recomendaciones ({items.length})
          </p>
          {items.map((item, i) => (
            <AdvisorItemCard key={i} item={item} />
          ))}
        </div>
      )}

      {items.length === 0 && blockers.length === 0 && (
        <EmptyState
          message="Sin recomendaciones"
          description="No hay items en este briefing"
          icon={TrendingUp}
        />
      )}

      {/* Watchlist */}
      {watchlist.length > 0 && (
        <Collapsible>
          <CollapsibleTrigger asChild>
            <Button variant="outline" size="sm" className="w-full text-xs">
              Watchlist ({watchlist.length})
            </Button>
          </CollapsibleTrigger>
          <CollapsibleContent className="space-y-2 mt-2">
            {watchlist.map((item, i) => (
              <AdvisorItemCard key={i} item={item} />
            ))}
          </CollapsibleContent>
        </Collapsible>
      )}
    </div>
  );
}

function HistoryTable({ cadence }: { cadence: AdvisorCadence }) {
  const { data, isLoading } = useAdvisorHistory({ cadence, limit: 20 });
  const rows = data?.rows ?? [];

  if (isLoading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    );
  }

  if (rows.length === 0) {
    return <EmptyState message="Sin historial" />;
  }

  return (
    <div className="divide-y divide-border rounded-lg border border-border overflow-hidden">
      {rows.map((b, i) => (
        <div key={i} className="flex items-center gap-3 px-4 py-3 hover:bg-muted/5 transition-colors">
          <div className="flex-1 min-w-0">
            <p className="text-sm text-foreground truncate">
              {b.headline ?? b.decision ?? "Briefing"}
            </p>
            <p className="text-xs text-muted-foreground flex items-center gap-1 mt-0.5">
              <Clock className="h-3 w-3" />
              {fmtRelativeTime(b.created_at)}
            </p>
          </div>
          {b.status && (
            <Badge variant="outline" className="text-xs shrink-0">
              {humanize(b.status)}
            </Badge>
          )}
        </div>
      ))}
    </div>
  );
}

export function AdvisorView() {
  const [cadence, setCadence] = useState<AdvisorCadence>("daily");
  const { data, isLoading, isError, refetch } = useAdvisorLatest(cadence);

  if (isError) {
    return <ErrorState message="No se pudo cargar el asesor" retry={() => void refetch()} />;
  }

  const briefing = data?.briefing ?? null;

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Asesor"
        description="Recomendaciones y análisis de portfolio"
      />

      <Tabs
        value={cadence}
        onValueChange={(v) => setCadence(v as AdvisorCadence)}
      >
        <TabsList>
          <TabsTrigger value="daily">Diario</TabsTrigger>
          <TabsTrigger value="weekly">Semanal</TabsTrigger>
        </TabsList>

        <TabsContent value={cadence} className="mt-4">
          <Tabs defaultValue="briefing">
            <TabsList>
              <TabsTrigger value="briefing">Último briefing</TabsTrigger>
              <TabsTrigger value="history">Historial</TabsTrigger>
            </TabsList>

            <TabsContent value="briefing" className="mt-4">
              {isLoading ? (
                <div className="space-y-3">
                  <Skeleton className="h-32 w-full" />
                  <Skeleton className="h-24 w-full" />
                  <Skeleton className="h-24 w-full" />
                </div>
              ) : briefing ? (
                <BriefingContent briefing={briefing} />
              ) : (
                <EmptyState
                  message="Sin briefing disponible"
                  description={`No hay datos para cadencia ${cadence}`}
                  icon={TrendingUp}
                />
              )}
            </TabsContent>

            <TabsContent value="history" className="mt-4">
              <HistoryTable cadence={cadence} />
            </TabsContent>
          </Tabs>
        </TabsContent>
      </Tabs>
    </div>
  );
}
