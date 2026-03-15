"use client";

import { useState } from "react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/ui/ErrorState";
import { SectionHeader } from "@/components/ui/SectionHeader";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { QualityStatusList } from "./QualityStatusList";
import { ReconciliationPanel } from "./ReconciliationPanel";
import { CashflowForm } from "./CashflowForm";
import { IncomeBreakdownPanel } from "./IncomeBreakdownPanel";
import { useQuality } from "@/hooks/useAdvisor";
import type { QualityRow } from "@/types/quality";

function QualityDetailPanel({ row }: { row: QualityRow }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center gap-2">
          <StatusBadge status={row.kind} label={row.kind.toUpperCase()} />
          <p className="text-sm font-semibold text-foreground">{row.label}</p>
        </div>
        <p className="text-sm text-muted-foreground">{row.value}</p>
      </CardHeader>
      <CardContent className="pt-0 space-y-3">
        <p className="text-xs text-foreground whitespace-pre-line">{row.detail}</p>
        {row.codes.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {row.codes.map((code) => (
              <span
                key={code}
                className="text-xs bg-muted/20 text-muted-foreground rounded-md px-2 py-0.5 font-mono"
              >
                {code}
              </span>
            ))}
          </div>
        )}
        {row.sources.length > 0 && (
          <div className="space-y-0.5">
            <p className="text-xs font-medium text-muted-foreground">Fuentes</p>
            {row.sources.map((src) => (
              <p key={src} className="text-xs text-muted-foreground/70">{src}</p>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export function QualityView() {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const { data, isLoading, isError, refetch } = useQuality();

  const selectedRow = data?.rows.find((r) => r.id === selectedId) ?? null;

  if (isError) {
    return <ErrorState message="No se pudo cargar la calidad de datos" retry={() => void refetch()} />;
  }

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Calidad"
        description="Estado de los datos y métricas del sistema"
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Status list */}
        <div className="space-y-3">
          <p className="text-sm font-medium text-foreground">Indicadores</p>
          {isLoading ? (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <Skeleton key={i} className="h-14 w-full" />
              ))}
            </div>
          ) : (
            <QualityStatusList
              rows={data?.rows ?? []}
              onSelect={setSelectedId}
              selectedId={selectedId}
            />
          )}
        </div>

        {/* Detail panel */}
        <div>
          {selectedRow ? (
            <QualityDetailPanel row={selectedRow} />
          ) : (
            <Card className="flex items-center justify-center h-full min-h-[200px]">
              <p className="text-sm text-muted-foreground">
                Seleccioná un indicador para ver detalles
              </p>
            </Card>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <IncomeBreakdownPanel />
        <ReconciliationPanel />
      </div>
      <CashflowForm />
    </div>
  );
}
