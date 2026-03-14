"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/ui/ErrorState";
import { SectionHeader } from "@/components/ui/SectionHeader";
import { PortfolioEvolutionChart } from "./PortfolioEvolutionChart";
import { InflationComparisonChart } from "./InflationComparisonChart";
import { AnnualReturnsChart } from "./AnnualReturnsChart";
import { useSnapshots } from "@/hooks/useReturns";
import { useInflationSeriesComparison, useInflationAnnual } from "@/hooks/useInflation";
import type { SnapshotPoint } from "@/types/returns";

function ChartCard({
  title,
  loading,
  children,
}: {
  title: string;
  loading?: boolean;
  children: React.ReactNode;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-semibold text-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        {loading ? (
          <Skeleton className="w-full h-64" />
        ) : (
          children
        )}
      </CardContent>
    </Card>
  );
}

export function HistoryView() {
  const snapshots = useSnapshots();
  const inflationSeries = useInflationSeriesComparison();
  const inflationAnnual = useInflationAnnual(10);

  if (snapshots.isError && inflationSeries.isError && inflationAnnual.isError) {
    return (
      <ErrorState
        message="No se pudo cargar el historial"
        retry={() => {
          void snapshots.refetch();
          void inflationSeries.refetch();
          void inflationAnnual.refetch();
        }}
      />
    );
  }

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Historia"
        description="Evolución del portfolio en el tiempo vs inflación"
      />

      <ChartCard title="Evolución del portfolio (ARS)" loading={snapshots.isLoading}>
        <PortfolioEvolutionChart
          data={(snapshots.data ?? []) as SnapshotPoint[]}
          height={300}
        />
      </ChartCard>

      <ChartCard
        title="Portfolio vs Inflación (base 100)"
        loading={inflationSeries.isLoading}
      >
        {inflationSeries.data ? (
          <>
            <InflationComparisonChart data={inflationSeries.data} height={280} />
            {inflationSeries.data.projection_used && (
              <p className="text-xs text-muted-foreground mt-2">
                * Inflación proyectada a partir de{" "}
                {inflationSeries.data.inflation_available_to ?? "último dato disponible"}
              </p>
            )}
          </>
        ) : !inflationSeries.isLoading ? (
          <p className="text-sm text-muted-foreground py-8 text-center">Sin datos de inflación</p>
        ) : null}
      </ChartCard>

      <ChartCard
        title="Retorno anual vs Inflación"
        loading={inflationAnnual.isLoading}
      >
        {inflationAnnual.data ? (
          <AnnualReturnsChart rows={inflationAnnual.data.rows} height={280} />
        ) : !inflationAnnual.isLoading ? (
          <p className="text-sm text-muted-foreground py-8 text-center">Sin datos anuales</p>
        ) : null}
      </ChartCard>
    </div>
  );
}
