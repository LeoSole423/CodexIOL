"use client";

import { ErrorState } from "@/components/ui/ErrorState";
import { KpiStrip } from "./KpiStrip";
import { EvolutionSection } from "./EvolutionSection";
import { AllocationSection } from "./AllocationSection";
import { MoversSection } from "./MoversSection";
import { AssetPerformanceSection } from "./AssetPerformanceSection";
import { useLatest } from "@/hooks/usePortfolio";
import { useReturns } from "@/hooks/useReturns";
import { useKpiMonthlyVsInflation } from "@/hooks/useInflation";

export function DashboardView() {
  const latestQ = useLatest();
  const returnsQ = useReturns();
  const kpiQ = useKpiMonthlyVsInflation();

  const anyLoading = latestQ.isLoading || returnsQ.isLoading;
  const anyError = latestQ.isError && returnsQ.isError;

  if (anyError) {
    return (
      <ErrorState
        message="No se pudo cargar el dashboard"
        retry={() => {
          void latestQ.refetch();
          void returnsQ.refetch();
        }}
      />
    );
  }

  return (
    <div className="space-y-6">
      <KpiStrip
        latest={latestQ.data}
        returns={returnsQ.data}
        monthlyKpi={kpiQ.data}
        loading={anyLoading}
      />

      <EvolutionSection />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <AllocationSection />
        <MoversSection />
      </div>

      <AssetPerformanceSection />
    </div>
  );
}
