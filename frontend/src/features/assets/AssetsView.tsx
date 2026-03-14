"use client";

import { SectionHeader } from "@/components/ui/SectionHeader";
import { ErrorState } from "@/components/ui/ErrorState";
import { AssetTable } from "./AssetTable";
import { useLatest } from "@/hooks/usePortfolio";

export function AssetsView() {
  const { data, isLoading, isError, refetch } = useLatest();

  if (isError) {
    return <ErrorState message="No se pudo cargar el listado de activos." retry={() => void refetch()} />;
  }

  return (
    <div className="space-y-6">
      <SectionHeader
        title="Activos"
        description="Composición actual del portfolio según último snapshot"
      />
      <AssetTable
        assets={data?.assets ?? []}
        loading={isLoading}
        snapshotDate={data?.snapshot?.snapshot_date ?? null}
      />
    </div>
  );
}
