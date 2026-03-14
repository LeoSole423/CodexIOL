import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  getAllocation,
  getAssetPerformance,
  getLatest,
  getMovers,
} from "@/lib/api";
import type { AllocationGroupBy, AssetPeriod } from "@/types/portfolio";

export function useLatest() {
  return useQuery({
    queryKey: ["latest"],
    queryFn: getLatest,
    refetchInterval: 5 * 60 * 1000,
  });
}

export function useAllocation(params: {
  group_by?: AllocationGroupBy;
  include_cash?: 0 | 1;
}) {
  return useQuery({
    queryKey: ["allocation", params],
    queryFn: () => getAllocation(params),
  });
}

export function useAssetPerformance(params: {
  period: AssetPeriod;
  month?: number;
  year?: number;
}) {
  return useQuery({
    queryKey: ["assetPerformance", params],
    queryFn: () => getAssetPerformance(params),
  });
}

export function useMovers(params?: {
  kind?: string;
  limit?: number;
  metric?: string;
}) {
  return useQuery({
    queryKey: ["movers", params],
    queryFn: () => getMovers(params),
  });
}

export function useManualCashflows(params?: { from?: string; to?: string }) {
  return useQuery({
    queryKey: ["cashflows", "manual", params],
    queryFn: async () => {
      const { getCashflowsManual } = await import("@/lib/api");
      return getCashflowsManual(params);
    },
  });
}

export function useCashflowMutations() {
  const qc = useQueryClient();

  const add = useMutation({
    mutationFn: async (data: Parameters<typeof import("@/lib/api").addManualCashflow>[0]) => {
      const { addManualCashflow } = await import("@/lib/api");
      return addManualCashflow(data);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cashflows"] }),
  });

  const remove = useMutation({
    mutationFn: async (id: number) => {
      const { deleteManualCashflow } = await import("@/lib/api");
      return deleteManualCashflow(id);
    },
    onSuccess: () => qc.invalidateQueries({ queryKey: ["cashflows"] }),
  });

  return { add, remove };
}
