import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  applyReconciliation,
  dismissReconciliation,
  getAdvisorHistory,
  getAdvisorLatest,
  getAdvisorOpportunities,
  getQuality,
  getReconciliationLatest,
  getReconciliationOpen,
} from "@/lib/api";
import type { AdvisorCadence } from "@/types/advisor";

export function useAdvisorLatest(cadence: AdvisorCadence = "daily") {
  return useQuery({
    queryKey: ["advisor", "latest", cadence],
    queryFn: () => getAdvisorLatest(cadence),
    refetchInterval: 10 * 60 * 1000,
  });
}

export function useAdvisorHistory(params?: {
  cadence?: AdvisorCadence;
  limit?: number;
}) {
  return useQuery({
    queryKey: ["advisor", "history", params],
    queryFn: () => getAdvisorHistory(params),
  });
}

export function useAdvisorOpportunities() {
  return useQuery({
    queryKey: ["advisor", "opportunities"],
    queryFn: getAdvisorOpportunities,
  });
}

export function useQuality() {
  return useQuery({
    queryKey: ["quality"],
    queryFn: getQuality,
    refetchInterval: 5 * 60 * 1000,
  });
}

export function useReconciliationOpen() {
  return useQuery({
    queryKey: ["reconciliation", "open"],
    queryFn: getReconciliationOpen,
  });
}

export function useReconciliationLatest() {
  return useQuery({
    queryKey: ["reconciliation", "latest"],
    queryFn: getReconciliationLatest,
  });
}

export function useReconciliationMutations() {
  const qc = useQueryClient();
  const invalidate = () =>
    qc.invalidateQueries({ queryKey: ["reconciliation"] });

  const apply = useMutation({
    mutationFn: ({ id, note }: { id: number; note?: string }) =>
      applyReconciliation(id, note),
    onSuccess: invalidate,
  });

  const dismiss = useMutation({
    mutationFn: ({ id, reason }: { id: number; reason: string }) =>
      dismissReconciliation(id, reason),
    onSuccess: invalidate,
  });

  return { apply, dismiss };
}
