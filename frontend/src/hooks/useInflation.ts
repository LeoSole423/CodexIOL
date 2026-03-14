import { useQuery } from "@tanstack/react-query";
import {
  getInflationAnnual,
  getInflationSeriesComparison,
  getKpiMonthlyVsInflation,
} from "@/lib/api";

export function useKpiMonthlyVsInflation() {
  return useQuery({
    queryKey: ["kpi", "monthly-vs-inflation"],
    queryFn: getKpiMonthlyVsInflation,
    refetchInterval: 5 * 60 * 1000,
  });
}

export function useInflationSeriesComparison(params?: {
  from?: string;
  to?: string;
}) {
  return useQuery({
    queryKey: ["inflation", "series", params],
    queryFn: () => getInflationSeriesComparison(params),
  });
}

export function useInflationAnnual(years = 10) {
  return useQuery({
    queryKey: ["inflation", "annual", years],
    queryFn: () => getInflationAnnual(years),
  });
}
