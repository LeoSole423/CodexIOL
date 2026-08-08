import { useQuery } from "@tanstack/react-query";
import { getReturns, getSnapshots } from "@/lib/api";

export function useReturns() {
  return useQuery({
    queryKey: ["returns"],
    queryFn: getReturns,
    refetchInterval: 5 * 60 * 1000,
  });
}

export function useSnapshots(params?: {
  from?: string;
  to?: string;
  mode?: "raw" | "market";
}) {
  return useQuery({
    queryKey: ["snapshots", params],
    queryFn: () => getSnapshots(params),
  });
}
