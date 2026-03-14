import type {
  AllocationGroupBy,
  AllocationItem,
  AssetPerformanceResponse,
  AssetPeriod,
  LatestResponse,
  MoversResponse,
} from "@/types/portfolio";
import type { ReturnsResponse, SnapshotPoint, SnapshotPointMarket } from "@/types/returns";
import type {
  CashflowsAutoResponse,
  ManualCashflow,
  ManualCashflowInput,
} from "@/types/cashflows";
import type {
  AdvisorBriefingResponse,
  AdvisorCadence,
  AdvisorHistoryResponse,
  OpportunityRunResponse,
} from "@/types/advisor";
import type { QualityResponse, ReconciliationLatestResponse, ReconciliationOpenResponse } from "@/types/quality";
import type { AnnualComparisonResponse, InflationSeriesComparison, MonthlyKPI } from "@/types/inflation";

async function fetchJSON<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...options?.headers },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "Unknown error");
    throw new Error(`API ${path}: ${res.status} ${text}`);
  }
  return res.json() as Promise<T>;
}

// ─── Portfolio ───────────────────────────────────────────────────────────────

export function getLatest(): Promise<LatestResponse> {
  return fetchJSON("/api/latest");
}

export function getAllocation(params: {
  group_by?: AllocationGroupBy;
  include_cash?: 0 | 1;
}): Promise<AllocationItem[]> {
  const q = new URLSearchParams();
  if (params.group_by) q.set("group_by", params.group_by);
  if (params.include_cash !== undefined)
    q.set("include_cash", String(params.include_cash));
  return fetchJSON(`/api/allocation?${q}`);
}

export function getAssetPerformance(params: {
  period: AssetPeriod;
  month?: number;
  year?: number;
}): Promise<AssetPerformanceResponse> {
  const q = new URLSearchParams({ period: params.period });
  if (params.month !== undefined) q.set("month", String(params.month));
  if (params.year !== undefined) q.set("year", String(params.year));
  return fetchJSON(`/api/assets/performance?${q}`);
}

export function getMovers(params?: {
  kind?: string;
  limit?: number;
  metric?: string;
}): Promise<MoversResponse> {
  const q = new URLSearchParams();
  if (params?.kind) q.set("kind", params.kind);
  if (params?.limit) q.set("limit", String(params.limit));
  if (params?.metric) q.set("metric", params.metric);
  return fetchJSON(`/api/movers?${q}`);
}

// ─── Returns ─────────────────────────────────────────────────────────────────

export function getReturns(): Promise<ReturnsResponse> {
  return fetchJSON("/api/returns");
}

export function getSnapshots(params?: {
  from?: string;
  to?: string;
  mode?: "raw" | "market";
}): Promise<SnapshotPoint[] | SnapshotPointMarket[]> {
  const q = new URLSearchParams();
  if (params?.from) q.set("from", params.from);
  if (params?.to) q.set("to", params.to);
  if (params?.mode) q.set("mode", params.mode);
  return fetchJSON(`/api/snapshots?${q}`);
}

// ─── Cashflows ───────────────────────────────────────────────────────────────

export function getCashflowsAuto(days = 30): Promise<CashflowsAutoResponse> {
  return fetchJSON(`/api/cashflows/auto?days=${days}`);
}

export function getCashflowsManual(params?: {
  from?: string;
  to?: string;
}): Promise<ManualCashflow[]> {
  const q = new URLSearchParams();
  if (params?.from) q.set("from", params.from);
  if (params?.to) q.set("to", params.to);
  return fetchJSON(`/api/cashflows/manual?${q}`);
}

export function addManualCashflow(data: ManualCashflowInput): Promise<ManualCashflow> {
  return fetchJSON("/api/cashflows/manual", {
    method: "POST",
    body: JSON.stringify(data),
  });
}

export function deleteManualCashflow(id: number): Promise<{ ok: boolean; id: number }> {
  return fetchJSON(`/api/cashflows/manual/${id}`, { method: "DELETE" });
}

// ─── Advisor ─────────────────────────────────────────────────────────────────

export function getAdvisorLatest(
  cadence: AdvisorCadence = "daily"
): Promise<AdvisorBriefingResponse> {
  return fetchJSON(`/api/advisor/latest?cadence=${cadence}`);
}

export function getAdvisorHistory(params?: {
  cadence?: AdvisorCadence;
  limit?: number;
}): Promise<AdvisorHistoryResponse> {
  const q = new URLSearchParams();
  if (params?.cadence) q.set("cadence", params.cadence);
  if (params?.limit) q.set("limit", String(params.limit));
  return fetchJSON(`/api/advisor/history?${q}`);
}

export function getAdvisorOpportunities(): Promise<OpportunityRunResponse> {
  return fetchJSON("/api/advisor/opportunities/latest");
}

// ─── Quality ─────────────────────────────────────────────────────────────────

export function getQuality(): Promise<QualityResponse> {
  return fetchJSON("/api/quality");
}

export function getReconciliationOpen(): Promise<ReconciliationOpenResponse> {
  return fetchJSON("/api/reconciliation/open");
}

export function getReconciliationLatest(): Promise<ReconciliationLatestResponse> {
  return fetchJSON("/api/reconciliation/latest");
}

export function applyReconciliation(
  proposalId: number,
  note?: string
): Promise<unknown> {
  return fetchJSON("/api/reconciliation/apply", {
    method: "POST",
    body: JSON.stringify({ proposal_id: proposalId, note }),
  });
}

export function dismissReconciliation(
  proposalId: number,
  reason: string
): Promise<unknown> {
  return fetchJSON("/api/reconciliation/dismiss", {
    method: "POST",
    body: JSON.stringify({ proposal_id: proposalId, reason }),
  });
}

// ─── Inflation ───────────────────────────────────────────────────────────────

export function getKpiMonthlyVsInflation(): Promise<MonthlyKPI> {
  return fetchJSON("/api/kpi/monthly-vs-inflation");
}

export function getInflationSeriesComparison(params?: {
  from?: string;
  to?: string;
}): Promise<InflationSeriesComparison> {
  const q = new URLSearchParams();
  if (params?.from) q.set("from", params.from);
  if (params?.to) q.set("to", params.to);
  return fetchJSON(`/api/compare/inflation/series?${q}`);
}

export function getInflationAnnual(years = 10): Promise<AnnualComparisonResponse> {
  return fetchJSON(`/api/compare/inflation/annual?years=${years}`);
}
