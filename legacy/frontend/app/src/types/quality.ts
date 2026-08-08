export type QualityKind = "ok" | "warn" | "danger" | "info";

export interface QualityRow {
  id: string;
  label: string;
  value: string;
  kind: QualityKind;
  detail: string;
  sources: string[];
  codes: string[];
}

export interface QualityResponse {
  rows: QualityRow[];
  meta: {
    snapshot_date: string;
  };
}

export interface ReconciliationSummary {
  coverage_mode: string;
  open_intervals: number;
  suppressed_intervals: number;
  counts: Record<string, number>;
  headline: string;
  latest_run_id: number | null;
  created_at_utc: string;
}

export interface ReconciliationProposal {
  id: number;
  interval_id?: number;
  resolution?: string;
  amount_ars?: number;
  note?: string;
  [key: string]: unknown;
}

export interface ReconciliationOpenResponse {
  run: {
    summary: ReconciliationSummary;
  };
  rows: ReconciliationProposal[];
}

export interface ReconciliationLatestResponse {
  summary: ReconciliationSummary;
  intervals: unknown[];
  proposals: ReconciliationProposal[];
}

export interface IncomeSummaryByKind {
  kind: string;
  count: number;
  total_ars: number;
}

export interface IncomeSummaryBySymbol {
  symbol: string;
  kind: string;
  count: number;
  total_ars: number;
}

export interface IncomeSummaryResponse {
  days: number;
  from: string | null;
  to: string | null;
  total_ars: number;
  by_kind: IncomeSummaryByKind[];
  by_symbol: IncomeSummaryBySymbol[];
}
