export interface DataFreshness {
  status: "fresh" | "aging" | "stale";
  days_stale: number;
}

export interface OrdersCoverage {
  total: number;
  unclassified: number;
  amount_missing: number;
}

export interface MovementsCoverage {
  total_rows: number;
  recent_rows: number;
  latest_movement_date: string;
}

export interface ReturnBlock {
  from: string | null;
  to: string | null;
  pct: number | null;
  delta: number | null;
  real_pct: number | null;
  real_delta: number | null;
  flow_total_ars: number;
  quality_warnings: string[];
  flow_confidence: string;
  data_freshness: DataFreshness;
  orders_coverage: OrdersCoverage;
  movements_coverage: MovementsCoverage;
  estimated: boolean;
}

export interface ReturnsResponse {
  daily: ReturnBlock;
  weekly: ReturnBlock;
  monthly: ReturnBlock;
  yearly: ReturnBlock;
  ytd: ReturnBlock;
  inception: ReturnBlock;
}

export interface SnapshotPoint {
  date: string;
  total_value: number;
}

export interface SnapshotPointMarket extends SnapshotPoint {
  raw_total_value: number;
  flow_total_ars: number;
  applied_flow_ars: number;
  external_raw_ars: number;
  external_adjusted_ars: number;
  external_final_ars: number;
  fx_revaluation_ars: number;
  imported_internal_ars: number;
  imported_external_ars: number;
  display_kind: string;
  display_label: string;
  reason_code: string;
  reason_detail: string;
  quality_warnings: string[];
}
