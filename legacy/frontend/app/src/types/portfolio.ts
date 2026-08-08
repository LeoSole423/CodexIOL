export interface Snapshot {
  snapshot_date: string;
  total_value: number;
  currency: string;
  titles_value: number;
  cash_total_ars: number | null;
  cash_disponible_ars: number | null;
  cash_disponible_usd: number | null;
  retrieved_at: string;
  close_time: string | null;
  minutes_from_close: number | null;
  source: string;
}

export interface Asset {
  symbol: string;
  description: string;
  currency: string;
  market: string;
  type: string;
  total_value: number;
  daily_var_pct: number | null;
  daily_var_points: number | null;
  gain_amount: number | null;
}

export interface LatestResponse {
  snapshot: Snapshot | null;
  assets: Asset[];
  message?: string;
}

export interface AllocationItem {
  key: string;
  value: number;
}

export type AllocationGroupBy = "symbol" | "type" | "market" | "currency";

export interface AssetPerformanceRow {
  symbol: string;
  description: string;
  currency: string;
  market: string;
  type: string;
  total_value: number;
  base_total_value: number;
  selected_value: number | null;
  selected_pct: number | null;
  weight_pct: number;
  flow_tag: string;
}

export interface AssetPerformanceResponse {
  period: string;
  from: string | null;
  to: string | null;
  warnings: string[];
  orders_stats: {
    total: number;
    unclassified: number;
    amount_missing: number;
  } | null;
  rows: AssetPerformanceRow[];
}

export type AssetPeriod =
  | "daily"
  | "weekly"
  | "monthly"
  | "yearly"
  | "accumulated";

export interface MoversResponse {
  gainers: Asset[];
  losers: Asset[];
  period?: string;
  from?: string | null;
  to?: string | null;
  metric?: string;
  currency?: string;
  warnings?: string[];
}
