export interface MonthlyKPI {
  month: string;
  from: string | null;
  to: string | null;
  market_pct: number | null;
  market_delta_ars: number | null;
  contributions_ars: number | null;
  net_pct: number | null;
  net_delta_ars: number | null;
  inflation_pct: number | null;
  inflation_projected: boolean;
  real_vs_inflation_pct: number | null;
  beats_inflation: boolean | null;
  quality_warnings: string[];
  inflation_available_to: string | null;
  flow_confidence: string | null;
  status: string;
  estimated: boolean;
}

export interface InflationSeriesComparison {
  stale: boolean;
  inflation_available_to: string | null;
  projection_used: boolean;
  labels: string[];
  portfolio_value: (number | null)[];
  inflation_value: (number | null)[];
  portfolio_index: (number | null)[];
  inflation_index: (number | null)[];
}

export interface AnnualComparisonRow {
  label: string;
  from: string;
  to: string;
  portfolio_pct: number | null;
  inflation_pct: number | null;
  real_pct: number | null;
  partial: boolean;
  inflation_projected: boolean;
}

export interface AnnualComparisonResponse {
  stale: boolean;
  inflation_available_to: string | null;
  projection_used: boolean;
  rows: AnnualComparisonRow[];
}
