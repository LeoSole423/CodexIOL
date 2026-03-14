export interface ManualCashflow {
  id: number;
  flow_date: string;
  kind: string;
  amount_ars: number;
  note: string | null;
  created_at_utc: string;
}

export interface AutoCashflowRow {
  flow_date: string;
  kind: string;
  amount_ars: number;
  display_kind: string;
  display_label: string;
  reason_code: string;
  reason_detail: string;
  quality_warnings: string[];
  gross_delta: number;
  cash_ars_delta: number | null;
  cash_usd_delta: number | null;
  buy_amount_ars: number;
  sell_amount_ars: number;
  income_amount_ars: number;
  fee_amount_ars: number;
  external_raw_ars: number;
  external_adjusted_ars: number;
  external_final_ars: number;
  fx_revaluation_ars: number;
  imported_internal_ars: number;
  imported_external_ars: number;
  residual_ratio: number | null;
  flow_tag: string;
}

export interface CashflowsAutoResponse {
  from: string | null;
  to: string | null;
  days: number;
  rows: AutoCashflowRow[];
}

export interface ManualCashflowInput {
  flow_date: string;
  kind: string;
  amount_ars: number;
  note?: string;
}
