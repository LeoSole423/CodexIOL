export type AdvisorCadence = "daily" | "weekly";

export interface AdvisorBriefingResponse {
  cadence: AdvisorCadence;
  briefing: AdvisorBriefing | null;
}

export interface AdvisorBriefing {
  id?: number;
  cadence?: string;
  headline?: string;
  decision?: string;
  status?: string;
  created_at?: string;
  items?: AdvisorItem[];
  watchlist?: AdvisorItem[];
  blockers?: string[];
  health?: AdvisorHealth;
  [key: string]: unknown;
}

export interface AdvisorItem {
  symbol?: string;
  action?: string;
  confidence?: string;
  status?: string;
  reason?: string;
  analysis?: string;
  quality_codes?: string[];
  [key: string]: unknown;
}

export interface AdvisorHealth {
  [key: string]: unknown;
}

export interface AdvisorHistoryResponse {
  cadence: AdvisorCadence | null;
  rows: AdvisorBriefing[];
}

export interface OpportunityRunResponse {
  run: OpportunityRun | null;
}

export interface OpportunityRun {
  id?: number;
  created_at?: string;
  mode?: string;
  budget_ars?: number;
  top?: number;
  [key: string]: unknown;
}
