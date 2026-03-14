// ─── Currency ────────────────────────────────────────────────────────────────

const ARS_FORMATTER = new Intl.NumberFormat("es-AR", {
  style: "currency",
  currency: "ARS",
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});

const USD_FORMATTER = new Intl.NumberFormat("es-AR", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const NUM_FORMATTER = new Intl.NumberFormat("es-AR", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});

export function fmtARS(value: number | null | undefined): string {
  if (value == null) return "—";
  return ARS_FORMATTER.format(value);
}

export function fmtUSD(value: number | null | undefined): string {
  if (value == null) return "—";
  return USD_FORMATTER.format(value);
}

export function fmtNum(value: number | null | undefined, decimals = 2): string {
  if (value == null) return "—";
  return new Intl.NumberFormat("es-AR", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
}

// ─── Percentage ──────────────────────────────────────────────────────────────

export function fmtPct(
  value: number | null | undefined,
  opts: { decimals?: number; sign?: boolean } = {}
): string {
  if (value == null) return "—";
  const { decimals = 2, sign = true } = opts;
  const formatted = new Intl.NumberFormat("es-AR", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(Math.abs(value));
  const prefix = sign && value > 0 ? "+" : value < 0 ? "-" : "";
  return `${prefix}${formatted}%`;
}

// ─── Delta ───────────────────────────────────────────────────────────────────

export function fmtDelta(
  value: number | null | undefined,
  currency: "ARS" | "USD" = "ARS"
): string {
  if (value == null) return "—";
  const fmt = currency === "ARS" ? fmtARS : fmtUSD;
  const sign = value > 0 ? "+" : "";
  return `${sign}${fmt(value)}`;
}

// ─── Dates ───────────────────────────────────────────────────────────────────

export function fmtMonthLabel(yyyyMM: string | null | undefined): string {
  if (!yyyyMM) return "—";
  const [year, month] = yyyyMM.split("-");
  const date = new Date(Number(year), Number(month) - 1, 1);
  return date.toLocaleString("es-AR", { month: "short", year: "numeric" });
}

export function fmtDate(isoDate: string | null | undefined): string {
  if (!isoDate) return "—";
  const d = new Date(isoDate.includes("T") ? isoDate : `${isoDate}T12:00:00`);
  return d.toLocaleDateString("es-AR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  });
}

export function fmtRelativeTime(isoStr: string | null | undefined): string {
  if (!isoStr) return "—";
  const diff = Date.now() - new Date(isoStr).getTime();
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return "hace un momento";
  if (mins < 60) return `hace ${mins} min`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `hace ${hrs}h`;
  const days = Math.floor(hrs / 24);
  return `hace ${days}d`;
}

// ─── Sign utilities ───────────────────────────────────────────────────────────

export function signClass(value: number | null | undefined): string {
  if (value == null || value === 0) return "text-muted-foreground";
  return value > 0 ? "text-positive" : "text-negative";
}

export function signPrefix(value: number | null | undefined): string {
  if (value == null || value === 0) return "";
  return value > 0 ? "+" : "";
}

// ─── Misc ─────────────────────────────────────────────────────────────────────

export function humanize(str: string): string {
  return str
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export { NUM_FORMATTER };
