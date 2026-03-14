export const fmtARS = new Intl.NumberFormat("es-AR", {
  style: "currency",
  currency: "ARS",
  maximumFractionDigits: 0,
});
export const fmtUSD = new Intl.NumberFormat("es-AR", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 0,
});
export const fmtNum = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 });

export function el(id) { return document.getElementById(id); }
export function escHtml(v) {
  return String(v == null ? "" : v)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
export function escAttr(v) {
  return escHtml(v).replace(/"/g, "&quot;");
}

export function signClass(v) {
  if (v == null) return "";
  if (v > 0) return "pos";
  if (v < 0) return "neg";
  return "";
}

export function fmtPct(v) {
  if (v == null) return "-";
  return (v >= 0 ? "+" : "") + fmtNum.format(v) + "%";
}

export function fmtDeltaARS(v) {
  if (v == null) return "-";
  return (v >= 0 ? "+" : "-") + fmtARS.format(Math.abs(v));
}

export function fmtDelta(v, fmtCur) {
  if (v == null) return "-";
  const f = fmtCur || fmtARS;
  return (v >= 0 ? "+" : "-") + f.format(Math.abs(v));
}

/* ── Counter animation ── */
export function animateValue(element, endVal, formatter, duration) {
  if (!element || endVal == null) return;
  const dur = duration || 800;
  const startTime = performance.now();
  const startVal = 0;

  function tick(now) {
    const elapsed = now - startTime;
    const progress = Math.min(elapsed / dur, 1);
    // Ease-out cubic
    const eased = 1 - Math.pow(1 - progress, 3);
    const current = startVal + (endVal - startVal) * eased;
    element.textContent = formatter ? formatter(current) : String(Math.round(current));
    if (progress < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

/* ── Relative time ── */
export function relativeTime(isoStr) {
  if (!isoStr) return null;
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return null;
  const diffMs = Date.now() - d.getTime();
  const mins = Math.floor(diffMs / 60000);
  if (mins < 1) return "ahora";
  if (mins < 60) return `hace ${mins} min`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `hace ${hours}h`;
  const days = Math.floor(hours / 24);
  return `hace ${days}d`;
}

export function pad2(v) {
  const n = parseInt(v, 10);
  if (isNaN(n)) return "--";
  return String(n).padStart(2, "0");
}

export function formatSnapshotWindow(snapshotDate, retrievedAt) {
  const d = String(snapshotDate || "");
  const dm = (d.length >= 10) ? `${pad2(d.slice(8, 10))}/${pad2(d.slice(5, 7))}` : "--/--";
  let hm = "--:--";
  if (retrievedAt) {
    const ts = new Date(retrievedAt);
    if (!isNaN(ts.getTime())) hm = `${pad2(ts.getHours())}:${pad2(ts.getMinutes())}`;
  }
  return `Snapshot ${dm} - ${hm}`;
}

export function formatRangeLabel(fromDate, toDate) {
  const f = String(fromDate || "").trim();
  const t = String(toDate || "").trim();
  if (!f || !t) return "Sin snapshots suficientes para ventana.";
  if (f === t) return `Base ${f}. Falta snapshot previo.`;
  return `${f} -> ${t}`;
}

export function hasValidRange(block) {
  if (!block) return false;
  const f = String(block.from || "").trim();
  const t = String(block.to || "").trim();
  if (!f || !t || f === t) return false;
  return true;
}

export function hasRenderableRange(block) {
  if (!block) return false;
  const f = String(block.from || "").trim();
  const t = String(block.to || "").trim();
  return !!(f && t);
}

export async function fetchJSON(url, options) {
  const opts = Object.assign({ cache: "no-store" }, options || {});
  const r = await fetch(url, opts);
  let data = null;
  try {
    data = await r.json();
  } catch (_) {
    data = null;
  }
  if (!r.ok) {
    const err = new Error((data && data.error) ? data.error : `HTTP ${r.status}`);
    err.status = r.status;
    err.payload = data;
    throw err;
  }
  return data;
}

export function formatByCurrency(currencyCode, value) {
  if (currencyCode === "dolar_Estadounidense" || currencyCode === "USD") {
    return fmtUSD.format(value || 0);
  }
  return fmtARS.format(value || 0);
}

export function labelCurrency(currencyCode) {
  if (currencyCode === "dolar_Estadounidense" || currencyCode === "USD") return "USD";
  if (currencyCode === "peso_Argentino" || currencyCode === "ARS") return "ARS";
  return "unknown";
}

export function fmtMonthLabel(yyyyMM) {
  const s = String(yyyyMM || "");
  if (s.length !== 7) return s || "-";
  const y = s.slice(0, 4);
  const m = parseInt(s.slice(5, 7), 10);
  const names = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];
  const name = (m >= 1 && m <= 12) ? names[m - 1] : s.slice(5, 7);
  return `${name} ${y}`;
}

export function humanizeEnum(value) {
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
}

export function advisorStatusInfo(status) {
  const s = String(status || "").toLowerCase();
  if (s === "actionable") return { label: "Accionable", className: "badge badge-ok" };
  if (s === "ok") return { label: "OK", className: "badge badge-ok" };
  if (s === "conditional") return { label: "Revisar", className: "badge badge-warn" };
  if (s === "warn") return { label: "Revisar", className: "badge badge-warn" };
  if (s === "watchlist") return { label: "Esperar", className: "badge badge-watchlist" };
  if (s === "blocked") return { label: "Bloqueado", className: "badge badge-blocked" };
  if (s === "info") return { label: "Seguimiento", className: "badge badge-info" };
  if (s === "error") return { label: "Error", className: "badge badge-error" };
  return { label: "-", className: "badge" };
}
