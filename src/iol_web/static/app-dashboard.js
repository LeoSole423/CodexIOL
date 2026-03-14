import {
  el, escHtml, signClass,
  fmtARS, fmtUSD, fmtNum, fmtPct, fmtDelta, fmtDeltaARS,
  animateValue, relativeTime,
  formatSnapshotWindow, formatRangeLabel, hasValidRange, hasRenderableRange,
  fetchJSON, formatByCurrency, labelCurrency, fmtMonthLabel, humanizeEnum,
  advisorStatusInfo,
} from "./app-utils.js";
import { buildLineChart, buildPieChart, renderAllocLegend } from "./app-charts.js";
import { refreshManualCashflows, refreshAutoCashflows } from "./app-cashflows.js";

let chartTotal = null;
let chartAlloc = null;
let assetCtx = { latestYear: null, latestMonth: null, years: [] };

/* ── Local helpers ── */

function fmtDateShort(value) {
  if (!value) return "-";
  const src = String(value);
  const dt = src.length <= 10 ? new Date(`${src}T00:00:00`) : new Date(src);
  if (isNaN(dt.getTime())) return src;
  return new Intl.DateTimeFormat("es-AR", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(dt);
}

function advisorGroupCounts(recommendations, watchlist) {
  const counts = { actionable: 0, conditional: 0, watchlist: 0, blocked: 0, total: 0 };
  const all = [].concat(recommendations || []).concat(watchlist || []);
  for (const row of all) {
    const key = String((row && row.status) || "").toLowerCase();
    if (Object.prototype.hasOwnProperty.call(counts, key)) counts[key] += 1;
    counts.total += 1;
  }
  return counts;
}

/* ── Table renderers ── */

function renderTable(targetId, rows, metricKey, metricLabel, currencyCode) {
  const root = el(targetId);
  if (!root) return;

  const fmtCur = (currencyCode === "USD") ? fmtUSD : fmtARS;

  const head = `
    <th>S\u00edmbolo</th>
    <th>Desc</th>
    <th class="num">Valor</th>
    <th class="num">${metricLabel || (metricKey === "daily_var_points" ? "D\u00eda" : "PnL")}</th>
  `;

  const body = (rows || []).map((r, idx) => {
    const metric = r[metricKey];
    const pct = (metricKey === "delta_value") ? r["delta_pct"] : null;
    const metricText = fmtDelta(metric, fmtCur);
    const pctText = (pct == null) ? "" : ` <span class="muted">(${fmtPct(pct)})</span>`;
    const flowTag = (metricKey === "delta_value") ? String(r.flow_tag || "none") : "none";
    let flowBadge = "";
    if (flowTag === "liquidated") flowBadge = `<span class="row-badge row-badge-liquidated">Liquidado</span>`;
    if (flowTag === "missing_cashflow") flowBadge = `<span class="row-badge row-badge-missing">Cerrado s/ flujo</span>`;
    return `
      <tr style="animation: fade-in 0.3s ease ${idx * 40}ms both;">
        <td>${r.symbol || "-"}</td>
        <td><div class="desc-wrap"><span class="muted">${(r.description || "").slice(0, 42)}</span>${flowBadge}</div></td>
        <td class="num">${fmtCur.format(r.total_value || 0)}</td>
        <td class="num ${signClass(metric)}">${metricText}${pctText}</td>
      </tr>
    `;
  }).join("");

  root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function renderAssetPerformanceTable(targetId, rows) {
  const root = el(targetId);
  if (!root) return;

  const head = `
    <th>S\u00edmbolo</th>
    <th>Descripci\u00f3n</th>
    <th>Moneda</th>
    <th class="num">Valor</th>
    <th class="num">Peso (%)</th>
    <th class="num">PnL seleccionado</th>
  `;

  const body = (rows || []).map((r, idx) => {
    const c = String(r.currency || "unknown");
    const metric = r.selected_value;
    const pct = r.selected_pct;
    const metricText = fmtDelta(metric, c === "dolar_Estadounidense" ? fmtUSD : fmtARS);
    const pctText = (pct == null) ? "" : ` <span class="muted">(${fmtPct(pct)})</span>`;
    const weightText = (r.weight_pct == null) ? "-" : fmtPct(r.weight_pct);
    const flowTag = String(r.flow_tag || "none");
    let flowBadge = "";
    if (flowTag === "liquidated") flowBadge = `<span class="row-badge row-badge-liquidated">Liquidado</span>`;
    if (flowTag === "missing_cashflow") flowBadge = `<span class="row-badge row-badge-missing">Cerrado s/ flujo</span>`;
    return `
      <tr style="animation: fade-in 0.3s ease ${idx * 18}ms both;">
        <td>${r.symbol || "-"}</td>
        <td><div class="desc-wrap"><span class="muted">${(r.description || "").slice(0, 56)}</span>${flowBadge}</div></td>
        <td class="muted">${labelCurrency(c)}</td>
        <td class="num">${formatByCurrency(c, Number(r.total_value || 0))}</td>
        <td class="num">${weightText}</td>
        <td class="num ${signClass(metric)}">${metricText}${pctText}</td>
      </tr>
    `;
  }).join("");

  root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function normalizeLegacyRows(rows, metricKey, pctKey) {
  const bySymbol = {};
  for (const r of (rows || [])) {
    const sym = String((r && r.symbol) || "").trim();
    if (!sym) continue;
    const prev = bySymbol[sym];
    const curMetric = Number((r && r[metricKey]) || 0);
    const prevMetric = prev ? Number((prev && prev[metricKey]) || 0) : null;
    if (!prev || Math.abs(curMetric) >= Math.abs(prevMetric)) bySymbol[sym] = r;
  }
  const out = Object.values(bySymbol).map((r) => ({
    symbol: r.symbol,
    description: r.description,
    currency: r.currency || "unknown",
    market: r.market,
    type: r.type,
    total_value: Number(r.total_value || 0),
    base_total_value: r.base_total_value == null ? null : Number(r.base_total_value || 0),
    selected_value: Number(r[metricKey] || 0),
    selected_pct: (pctKey && r[pctKey] != null) ? Number(r[pctKey]) : null,
    flow_tag: String(r.flow_tag || "none"),
  }));

  const totalVisible = out.reduce((s, it) => s + Number(it.total_value || 0), 0);
  for (const it of out) {
    const tv = Number(it.total_value || 0);
    it.weight_pct = totalVisible > 0 ? (tv / totalVisible * 100.0) : 0.0;
  }
  out.sort((a, b) => {
    const am = Number(a.selected_value || 0);
    const bm = Number(b.selected_value || 0);
    if (bm !== am) return bm - am;
    const at = Number(a.total_value || 0);
    const bt = Number(b.total_value || 0);
    if (bt !== at) return bt - at;
    return String(a.symbol || "").localeCompare(String(b.symbol || ""));
  });
  return out;
}

async function fetchLegacyAssetPerformance(period, latestYear, latestMonth, years) {
  try {
    if (period === "accumulated") {
      const total = await fetchJSON("/api/movers?kind=total&limit=100");
      const merged = (total.gainers || []).concat(total.losers || []);
      return {
        period,
        from: null,
        to: null,
        warnings: [],
        orders_stats: null,
        rows: normalizeLegacyRows(merged, "gain_amount", null),
        fallback: true,
      };
    }

    let url = `/api/movers?kind=period&period=${encodeURIComponent(period)}&limit=100&metric=pnl&currency=all`;
    if (period === "monthly") {
      const m = getAssetMonth(latestMonth || 1);
      url = `/api/movers?kind=period&period=monthly&month=${encodeURIComponent(m)}&year=${encodeURIComponent(latestYear)}&limit=100&metric=pnl&currency=all`;
    } else if (period === "yearly") {
      const y = getAssetYear(latestYear || new Date().getFullYear(), years);
      url = `/api/movers?kind=period&period=yearly&year=${encodeURIComponent(y)}&limit=100&metric=pnl&currency=all`;
    }

    const mv = await fetchJSON(url);
    const merged = (mv.gainers || []).concat(mv.losers || []);
    return {
      period,
      from: mv.from || null,
      to: mv.to || null,
      warnings: mv.warnings || [],
      orders_stats: mv.orders_stats || null,
      rows: normalizeLegacyRows(merged, "delta_value", "delta_pct"),
      fallback: true,
    };
  } catch (_) {
    return {
      period,
      from: null,
      to: null,
      warnings: [],
      orders_stats: null,
      rows: [],
      fallback: true,
    };
  }
}

/* ── KPI / return card renderers ── */

function putReturnCard(mainId, subId, block, label, options) {
  const opts = Object.assign({
    allowPartialRange: false,
    suppressEmptyState: false,
  }, options || {});
  const n = el(mainId);
  const hasRange = opts.allowPartialRange ? hasRenderableRange(block) : hasValidRange(block);
  if (n) {
    if (!hasRange) {
      n.textContent = opts.suppressEmptyState ? "-" : "Sin base";
      n.className = "v muted";
    } else {
      const mainPct = (block && block.real_pct != null) ? block.real_pct : null;
      n.textContent = mainPct == null ? "-" : fmtPct(mainPct);
      n.className = "v " + signClass(mainPct);
    }
  }
  const s = el(subId);
  if (s) {
    if (!hasRange) {
      if (!opts.suppressEmptyState) {
        s.innerHTML = `<div class="kpi-empty-state">Sin snapshots suficientes para ventana${label ? ` (${label})` : ""}.</div>`;
        return;
      }
      s.innerHTML = `
        <div style="font-size: 14px; font-weight: 600; margin-top: 4px; margin-bottom: 12px;" class="muted">
          -
        </div>
        <div class="mini-grid">
          <div class="mg-row">
            <span class="mg-lbl">Var. Saldo</span>
            <span class="mg-val">-</span>
          </div>
          <div class="mg-row">
            <span class="mg-lbl">Dep./Retiros</span>
            <span class="mg-val">-</span>
          </div>
        </div>
      `;
      return;
    }
    const netArsStr = (block && block.real_delta != null) ? fmtDeltaARS(block.real_delta) : "-";
    const grossArsStr = (block && block.delta != null) ? fmtDeltaARS(block.delta) : "-";
    const flowArsStr = (block && block.flow_total_ars != null) ? fmtDeltaARS(block.flow_total_ars) : "-";

    s.innerHTML = `
      <div style="font-size: 14px; font-weight: 600; margin-top: 4px; margin-bottom: 12px;" class="${signClass(block?.real_delta)}">
        ${netArsStr}
      </div>
      <div class="mini-grid">
        <div class="mg-row">
          <span class="mg-lbl">Var. Saldo</span>
          <span class="mg-val">${grossArsStr}</span>
        </div>
        <div class="mg-row">
          <span class="mg-lbl">Dep./Retiros</span>
          <span class="mg-val">${flowArsStr}</span>
        </div>
      </div>
    `;
  }
}

function renderRangeState(mainId, subId, block, label, options) {
  putReturnCard(mainId, subId, block, label, options);
}

function flowConfidenceLabel(block) {
  const c = String((block && block.flow_confidence) || "");
  if (c === "high") return "Confirmado";
  if (c === "medium") return "Parcial";
  if (c === "low") return "Estimado";
  return "Sin señal";
}

function renderFlowBreakdown(targetId, block) {
  const root = el(targetId);
  if (!root) return;
  const flow = (block && block.flow_breakdown) ? block.flow_breakdown : {};
  const gross = flow.gross_delta_ars != null ? flow.gross_delta_ars : (block && block.delta);
  const market = flow.market_delta_ars != null ? flow.market_delta_ars : (block && block.real_delta);
  const external = flow.external_flow_ars != null ? flow.external_flow_ars : (block && block.flow_total_ars);
  const fx = flow.fx_revaluation_ars;
  root.innerHTML = `
    <div class="mini-grid" style="margin-bottom: 12px;">
      <div class="mg-row">
        <span class="mg-lbl">Var. saldo</span>
        <span class="mg-val">${fmtDeltaARS(gross)}</span>
      </div>
      <div class="mg-row">
        <span class="mg-lbl">Mercado neto</span>
        <span class="mg-val">${fmtDeltaARS(market)}</span>
      </div>
      <div class="mg-row">
        <span class="mg-lbl">Flujos externos</span>
        <span class="mg-val">${fmtDeltaARS(external)}</span>
      </div>
      <div class="mg-row">
        <span class="mg-lbl">FX caja USD</span>
        <span class="mg-val">${fmtDeltaARS(fx)}</span>
      </div>
      <div class="mg-row">
        <span class="mg-lbl">Confianza</span>
        <span class="mg-val">${escHtml(flowConfidenceLabel(block))}</span>
      </div>
    </div>
  `;
}

function putTextAndSign(id, value, formatter, baseClass = "v") {
  const node = el(id);
  if (!node) return;
  if (value == null) {
    node.textContent = "-";
    node.className = baseClass;
    return;
  }
  node.textContent = formatter ? formatter(value) : String(value);
  const spacing = baseClass ? " " : "";
  node.className = baseClass + spacing + signClass(Number(value));
}

function renderMonthVsInflationKpi(kpi) {
  const v = el("kpiMonthVsInflValue");
  const s = el("kpiMonthVsInflSub");
  const b = el("kpiMonthVsInflBadge");
  if (!v || !s || !b) return;

  const status = (kpi && kpi.status) ? String(kpi.status) : "";
  const baseClass = "kpi-value hero-real-value";
  v.className = baseClass;
  b.style.display = "none";
  b.className = "kpi-status-badge";
  b.textContent = "";

  if (!kpi || !status) {
    v.textContent = "-";
    s.textContent = "No se pudo cargar el KPI mensual.";
    return;
  }

  if (status === "insufficient_snapshots") {
    v.textContent = "-";
    s.textContent = "Faltan snapshots para calcular acumulado mensual.";
    return;
  }

  if (status === "inflation_unavailable") {
    const net = (kpi.net_pct != null) ? fmtPct(kpi.net_pct) : "-";
    v.textContent = net;
    v.className = baseClass + " " + signClass(kpi.net_pct);
    s.textContent = `Neto ${net} | IPC no disponible | ${flowConfidenceLabel(kpi)}`;
    return;
  }

  const real = (kpi.real_vs_inflation_pct != null) ? Number(kpi.real_vs_inflation_pct) : null;
  const net = (kpi.net_pct != null) ? fmtPct(kpi.net_pct) : "-";
  const ipc = (kpi.inflation_pct != null)
    ? (fmtPct(kpi.inflation_pct) + (kpi.inflation_projected ? " (est.)" : ""))
    : "-";

  v.textContent = real == null ? "-" : fmtPct(real);
  v.className = baseClass + " " + signClass(real);
  s.textContent = `Neto ${net} | IPC ${ipc} | ${flowConfidenceLabel(kpi)}`;

  if (kpi.beats_inflation == null || real == null) {
    b.style.display = "inline-flex";
    b.className = "kpi-status-badge kpi-status-neutral";
    b.textContent = kpi.estimated ? "Estimado" : "Sin senal";
    return;
  }

  b.style.display = "inline-flex";
  if (kpi.estimated) {
    b.className = "kpi-status-badge kpi-status-neutral";
    b.textContent = "Estimado";
  } else if (kpi.beats_inflation) {
    b.className = "kpi-status-badge kpi-status-up";
    b.textContent = "Le ganas al IPC";
  } else {
    b.className = "kpi-status-badge kpi-status-down";
    b.textContent = "Debajo del IPC";
  }
}

function renderHeroMonthInsights(kpi) {
  if (!kpi) {
    putTextAndSign("kpiMonthNetPct", null, null, "kpi-pct");
    if (el("kpiMonthGrossFlow")) el("kpiMonthGrossFlow").innerHTML = "";
    putTextAndSign("kpiMonthNetArs", null, null, "kpi-value");
    return;
  }

  const status = String(kpi.status || "");
  if (status === "ok" || status === "inflation_unavailable") {
    putTextAndSign("kpiMonthNetPct", kpi.net_pct, (v) => fmtPct(v), "kpi-pct");

    const totalChangeArs = (kpi.market_delta_ars != null) ? kpi.market_delta_ars : null;
    const totalChangeStr = (totalChangeArs != null) ? fmtDeltaARS(totalChangeArs) : "-";
    const flowArsStr = (kpi.contributions_ars != null) ? fmtDeltaARS(kpi.contributions_ars) : "-";

    const mg = el("kpiMonthGrossFlow");
    if (mg) {
      mg.innerHTML = `
        <div class="mini-grid" style="margin-bottom: 12px;">
          <div class="mg-row">
            <span class="mg-lbl">Var. Saldo</span>
            <span class="mg-val">${totalChangeStr}</span>
          </div>
          <div class="mg-row">
            <span class="mg-lbl">Depósitos/Retiros</span>
            <span class="mg-val">${flowArsStr}</span>
          </div>
        </div>
      `;
    }

    putTextAndSign("kpiMonthNetArs", kpi.net_delta_ars, (v) => fmtDeltaARS(v), "kpi-value");
    return;
  }

  putTextAndSign("kpiMonthNetPct", null, null, "kpi-pct");
  if (el("kpiMonthGrossFlow")) el("kpiMonthGrossFlow").innerHTML = "";
  putTextAndSign("kpiMonthNetArs", null, null, "kpi-value");
}

function renderHeroMonthInsightsEnhanced(kpi) {
  if (!kpi) {
    renderHeroMonthInsights(kpi);
    return;
  }
  const status = String(kpi.status || "");
  if (status !== "ok" && status !== "inflation_unavailable") {
    renderHeroMonthInsights(kpi);
    return;
  }
  putTextAndSign("kpiMonthNetPct", kpi.net_pct, (v) => fmtPct(v), "kpi-pct");
  renderFlowBreakdown("kpiMonthGrossFlow", {
    delta: kpi.market_delta_ars,
    real_delta: kpi.net_delta_ars,
    flow_total_ars: kpi.contributions_ars,
    flow_confidence: kpi.flow_confidence,
    flow_breakdown: {
      gross_delta_ars: kpi.market_delta_ars,
      market_delta_ars: kpi.net_delta_ars,
      external_flow_ars: kpi.contributions_ars,
      fx_revaluation_ars: null,
    },
  });
  putTextAndSign("kpiMonthNetArs", kpi.net_delta_ars, (v) => fmtDeltaARS(v), "kpi-value");
}

function renderWindowStatus(targetId, block) {
  const node = el(targetId);
  if (!node) return;
  if (hasValidRange(block)) {
    node.textContent = `Ventana ${formatRangeLabel(block.from, block.to)}`;
    node.className = "kpi-meta hero-window";
    return;
  }
  node.textContent = "Sin snapshots suficientes para ventana.";
  node.className = "kpi-meta hero-window muted";
}

function renderInflationCompactFromKpi(targetId, kpi) {
  const root = el(targetId);
  if (!root) return;

  const head = `
    <th>Mes</th>
    <th>Snapshots</th>
    <th class="num">Retorno</th>
    <th class="num">IPC</th>
    <th class="num">Real</th>
  `;

  if (!kpi || !kpi.status) {
    root.innerHTML = `<div class="hint">No se pudo cargar la comparativa mensual.</div>`;
    const hint = el("inflationHint");
    if (hint) hint.style.display = "none";
    return;
  }

  const status = String(kpi.status || "");
  if (status === "insufficient_snapshots") {
    root.innerHTML = `<div class="hint">Faltan snapshots para calcular el mes.</div>`;
    const hint = el("inflationHint");
    if (hint) hint.style.display = "none";
    return;
  }

  const month = fmtMonthLabel(kpi.month || "");
  const snaps = (kpi.from && kpi.to) ? `${kpi.from} \u2192 ${kpi.to}` : "-";
  const ret = (kpi.net_pct == null) ? "-" : fmtPct(kpi.net_pct);
  const ipc = (kpi.inflation_pct == null)
    ? "-"
    : (fmtPct(kpi.inflation_pct) + (kpi.inflation_projected ? " (est.)" : ""));
  const real = (kpi.real_vs_inflation_pct == null) ? "-" : fmtPct(kpi.real_vs_inflation_pct);
  const cls = signClass(kpi.real_vs_inflation_pct);

  root.innerHTML = `
    <table><thead><tr>${head}</tr></thead><tbody>
      <tr>
        <td>${month}</td>
        <td class="muted">${snaps}</td>
        <td class="num">${ret}</td>
        <td class="num">${ipc}</td>
        <td class="num ${cls}">${real}</td>
      </tr>
    </tbody></table>
  `;

  const hint = el("inflationHint");
  if (hint) {
    const bits = [];
    if (kpi.inflation_available_to) bits.push(`IPC disponible hasta ${fmtMonthLabel(kpi.inflation_available_to)}`);
    if (kpi.inflation_projected && kpi.inflation_available_to) bits.push(`mes actual estimado con ${fmtMonthLabel(kpi.inflation_available_to)}`);
    if (status === "inflation_unavailable") bits.push("IPC no disponible para este mes");
    if (bits.length) {
      hint.style.display = "block";
      hint.textContent = "Inflaci\u00f3n: " + bits.join(" | ") + ".";
    } else {
      hint.style.display = "none";
    }
  }
}

/* ── Advisor teaser (dashboard preview) ── */

async function loadAdvisorTeaser() {
  const root = el("advisorTeaser");
  if (!root) return;
  try {
    const out = await fetchJSON("/api/advisor/latest?cadence=daily");
    const briefing = out && out.briefing;
    if (!briefing) {
      root.innerHTML = `<div class="hint">Todavía no hay briefing diario generado.</div>`;
      return;
    }
    const info = advisorStatusInfo(briefing.status);
    const counts = advisorGroupCounts(briefing.recommendations, briefing.watchlist);
    const recs = [];
    if (counts.actionable) recs.push(`${counts.actionable} accionables`);
    if (counts.conditional) recs.push(`${counts.conditional} condicionales`);
    if (counts.watchlist) recs.push(`${counts.watchlist} watchlist`);
    root.innerHTML = `
      <div class="advisor-teaser-head">
        <span class="${info.className}">${info.label}</span>
        <span class="muted">${escHtml(fmtDateShort(briefing.as_of || "-"))}</span>
      </div>
      <div class="advisor-teaser-body">${recs.join(" · ") || "Sin recomendaciones destacadas."}</div>
    `;
  } catch (_) {
    root.innerHTML = `<div class="hint">No se pudo cargar el briefing diario.</div>`;
  }
}

/* ── localStorage state helpers ── */

export function getAssetPeriod() {
  try {
    const v1 = (localStorage.getItem("assetPeriod") || "").trim().toLowerCase();
    if (["daily", "weekly", "monthly", "yearly", "accumulated"].includes(v1)) return v1;
  } catch (_) { }
  return "daily";
}

function setAssetPeriod(v) {
  try { localStorage.setItem("assetPeriod", String(v || "daily")); } catch (_) { }
}

function getAssetMonth(defaultMonth) {
  try {
    const current = parseInt(localStorage.getItem("assetMonth") || "", 10);
    if (current >= 1 && current <= 12) return current;
  } catch (_) { }
  return (defaultMonth >= 1 && defaultMonth <= 12) ? defaultMonth : 1;
}

function setAssetMonth(v) {
  try { localStorage.setItem("assetMonth", String(parseInt(v, 10) || 1)); } catch (_) { }
}

function getAssetYear(defaultYear, years) {
  let y = defaultYear;
  try {
    const current = parseInt(localStorage.getItem("assetYear") || "", 10);
    if (!isNaN(current)) y = current;
  } catch (_) { }
  if (Array.isArray(years) && years.length) {
    if (years.includes(y)) return y;
    return years[years.length - 1];
  }
  return y;
}

function setAssetYear(v) {
  try { localStorage.setItem("assetYear", String(parseInt(v, 10) || new Date().getFullYear())); } catch (_) { }
}

export function getActiveRange() {
  const wrap = el("rangeButtons");
  if (!wrap) return "30";
  const on = wrap.querySelector("button.on");
  return on ? on.getAttribute("data-range") : "30";
}

function getEvolutionMode() {
  try {
    const v = String(localStorage.getItem("evolutionMode") || "").toLowerCase();
    if (v === "raw" || v === "market") return v;
  } catch (_) { }
  return "raw";
}

function setEvolutionMode(mode) {
  const m = (mode === "market") ? "market" : "raw";
  try { localStorage.setItem("evolutionMode", m); } catch (_) { }
}

/* ── Asset picker ── */

function updateAssetPicker(period, latestYear, latestMonth, years) {
  const picker = el("assetPicker");
  const label = el("assetPickLabel");
  const prev = el("assetPickPrev");
  const next = el("assetPickNext");
  if (!picker || !label || !prev || !next) return;

  if (period === "monthly") {
    picker.style.display = "inline-flex";
    const m = getAssetMonth(latestMonth || 1);
    setAssetMonth(m);
    const names = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];
    label.textContent = `${names[m - 1]} ${latestYear}`;
    prev.disabled = false;
    next.disabled = false;
    return;
  }

  if (period === "yearly") {
    picker.style.display = "inline-flex";
    const ys = Array.isArray(years) ? years : [];
    const y = getAssetYear(latestYear || new Date().getFullYear(), ys);
    setAssetYear(y);
    label.textContent = String(y);
    if (!ys.length) {
      prev.disabled = true;
      next.disabled = true;
    } else {
      const idx = ys.indexOf(y);
      prev.disabled = idx <= 0;
      next.disabled = idx < 0 || idx >= ys.length - 1;
    }
    return;
  }

  picker.style.display = "none";
}

function moveAssetPicker(dir) {
  const period = getAssetPeriod();
  if (period === "monthly") {
    const baseMonth = assetCtx.latestMonth || 1;
    let m = getAssetMonth(baseMonth);
    m = m + dir;
    if (m < 1) m = 12;
    if (m > 12) m = 1;
    setAssetMonth(m);
    loadDashboard(getActiveRange());
    return;
  }
  if (period === "yearly") {
    const years = assetCtx.years || [];
    if (!years.length) return;
    let y = getAssetYear(assetCtx.latestYear || years[years.length - 1], years);
    const idx = years.indexOf(y);
    const ni = idx + dir;
    if (ni < 0 || ni >= years.length) return;
    setAssetYear(years[ni]);
    loadDashboard(getActiveRange());
  }
}

/* ── Control initializers ── */

export function initRangeButtons() {
  const wrap = el("rangeButtons");
  if (!wrap) return;
  wrap.addEventListener("click", (ev) => {
    const btn = ev.target && ev.target.closest("button");
    if (!btn) return;
    Array.from(wrap.querySelectorAll("button")).forEach(b => b.classList.remove("on"));
    btn.classList.add("on");
    loadDashboard(btn.getAttribute("data-range"));
  });
}

export function initAllocControls() {
  const groupSel = el("allocGroupBy");
  const cashChk = el("allocCash");
  if (groupSel) groupSel.addEventListener("change", () => loadDashboard(getActiveRange()));
  if (cashChk) cashChk.addEventListener("change", () => loadDashboard(getActiveRange()));
}

export function initCashflowControls() {
  const form = el("cashflowForm");
  if (!form) return;

  const dateInput = el("cashflowDate");
  const kindInput = el("cashflowKind");
  const amountInput = el("cashflowAmount");
  const noteInput = el("cashflowNote");
  const hint = el("cashflowHint");

  if (dateInput && !dateInput.value) {
    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, "0");
    const d = String(now.getDate()).padStart(2, "0");
    dateInput.value = `${y}-${m}-${d}`;
  }

  form.addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const payload = {
      flow_date: dateInput ? dateInput.value : "",
      kind: kindInput ? kindInput.value : "",
      amount_ars: amountInput ? Number(amountInput.value || 0) : 0,
      note: noteInput ? (noteInput.value || "").trim() : "",
    };
    try {
      await fetchJSON("/api/cashflows/manual", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (hint) {
        hint.style.display = "block";
        hint.textContent = "Ajuste guardado.";
      }
      if (amountInput) amountInput.value = "";
      if (noteInput) noteInput.value = "";
      await loadDashboard(getActiveRange());
    } catch (e) {
      if (hint) {
        hint.style.display = "block";
        hint.textContent = `No se pudo guardar: ${e && e.message ? e.message : "error"}`;
      }
    }
  });

  const table = el("tblCashflowsManual");
  if (table) {
    table.addEventListener("click", async (ev) => {
      const btn = ev.target && ev.target.closest("[data-cashflow-del]");
      if (!btn) return;
      const id = btn.getAttribute("data-cashflow-del");
      if (!id) return;
      try {
        await fetchJSON(`/api/cashflows/manual/${encodeURIComponent(id)}`, { method: "DELETE" });
        if (hint) {
          hint.style.display = "block";
          hint.textContent = "Ajuste eliminado.";
        }
        await loadDashboard(getActiveRange());
      } catch (e) {
        if (hint) {
          hint.style.display = "block";
          hint.textContent = `No se pudo eliminar: ${e && e.message ? e.message : "error"}`;
        }
      }
    });
  }
}

export function initAssetPerformanceControls() {
  const wrap = el("assetPeriodButtons");
  if (!wrap) return;

  const active = getAssetPeriod();
  Array.from(wrap.querySelectorAll("button")).forEach((b) => {
    b.classList.toggle("on", b.getAttribute("data-period") === active);
  });

  wrap.addEventListener("click", (ev) => {
    const btn = ev.target && ev.target.closest("button");
    if (!btn) return;
    const p = (btn.getAttribute("data-period") || "daily").toLowerCase();
    setAssetPeriod(p);
    Array.from(wrap.querySelectorAll("button")).forEach((b) => b.classList.remove("on"));
    btn.classList.add("on");
    loadDashboard(getActiveRange());
  });
}

export function initAssetPickerControls() {
  const prev = el("assetPickPrev");
  const next = el("assetPickNext");
  if (prev) prev.addEventListener("click", () => moveAssetPicker(-1));
  if (next) next.addEventListener("click", () => moveAssetPicker(+1));
}

export function initEvolutionModeControls() {
  const wrap = el("evolutionModeButtons");
  if (!wrap) return;
  const current = getEvolutionMode();
  Array.from(wrap.querySelectorAll("button")).forEach((b) => {
    b.classList.toggle("on", b.getAttribute("data-mode") === current);
  });
  wrap.addEventListener("click", (ev) => {
    const btn = ev.target && ev.target.closest("button");
    if (!btn) return;
    const mode = btn.getAttribute("data-mode") || "raw";
    setEvolutionMode(mode);
    Array.from(wrap.querySelectorAll("button")).forEach((b) => b.classList.remove("on"));
    btn.classList.add("on");
    loadDashboard(getActiveRange());
  });
}

/* ── Main dashboard loader ── */

export async function loadDashboard(rangeDays) {
  const [latest, ret, monthlyKpi] = await Promise.all([
    fetchJSON("/api/latest"),
    fetchJSON("/api/returns"),
    fetchJSON("/api/kpi/monthly-vs-inflation").catch(() => null),
  ]);

  if (!latest || !latest.snapshot) {
    const hint = el("noDataHint");
    if (hint) hint.style.display = "block";
    return;
  }

  const snap = latest.snapshot;

  const kpiTotalEl = el("kpiTotal");
  if (kpiTotalEl) {
    animateValue(kpiTotalEl, snap.total_value || 0, (v) => fmtARS.format(v), 900);
  }

  if (el("kpiDate")) el("kpiDate").textContent = `Snapshot ${snap.snapshot_date || "-"}`;
  if (el("heroSnapshotWindow")) {
    el("heroSnapshotWindow").textContent = formatSnapshotWindow(snap.snapshot_date, snap.retrieved_at);
  }

  const updBadge = el("updatedBadge");
  const updText = el("updatedText");
  if (updBadge && updText && snap.retrieved_at) {
    const rel = relativeTime(snap.retrieved_at);
    if (rel) {
      updText.textContent = `Actualizado ${rel}`;
      updBadge.style.display = "inline-flex";
    }
  }

  const cashArsEl = el("kpiCashArs");
  if (cashArsEl) {
    const cashArs = snap.cash_disponible_ars;
    if (cashArs != null) {
      animateValue(cashArsEl, cashArs, (v) => fmtARS.format(v), 700);
    } else {
      cashArsEl.textContent = "-";
    }
  }
  const cashUsdEl = el("kpiCashUsd");
  if (cashUsdEl) {
    const cashUsd = snap.cash_disponible_usd;
    if (cashUsd != null) {
      animateValue(cashUsdEl, cashUsd, (v) => fmtUSD.format(v), 700);
    } else {
      cashUsdEl.textContent = "-";
    }
  }

  if (el("heroLiquidityRatio")) {
    const total = Number(snap.total_value || 0);
    let liquidArs = null;
    let ratioNote = "Caja disponible sobre total visible.";
    if (snap.cash_total_ars != null) {
      liquidArs = Number(snap.cash_total_ars || 0);
      ratioNote = "Cash ARS + USD convertido a ARS.";
    } else if (snap.cash_disponible_ars != null) {
      liquidArs = Number(snap.cash_disponible_ars || 0);
      if (snap.cash_disponible_usd != null) ratioNote = "Sin FX USD: usa solo cash ARS disponible.";
    }
    const ratioPct = (total > 0 && liquidArs != null) ? (liquidArs / total * 100.0) : null;
    el("heroLiquidityRatio").textContent = ratioPct == null ? "-" : `${fmtNum.format(ratioPct)}%`;
    el("heroLiquidityRatio").className = "v";
    if (el("heroLiquidityNote")) el("heroLiquidityNote").textContent = ratioNote;
  }

  const daily = ret.daily || {};
  if (el("kpiDailyDelta")) {
    const v = (daily.real_delta != null) ? daily.real_delta : daily.delta;
    const deltaEl = el("kpiDailyDelta");
    if (v != null) {
      animateValue(deltaEl, v, (val) => fmtDeltaARS(val), 700);
      deltaEl.className = "kpi-value " + signClass(v);
    } else {
      deltaEl.textContent = "-";
    }
  }
  if (el("kpiDailyPct")) {
    const p = (daily.real_pct != null) ? daily.real_pct : daily.pct;
    el("kpiDailyPct").textContent = p == null ? "-" : fmtPct(p);
    el("kpiDailyPct").className = "kpi-pct " + signClass(p);
  }
  if (el("kpiDailyGross")) {
    const grossArsStr = (daily.delta != null) ? fmtDeltaARS(daily.delta) : "-";
    const flowArsStr = (daily.flow_total_ars != null) ? fmtDeltaARS(daily.flow_total_ars) : "-";

    el("kpiDailyGross").innerHTML = `
      <div class="mini-grid" style="margin-bottom: 12px;">
        <div class="mg-row">
          <span class="mg-lbl">Var. Saldo</span>
          <span class="mg-val">${grossArsStr}</span>
        </div>
        <div class="mg-row">
          <span class="mg-lbl">Depósitos/Retiros</span>
          <span class="mg-val">${flowArsStr}</span>
        </div>
      </div>
    `;
    renderFlowBreakdown("kpiDailyGross", daily);
  }

  const horizonsOpts = { allowPartialRange: true, suppressEmptyState: true };
  renderRangeState("retWeekly", "retWeeklySub", ret.weekly, "7 dias", horizonsOpts);
  renderRangeState("retMonthly", "retMonthlySub", ret.monthly, "Mes", horizonsOpts);
  renderRangeState("retYearly", "retYearlySub", ret.yearly, "Ano", horizonsOpts);
  renderRangeState("retInception", "retInceptionSub", ret.inception, "Desde inicio", horizonsOpts);
  renderMonthVsInflationKpi(monthlyKpi);
  renderHeroMonthInsightsEnhanced(monthlyKpi);
  renderWindowStatus("heroDailyWindow", daily);
  renderWindowStatus("heroMonthlyWindow", monthlyKpi);

  renderInflationCompactFromKpi("tblInflationCompare", monthlyKpi);

  const evolutionMode = getEvolutionMode();
  const series = await fetchJSON(`/api/snapshots?mode=${encodeURIComponent(evolutionMode)}`);
  const labelsAll = series.map(x => x.date);
  const valuesAll = series.map(x => x.total_value);
  const years = Array.from(
    new Set(labelsAll.map(d => parseInt(String(d).slice(0, 4), 10)).filter(n => !isNaN(n)))
  ).sort((a, b) => a - b);

  const latestDate = String(snap.snapshot_date || "");
  const latestYear = parseInt(latestDate.slice(0, 4), 10);
  const latestMonth = parseInt(latestDate.slice(5, 7), 10);
  assetCtx = { latestYear: latestYear || null, latestMonth: latestMonth || null, years };

  let labels = labelsAll, values = valuesAll;
  if (rangeDays && rangeDays !== "all") {
    const n = parseInt(rangeDays, 10);
    labels = labelsAll.slice(Math.max(0, labelsAll.length - n));
    values = valuesAll.slice(Math.max(0, valuesAll.length - n));
  }

  const canvas = el("chartTotal");
  if (canvas && labels.length) {
    if (chartTotal) chartTotal.destroy();
    chartTotal = buildLineChart(canvas, labels, values);
  }

  const evoHint = el("evolutionHint");
  if (evoHint) {
    if (evolutionMode === "market") {
      const warns = new Set();
      for (const r of (series || [])) {
        const ww = Array.isArray(r && r.quality_warnings) ? r.quality_warnings : [];
        ww.forEach((w) => warns.add(String(w)));
      }
      const hasQualityIssue = warns.has("CASH_MISSING") || warns.has("ORDERS_INCOMPLETE") || warns.has("INFERENCE_PARTIAL");
      evoHint.style.display = "block";
      evoHint.textContent = hasQualityIssue
        ? "Modo ajustado por flujos externos activo. Hay tramos con calidad incompleta (caja/ordenes), por lo que algunos puntos pueden quedar sin ajuste fino."
        : "Modo ajustado por flujos externos activo (descuenta aportes/retiros inferidos + ajustes manuales).";
    } else {
      evoHint.style.display = "none";
    }
  }

  const period = getAssetPeriod();
  function buildAssetPerformanceUrl() {
    let url = `/api/assets/performance?period=${encodeURIComponent(period)}`;
    if (period === "monthly") {
      const m = getAssetMonth(latestMonth || 1);
      url += `&month=${encodeURIComponent(m)}&year=${encodeURIComponent(latestYear)}`;
    } else if (period === "yearly") {
      const y = getAssetYear(latestYear || new Date().getFullYear(), years);
      url += `&year=${encodeURIComponent(y)}`;
    }
    return url;
  }

  let assetsPerf = null;
  try {
    assetsPerf = await fetchJSON(buildAssetPerformanceUrl());
  } catch (_) {
    assetsPerf = await fetchLegacyAssetPerformance(period, latestYear, latestMonth, years);
  }
  if (!assetsPerf) {
    assetsPerf = { period, from: null, to: null, warnings: [], orders_stats: null, rows: [] };
  }
  updateAssetPicker(period, latestYear, latestMonth, years);
  if (period !== "daily" && assetsPerf && assetsPerf.from == null) {
    const msg = (period === "monthly")
      ? "No hay snapshots para ese a\u00f1o."
      : (period === "yearly")
        ? "No hay snapshots para ese a\u00f1o."
        : "No hay snapshots suficientes para ese per\u00edodo. Deja corriendo el scheduler o ejecuta `iol snapshot run` en distintos d\u00edas.";
    const t = el("tblAssetPerformance");
    if (t) t.innerHTML = `<div class="hint">${msg}</div>`;
  } else {
    renderAssetPerformanceTable("tblAssetPerformance", assetsPerf.rows || []);
  }

  const ah = el("assetHint");
  if (ah) {
    const warns = (assetsPerf && assetsPerf.warnings) ? assetsPerf.warnings : [];
    if (warns && warns.length) {
      const from = assetsPerf.from;
      const to = assetsPerf.to;
      const cmd = (from && to) ? `iol snapshot backfill --from ${from} --to ${to}` : "iol snapshot backfill --from YYYY-MM-DD --to YYYY-MM-DD";
      const msg = warns.includes("ORDERS_NONE")
        ? `No hay operaciones terminadas en la DB para este per\u00edodo. Si vendiste activos, corr\u00e9 un backfill: <code>${cmd}</code> y luego ejecut\u00e1 <code>iol snapshot run</code> (o dej\u00e1 el scheduler).`
        : `Hay operaciones incompletas (faltan lado/monto/fecha). Corr\u00e9 un backfill: <code>${cmd}</code> y luego ejecut\u00e1 <code>iol snapshot run</code> (o dej\u00e1 el scheduler).`;
      ah.style.display = "block";
      ah.innerHTML = msg;
    } else {
      ah.style.display = "none";
    }
    if ((!warns || !warns.length) && assetsPerf.fallback) {
      ah.style.display = "block";
      ah.textContent = "Usando fallback de compatibilidad (endpoint nuevo no disponible). Reinicia el servicio web para activar /api/assets/performance.";
    }
  }

  const map = { daily: "d\u00eda", weekly: "semana", monthly: "mes", yearly: "a\u00f1o", accumulated: "acumulado" };
  const t = map[period] || period;
  const badge = el("assetPeriodBadge");
  if (badge) badge.textContent = t;

  const groupSel = el("allocGroupBy");
  const cashChk = el("allocCash");
  const groupBy = groupSel ? groupSel.value : "symbol";
  const includeCash = cashChk && cashChk.checked ? 1 : 0;
  const alloc = await fetchJSON(`/api/allocation?group_by=${encodeURIComponent(groupBy)}&include_cash=${includeCash}`);
  const items = (alloc || []).slice().sort((a, b) => Number(b.value || 0) - Number(a.value || 0));
  const TOP_N = 8;
  const head = items.slice(0, TOP_N);
  const tail = items.slice(TOP_N);
  const tailSum = tail.reduce((s, it) => s + (Number(it.value) || 0), 0);
  if (tailSum > 0) head.push({ key: "Otros", value: tailSum });

  const labelsA = head.map(x => x.key);
  const valuesA = head.map(x => x.value);

  const allocCanvas = el("chartAlloc");
  if (allocCanvas) {
    if (chartAlloc) chartAlloc.destroy();
    const palette = [
      "rgba(103,232,249,0.85)",
      "rgba(52,211,153,0.78)",
      "rgba(251,113,133,0.75)",
      "rgba(196,181,253,0.70)",
      "rgba(252,211,77,0.70)",
      "rgba(147,197,253,0.68)",
      "rgba(253,186,116,0.68)",
      "rgba(110,231,183,0.65)",
      "rgba(248,113,113,0.62)",
      "rgba(129,140,248,0.62)",
    ];
    const colors = labelsA.map((_, i) => palette[i % palette.length]);
    chartAlloc = buildPieChart(allocCanvas, labelsA, valuesA);
    renderAllocLegend("allocLegend", labelsA, valuesA, colors);
  }

  await Promise.all([
    refreshManualCashflows(),
    refreshAutoCashflows(30),
    loadAdvisorTeaser(),
  ]);
}
