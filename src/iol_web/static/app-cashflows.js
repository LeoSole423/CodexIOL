import { el, escHtml, escAttr, signClass, fmtDeltaARS, fetchJSON } from "./app-utils.js";

function renderManualCashflows(rows) {
  const root = el("tblCashflowsManual");
  if (!root) return;
  const list = rows || [];
  if (!list.length) {
    root.innerHTML = `<div class="hint">Sin ajustes manuales.</div>`;
    return;
  }
  const head = `
    <th>Fecha</th>
    <th>Tipo</th>
    <th class="num">Monto</th>
    <th>Nota</th>
    <th class="num">Acciones</th>
  `;
  const body = list.map((r) => {
    return `
      <tr>
        <td>${r.flow_date || "-"}</td>
        <td>${r.kind || "-"}</td>
        <td class="num ${signClass(r.amount_ars)}">${fmtDeltaARS(r.amount_ars)}</td>
        <td class="muted">${(r.note || "").slice(0, 120)}</td>
        <td class="num">
          <div class="cashflow-row-actions">
            <button class="btn-link" data-cashflow-del="${r.id}">Eliminar</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");
  root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function renderAutoCashflows(rows) {
  const root = el("tblCashflowsAuto");
  if (!root) return;
  const list = rows || [];
  if (!list.length) {
    root.innerHTML = `<div class="hint">Sin flujos autom&aacute;ticos detectados.</div>`;
    return;
  }

  function toNumber(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }

  function calcResidualRatio(row) {
    const fromApi = Number(row && row.residual_ratio);
    if (Number.isFinite(fromApi) && fromApi >= 0) return fromApi;
    const buy = Math.abs(toNumber(row && row.buy_amount_ars));
    const sell = Math.abs(toNumber(row && row.sell_amount_ars));
    const income = Math.abs(toNumber(row && row.income_amount_ars));
    const traded = buy + sell + income;
    if (traded <= 0) return null;
    return Math.abs(toNumber(row && row.amount_ars)) / traded;
  }

  function fallbackKindMeta(row) {
    const k = String((row && row.kind) || "").toLowerCase();
    const warnings = Array.isArray(row.quality_warnings) ? row.quality_warnings : [];
    if (warnings.includes("CASH_MISSING") || warnings.includes("ORDERS_INCOMPLETE") || k === "correction") {
      return {
        label: "Correcci&oacute;n",
        className: "row-badge row-badge-correction",
        detail: "Datos incompletos de caja/&oacute;rdenes; revisar manualmente.",
      };
    }
    const residual = calcResidualRatio(row);
    if (k === "withdraw" && residual != null && residual <= 0.03) {
      return {
        label: "Costo/Impuesto operativo",
        className: "row-badge row-badge-operational",
        detail: "Salida interna probable por costos/impuestos.",
      };
    }
    const amount = toNumber(row && row.amount_ars);
    return {
      label: amount >= 0 ? "Flujo externo probable (+)" : "Flujo externo probable (-)",
      className: "row-badge row-badge-external",
      detail: "Clasificado por signo del flujo neto inferido.",
    };
  }

  function apiKindMeta(row) {
    const dk = String((row && row.display_kind) || "").toLowerCase();
    const label = String((row && row.display_label) || "").trim();
    if (!dk && !label) return null;
    const cls = {
      correction_unknown: "row-badge row-badge-correction",
      operational_fee_or_tax: "row-badge row-badge-operational",
      dividend_or_coupon_income: "row-badge row-badge-income",
      rotation_internal: "row-badge row-badge-rotation",
      settlement_carryover: "row-badge row-badge-settlement",
      fx_revaluation_usd_cash: "row-badge row-badge-fx",
      external_deposit_probable: "row-badge row-badge-external",
      external_withdraw_probable: "row-badge row-badge-external",
    };
    return {
      label: label || "-",
      className: cls[dk] || "",
      detail: String((row && row.reason_detail) || "").trim(),
    };
  }

  function kindMeta(row) {
    const fromApi = apiKindMeta(row);
    if (fromApi) return fromApi;
    return fallbackKindMeta(row);
  }

  const head = `
    <th>Fecha</th>
    <th>Tipo</th>
    <th class="num">Monto neto</th>
    <th class="num">Caja</th>
    <th class="num">Compras</th>
    <th class="num">Ventas</th>
    <th class="num">Ingresos</th>
    <th>Warning</th>
  `;
  const body = list.map((r) => {
    const warns = Array.isArray(r.quality_warnings) ? r.quality_warnings : [];
    const hasWarn = warns.length > 0;
    const warnTxt = hasWarn ? warns.join(", ") : "-";
    const kind = kindMeta(r);
    const compDetail = [
      `Ext final ${fmtDeltaARS(r.external_final_ars != null ? r.external_final_ars : r.amount_ars)}`,
      `Ext raw ${fmtDeltaARS(r.external_raw_ars || 0)}`,
      `FX ${fmtDeltaARS(r.fx_revaluation_ars || 0)}`,
      `Imp int ${fmtDeltaARS(r.imported_internal_ars || 0)}`,
      `Imp ext ${fmtDeltaARS(r.imported_external_ars || 0)}`,
    ].join(" | ");
    const tooltip = [kind.detail, compDetail].filter(Boolean).join(" | ");
    const warnBadge = hasWarn
      ? `<span class="row-badge row-badge-correction">${warnTxt}</span>`
      : `<span class="muted">-</span>`;
    const kindCell = kind.className
      ? `<span class="${kind.className}" title="${escAttr(tooltip)}">${kind.label}</span>`
      : `<span title="${escAttr(tooltip)}">${kind.label || "-"}</span>`;
    return `
      <tr>
        <td>${r.flow_date || "-"}</td>
        <td>${kindCell}</td>
        <td class="num ${signClass(r.amount_ars)}">${fmtDeltaARS(r.amount_ars)}</td>
        <td class="num ${signClass(r.cash_delta_ars)}">${fmtDeltaARS(r.cash_delta_ars)}</td>
        <td class="num">${fmtDeltaARS(r.buy_amount_ars || 0)}</td>
        <td class="num">${fmtDeltaARS(r.sell_amount_ars || 0)}</td>
        <td class="num">${fmtDeltaARS(r.income_amount_ars || 0)}</td>
        <td>${warnBadge}</td>
      </tr>
    `;
  }).join("");
  root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

export async function refreshManualCashflows() {
  if (!el("tblCashflowsManual")) return;
  try {
    const rows = await fetchJSON("/api/cashflows/manual");
    renderManualCashflows(rows);
  } catch (e) {
    const root = el("tblCashflowsManual");
    if (root) root.innerHTML = `<div class="hint">No se pudieron cargar los ajustes manuales.</div>`;
  }
}

export async function refreshAutoCashflows(days) {
  if (!el("tblCashflowsAuto")) return;
  const d = parseInt(days || 30, 10) || 30;
  try {
    const out = await fetchJSON(`/api/cashflows/auto?days=${encodeURIComponent(d)}`);
    renderAutoCashflows((out && out.rows) ? out.rows : []);
  } catch (_) {
    const root = el("tblCashflowsAuto");
    if (root) root.innerHTML = `<div class="hint">No se pudieron cargar los flujos autom&aacute;ticos.</div>`;
  }
}
