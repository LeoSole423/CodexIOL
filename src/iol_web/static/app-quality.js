import {
  el, escHtml, escAttr, signClass,
  fmtARS, fmtNum, fmtPct, fmtDeltaARS, fmtMonthLabel,
  fetchJSON, relativeTime, humanizeEnum, advisorStatusInfo,
  hasValidRange,
} from "./app-utils.js";
import { refreshManualCashflows, refreshAutoCashflows } from "./app-cashflows.js";

let qualityCtx = {
  level: "all",
  activeId: null,
  days: 30,
  warnOnly: false,
  warnQuery: "",
  events: [],
  model: null,
  reconciliation: null,
};

function qualityKindClass(kind) {
  if (kind === "ok") return "health-ok";
  if (kind === "warn") return "health-warn";
  return "health-muted";
}

function buildQualityModel(ret, monthlyKpi, latestSnap) {
  const periodBlocks = [
    { key: "daily", label: "Día", block: ret && ret.daily },
    { key: "weekly", label: "Semana", block: ret && ret.weekly },
    { key: "monthly", label: "Mes", block: ret && ret.monthly },
    { key: "yearly", label: "Año", block: ret && ret.yearly },
    { key: "inception", label: "Desde inicio", block: ret && ret.inception },
  ];

  const warnSet = new Set();
  const warnsBySource = [];
  periodBlocks.forEach((item) => {
    const warns = Array.isArray(item.block && item.block.quality_warnings) ? item.block.quality_warnings : [];
    if (warns.length) {
      warnsBySource.push(`${item.label}: ${warns.join(", ")}`);
    }
    warns.forEach((w) => warnSet.add(String(w)));
  });

  const monthlyWarns = Array.isArray(monthlyKpi && monthlyKpi.quality_warnings) ? monthlyKpi.quality_warnings : [];
  if (monthlyWarns.length) warnsBySource.push(`KPI mensual: ${monthlyWarns.join(", ")}`);
  monthlyWarns.forEach((w) => warnSet.add(String(w)));

  const criticalWarns = ["CASH_MISSING", "ORDERS_INCOMPLETE", "INFERENCE_PARTIAL"];
  const criticalCount = criticalWarns.filter((w) => warnSet.has(w)).length;
  let qualityValue = "OK";
  let qualityKind = "ok";
  let qualityDetail = "No se detectan señales de riesgo en la inferencia.";
  if (criticalCount > 0) {
    qualityValue = `Revisar (${criticalCount})`;
    qualityKind = "warn";
    qualityDetail = "Se detectaron señales críticas de calidad que pueden requerir validación manual.";
  } else if (warnSet.size > 0) {
    qualityValue = "OK (sin operaciones)";
    qualityKind = "info";
    qualityDetail = "Hay señales informativas, pero sin impacto crítico sobre la inferencia.";
  }

  const ipcStatus = String((monthlyKpi && monthlyKpi.status) || "");
  let ipcValue = "Sin dato";
  let ipcKind = "info";
  let ipcDetail = "No hay estado mensual de IPC disponible.";
  if (ipcStatus === "ok" && monthlyKpi && monthlyKpi.inflation_projected) {
    ipcValue = "Estimado";
    ipcKind = "warn";
    ipcDetail = "El IPC del mes actual es estimado con información parcial.";
  } else if (ipcStatus === "ok") {
    ipcValue = "OK";
    ipcKind = "ok";
    ipcDetail = "El cálculo mensual usa un dato IPC confirmado.";
  } else if (ipcStatus === "inflation_unavailable") {
    ipcValue = "No disponible";
    ipcKind = "warn";
    ipcDetail = "No se pudo obtener el IPC para el período evaluado.";
  } else if (ipcStatus === "insufficient_snapshots") {
    ipcValue = "Sin base mensual";
    ipcKind = "info";
    ipcDetail = "Faltan snapshots para construir una base mensual confiable.";
  }

  const coverageCount = periodBlocks.filter((x) => hasValidRange(x.block)).length;
  const missingCoverage = periodBlocks.filter((x) => !hasValidRange(x.block)).map((x) => x.label);
  let coverageKind = "info";
  if (coverageCount >= 4) coverageKind = "ok";
  else if (coverageCount >= 2) coverageKind = "warn";
  const coverageDetail = missingCoverage.length
    ? `Ventanas sin base válida: ${missingCoverage.join(", ")}.`
    : "Todas las ventanas tienen base válida.";

  const rel = latestSnap && latestSnap.retrieved_at ? relativeTime(latestSnap.retrieved_at) : null;
  const updatedValue = rel ? `Actualizado ${rel}` : "Sin timestamp";
  const updatedDetail = latestSnap && latestSnap.retrieved_at
    ? `Último retrieval: ${latestSnap.retrieved_at}.`
    : "No hay timestamp de actualización en el último snapshot.";

  return {
    rows: [
      {
        id: "quality_inference",
        label: "Calidad de inferencia",
        value: qualityValue,
        kind: qualityKind,
        detail: qualityDetail,
        sources: warnsBySource.length ? warnsBySource : ["Sin warnings reportados por ventanas."],
        codes: Array.from(warnSet).sort(),
      },
      {
        id: "ipc_monthly",
        label: "Estado IPC mensual",
        value: ipcValue,
        kind: ipcKind,
        detail: ipcDetail,
        sources: [
          `Status KPI: ${ipcStatus || "n/a"}`,
          `Mes KPI: ${String((monthlyKpi && monthlyKpi.month) || "-")}`,
        ],
        codes: monthlyWarns.map((w) => String(w)),
      },
      {
        id: "coverage_windows",
        label: "Cobertura de ventanas",
        value: `${coverageCount}/5 con base valida`,
        kind: coverageKind,
        detail: coverageDetail,
        sources: periodBlocks.map((x) => {
          const b = x.block || {};
          return `${x.label}: ${String(b.from || "-")} -> ${String(b.to || "-")}`;
        }),
        codes: [],
      },
      {
        id: "updated_at",
        label: "Ultima actualizacion",
        value: updatedValue,
        kind: "info",
        detail: updatedDetail,
        sources: [
          `Snapshot: ${String((latestSnap && latestSnap.snapshot_date) || "-")}`,
          `Retrieved_at: ${String((latestSnap && latestSnap.retrieved_at) || "-")}`,
        ],
        codes: [],
      },
    ],
  };
}

function setHeroHealthStatus(rowId, label, value, kind) {
  const row = el(rowId);
  if (!row) return;
  row.innerHTML = `
    <div class="health-k">${label}</div>
    <div class="health-v ${qualityKindClass(kind)}">${value}</div>
  `;
}

function renderHeroHealth(ret, monthlyKpi, latestSnap) {
  const model = buildQualityModel(ret, monthlyKpi, latestSnap);
  const map = {
    quality_inference: "heroQualityStatus",
    ipc_monthly: "heroIpcStatus",
    coverage_windows: "heroCoverageStatus",
    updated_at: "heroUpdatedStatus",
  };
  (model.rows || []).forEach((row) => {
    const id = map[row.id];
    if (id) setHeroHealthStatus(id, row.label, row.value, row.kind);
  });
}

function renderQualityStatusDetail(model, rowId) {
  const root = el("qualityStatusDetail");
  if (!root) return;
  const rows = (model && model.rows) ? model.rows : [];
  const row = rows.find((r) => r.id === rowId) || rows[0];
  if (!row) {
    root.innerHTML = `<div class="hint">Sin estado disponible.</div>`;
    return;
  }
  const sources = Array.isArray(row.sources) ? row.sources : [];
  const codes = Array.isArray(row.codes) ? row.codes : [];
  const sourcesHtml = sources.length
    ? `<ul class="quality-list-details">${sources.map((s) => `<li>${escHtml(s)}</li>`).join("")}</ul>`
    : `<div class="hint">Sin señales fuente.</div>`;
  const codesHtml = codes.length
    ? `<div class="quality-code-row">${codes.map((c) => `<span class="quality-code">${escHtml(c)}</span>`).join("")}</div>`
    : `<div class="hint">Sin códigos asociados.</div>`;
  root.innerHTML = `
    <div class="quality-detail-head">
      <div class="quality-detail-title">${escHtml(row.label)}</div>
      <div class="quality-detail-value ${qualityKindClass(row.kind)}">${escHtml(row.value)}</div>
    </div>
    <div class="quality-detail-text">${escHtml(row.detail || "-")}</div>
    <div class="quality-detail-block">
      <div class="quality-detail-label">Señales fuente</div>
      ${sourcesHtml}
    </div>
    <div class="quality-detail-block">
      <div class="quality-detail-label">Códigos</div>
      ${codesHtml}
    </div>
  `;
}

function renderQualityStatusList(model) {
  const root = el("qualityStatusList");
  if (!root) return;
  const rows = Array.isArray(model && model.rows) ? model.rows : [];
  const level = String(qualityCtx.level || "all");
  const filtered = rows.filter((r) => level === "all" || r.kind === level);
  if (!filtered.length) {
    root.innerHTML = `<div class="hint">No hay filas para el filtro seleccionado.</div>`;
    renderQualityStatusDetail(model, null);
    return;
  }
  if (!filtered.some((r) => r.id === qualityCtx.activeId)) {
    qualityCtx.activeId = filtered[0].id;
  }
  root.innerHTML = filtered.map((row) => {
    const isActive = row.id === qualityCtx.activeId;
    return `
      <button class="quality-item${isActive ? " active" : ""}" data-quality-id="${escAttr(row.id)}" type="button">
        <span class="quality-item-main">
          <span class="quality-item-label">${escHtml(row.label)}</span>
          <span class="quality-item-value ${qualityKindClass(row.kind)}">${escHtml(row.value)}</span>
        </span>
        <span class="quality-item-sub">${escHtml(row.detail || "")}</span>
      </button>
    `;
  }).join("");
  renderQualityStatusDetail(model, qualityCtx.activeId);
}

function reconciliationActionLabel(row) {
  const t = String((row && row.resolution_type) || "");
  if (t === "manual_adjustment") return "Aceptar ajuste";
  if (t === "ignore_internal") return "Marcar interno";
  if (t === "review_orders") return "Marcar revisado";
  if (t === "import") return "Registrar importación";
  return "Aplicar";
}

function reconciliationSummaryCopy(summary) {
  if (!summary) return "Sin resumen de conciliación.";
  const coverage = humanizeEnum(String(summary.coverage_mode || "none"));
  const open = Number(summary.open_intervals || 0);
  const manual = Number(((summary.manual_stats || {}).recent_rows) || 0);
  const imported = Number(((summary.import_stats || {}).recent_rows) || 0);
  const bits = [`Cobertura ${coverage}`];
  if (imported) bits.push(`${imported} importados recientes`);
  if (manual) bits.push(`${manual} manuales recientes`);
  bits.push(`${open} abiertos`);
  return bits.join(" | ");
}

function renderReconciliationSummary(payload) {
  const root = el("reconciliationSummary");
  if (!root) return;
  const summary = ((payload && payload.run) ? payload.run.summary : null) || {};
  const headline = summary.headline || "Sin ejecución reciente de conciliación.";
  const coverageMode = String(summary.coverage_mode || "none");
  const status = Number(summary.open_intervals || 0) > 0 ? "warn" : (coverageMode === "none" ? "info" : "ok");
  const info = advisorStatusInfo(status);
  root.innerHTML = `
    <div class="quality-detail-head">
      <div class="quality-detail-title">Estado de conciliación</div>
      <div class="quality-detail-value ${qualityKindClass(status)}">${escHtml(info.label)}</div>
    </div>
    <div class="quality-detail-text">${escHtml(headline)}</div>
    <div class="quality-detail-block">
      <div class="quality-detail-label">Resumen</div>
      <div class="quality-detail-text">${escHtml(reconciliationSummaryCopy(summary))}</div>
    </div>
  `;
}

function renderReconciliationOpen(payload) {
  const root = el("tblReconciliationOpen");
  if (!root) return;
  const rows = (payload && payload.rows) ? payload.rows : [];
  if (!rows.length) {
    root.innerHTML = `<div class="hint">No hay propuestas abiertas. La conciliación no detectó acciones pendientes.</div>`;
    return;
  }
  const body = rows.map((row) => {
    const interval = row.interval || {};
    const amount = row.suggested_amount_ars == null ? "-" : fmtARS.format(Number(row.suggested_amount_ars || 0));
    const badge = advisorStatusInfo((row.impact_on_inference === "clears_warning") ? "ok" : "warn");
    return `
      <tr>
        <td>${escHtml(String(interval.end_snapshot_date || interval.base_snapshot_date || "-"))}</td>
        <td>${escHtml(humanizeEnum(row.issue_code || "-"))}</td>
        <td>${escHtml(row.reason || "-")}</td>
        <td>${escHtml(humanizeEnum(row.resolution_type || "-"))}</td>
        <td class="num">${escHtml(amount)}</td>
        <td><span class="${escAttr(badge.className)}">${escHtml(badge.label)}</span></td>
        <td class="num">
          <div class="cashflow-row-actions">
            <button class="seg" type="button" data-recon-apply="${escAttr(row.id)}">${escHtml(reconciliationActionLabel(row))}</button>
            <button class="seg" type="button" data-recon-dismiss="${escAttr(row.id)}">Descartar</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");
  root.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Fecha</th>
          <th>Problema</th>
          <th>Motivo</th>
          <th>Acción sugerida</th>
          <th class="num">Monto</th>
          <th>Impacto</th>
          <th class="num">Acciones</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

async function refreshReconciliation() {
  if (!el("tblReconciliationOpen")) return;
  try {
    const payload = await fetchJSON("/api/reconciliation/open");
    const hint = el("reconciliationHint");
    if (hint) hint.style.display = "none";
    qualityCtx.reconciliation = payload;
    renderReconciliationSummary(payload);
    renderReconciliationOpen(payload);
  } catch (e) {
    const hint = el("reconciliationHint");
    if (hint) {
      hint.style.display = "block";
      hint.textContent = `No se pudo cargar la conciliación: ${e && e.message ? e.message : "error"}`;
    }
    const root = el("tblReconciliationOpen");
    if (root) root.innerHTML = `<div class="hint">No se pudo cargar la cola de conciliación.</div>`;
  }
}

function renderQualityEventsTable(rows) {
  const root = el("qualityEventsTable");
  if (!root) return;
  const list = Array.isArray(rows) ? rows : [];
  const onlyWarn = !!qualityCtx.warnOnly;
  const q = String(qualityCtx.warnQuery || "").trim().toUpperCase();
  const filtered = list.filter((r) => {
    const warns = Array.isArray(r && r.quality_warnings) ? r.quality_warnings.map((w) => String(w)) : [];
    if (onlyWarn && warns.length === 0) return false;
    if (!q) return true;
    const hay = warns.join(" ").toUpperCase();
    return hay.includes(q);
  });
  if (!filtered.length) {
    root.innerHTML = `<div class="hint">Sin eventos para los filtros elegidos.</div>`;
    return;
  }
  const head = `
    <th>Fecha</th>
    <th>Etiqueta</th>
    <th>Reason</th>
    <th>Warnings</th>
    <th class="num">Monto neto</th>
    <th class="num">FX</th>
    <th class="num">Imp. interno</th>
    <th class="num">Imp. externo</th>
  `;
  const body = filtered.map((r) => {
    const warns = Array.isArray(r && r.quality_warnings) ? r.quality_warnings.map((w) => String(w)) : [];
    const warnText = warns.length ? warns.join(", ") : "-";
    const label = String(r.display_label || r.display_kind || "-");
    const reason = String(r.reason_code || "-");
    return `
      <tr>
        <td>${escHtml(r.flow_date || "-")}</td>
        <td>${escHtml(label)}</td>
        <td><code>${escHtml(reason)}</code></td>
        <td>${escHtml(warnText)}</td>
        <td class="num ${signClass(r.amount_ars)}">${fmtDeltaARS(r.amount_ars)}</td>
        <td class="num ${signClass(r.fx_revaluation_ars)}">${fmtDeltaARS(r.fx_revaluation_ars || 0)}</td>
        <td class="num ${signClass(r.imported_internal_ars)}">${fmtDeltaARS(r.imported_internal_ars || 0)}</td>
        <td class="num ${signClass(r.imported_external_ars)}">${fmtDeltaARS(r.imported_external_ars || 0)}</td>
      </tr>
    `;
  }).join("");
  root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function getQualityDays() {
  const wrap = el("qualityDaysButtons");
  if (!wrap) return qualityCtx.days || 30;
  const on = wrap.querySelector("button.on");
  const d = parseInt(on ? on.getAttribute("data-days") : "", 10);
  if (Number.isFinite(d) && d > 0) return d;
  return qualityCtx.days || 30;
}

async function loadQualityEvents(days) {
  const d = parseInt(days || 30, 10) || 30;
  qualityCtx.days = d;
  try {
    const out = await fetchJSON(`/api/cashflows/auto?days=${encodeURIComponent(d)}`);
    qualityCtx.events = (out && Array.isArray(out.rows)) ? out.rows : [];
    renderQualityEventsTable(qualityCtx.events);
  } catch (_) {
    qualityCtx.events = [];
    const root = el("qualityEventsTable");
    if (root) root.innerHTML = `<div class="hint">No se pudieron cargar los eventos recientes.</div>`;
  }
}

export async function loadQualityPage() {
  let model = null;
  try {
    model = await fetchJSON("/api/quality");
  } catch (_) {
    const [latest, ret, monthlyKpi] = await Promise.all([
      fetchJSON("/api/latest").catch(() => null),
      fetchJSON("/api/returns").catch(() => null),
      fetchJSON("/api/kpi/monthly-vs-inflation").catch(() => null),
    ]);
    const snap = latest && latest.snapshot ? latest.snapshot : null;
    model = buildQualityModel(ret || {}, monthlyKpi || {}, snap);
  }
  qualityCtx.model = model;
  if (!qualityCtx.activeId) {
    const firstWarn = (model.rows || []).find((r) => r.kind === "warn");
    qualityCtx.activeId = (firstWarn || (model.rows || [])[0] || {}).id || null;
  }
  renderQualityStatusList(model);
  await Promise.all([
    refreshReconciliation(),
    refreshManualCashflows(),
    refreshAutoCashflows(30),
  ]);
}

export function initQualityPageControls() {
  const severityWrap = el("qualitySeverityButtons");
  if (severityWrap) {
    severityWrap.addEventListener("click", (ev) => {
      const btn = ev.target && ev.target.closest("button");
      if (!btn) return;
      const level = String(btn.getAttribute("data-level") || "all");
      qualityCtx.level = level;
      Array.from(severityWrap.querySelectorAll("button")).forEach((b) => b.classList.remove("on"));
      btn.classList.add("on");
      renderQualityStatusList(qualityCtx.model || { rows: [] });
    });
  }

  const list = el("qualityStatusList");
  if (list) {
    list.addEventListener("click", (ev) => {
      const btn = ev.target && ev.target.closest("button[data-quality-id]");
      if (!btn) return;
      qualityCtx.activeId = String(btn.getAttribute("data-quality-id") || "");
      renderQualityStatusList(qualityCtx.model || { rows: [] });
    });
  }

  const reconciliationTable = el("tblReconciliationOpen");
  if (reconciliationTable) {
    reconciliationTable.addEventListener("click", async (ev) => {
      const applyBtn = ev.target && ev.target.closest("[data-recon-apply]");
      const dismissBtn = ev.target && ev.target.closest("[data-recon-dismiss]");
      if (!applyBtn && !dismissBtn) return;
      const hint = el("reconciliationHint");
      const proposalId = applyBtn
        ? applyBtn.getAttribute("data-recon-apply")
        : dismissBtn.getAttribute("data-recon-dismiss");
      if (!proposalId) return;
      try {
        if (applyBtn) {
          await fetchJSON("/api/reconciliation/apply", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ proposal_id: Number(proposalId) }),
          });
          if (hint) {
            hint.style.display = "block";
            hint.textContent = "Propuesta aplicada.";
          }
        } else {
          const reason = window.prompt("Motivo para descartar la propuesta:");
          if (!reason) return;
          await fetchJSON("/api/reconciliation/dismiss", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ proposal_id: Number(proposalId), reason }),
          });
          if (hint) {
            hint.style.display = "block";
            hint.textContent = "Propuesta descartada.";
          }
        }
        await loadQualityPage();
      } catch (e) {
        if (hint) {
          hint.style.display = "block";
          hint.textContent = `No se pudo actualizar la propuesta: ${e && e.message ? e.message : "error"}`;
        }
      }
    });
  }

  const daysWrap = el("qualityDaysButtons");
  if (daysWrap) {
    daysWrap.addEventListener("click", async (ev) => {
      const btn = ev.target && ev.target.closest("button");
      if (!btn) return;
      const days = parseInt(btn.getAttribute("data-days") || "", 10);
      if (!Number.isFinite(days) || days <= 0) return;
      Array.from(daysWrap.querySelectorAll("button")).forEach((b) => b.classList.remove("on"));
      btn.classList.add("on");
      await loadQualityEvents(days);
    });
  }

  const warnOnly = el("qualityWarnOnly");
  if (warnOnly) {
    warnOnly.addEventListener("change", () => {
      qualityCtx.warnOnly = !!warnOnly.checked;
      renderQualityEventsTable(qualityCtx.events || []);
    });
  }

  const search = el("qualityWarnSearch");
  if (search) {
    search.addEventListener("input", () => {
      qualityCtx.warnQuery = String(search.value || "");
      renderQualityEventsTable(qualityCtx.events || []);
    });
  }
}
