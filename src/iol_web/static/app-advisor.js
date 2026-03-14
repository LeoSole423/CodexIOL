import {
  el,
  escHtml,
  escAttr,
  signClass,
  fmtPct,
  fmtDeltaARS,
  fmtARS,
  fmtNum,
  fetchJSON,
  relativeTime,
  humanizeEnum,
  advisorStatusInfo,
} from "./app-utils.js";

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

function fmtDateTimeShort(value) {
  if (!value) return "-";
  const dt = new Date(value);
  if (isNaN(dt.getTime())) return String(value);
  return new Intl.DateTimeFormat("es-AR", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(dt);
}

function advisorPolicyLabel(value) {
  const src = String(value || "").trim().toLowerCase();
  if (src === "strict_official_reuters") return "DB local + oficiales/Reuters";
  if (!src) return "Fuentes no informadas";
  return humanizeEnum(src);
}

function advisorGroupCounts(recommendations, watchlist) {
  const counts = { actionable: 0, conditional: 0, watchlist: 0, blocked: 0, total: 0 };
  const all = []
    .concat(recommendations || [])
    .concat(watchlist || []);
  for (const row of all) {
    const key = String((row && row.status) || "").toLowerCase();
    if (Object.prototype.hasOwnProperty.call(counts, key)) counts[key] += 1;
    counts.total += 1;
  }
  return counts;
}

function advisorWarningLabels(briefing) {
  const labels = (((briefing || {}).quality || {}).summary || {}).labels;
  if (Array.isArray(labels) && labels.length) return labels.filter(Boolean).slice(0, 4);
  const rows = (((briefing || {}).quality || {}).rows || [])
    .filter((row) => ["warn", "blocked", "error"].includes(String((row && row.kind) || "").toLowerCase()))
    .map((row) => row.label)
    .filter(Boolean);
  return rows.slice(0, 4);
}

function advisorPrimaryNextStep(briefing) {
  if (!briefing) return null;
  const primary = (briefing.recommendations || []).find((row) => row && row.next_step);
  if (primary && primary.next_step) return primary.next_step;
  const fallback = (briefing.watchlist || []).find((row) => row && row.next_step);
  return fallback ? fallback.next_step : null;
}

function advisorMetricTile(label, value, extraClass) {
  return `
    <div class="kpi-insight">
      <div class="k">${escHtml(label)}</div>
      <div class="v${extraClass ? ` ${extraClass}` : ""}">${escHtml(value)}</div>
    </div>
  `;
}

function advisorBriefingCopy(briefing) {
  if (!briefing) return "Sin briefing generado todavía.";
  const counts = advisorGroupCounts(briefing.recommendations, briefing.watchlist);
  const fragments = [];
  if (counts.actionable) fragments.push(`${counts.actionable} accionables`);
  if (counts.conditional) fragments.push(`${counts.conditional} condicionales`);
  if (counts.watchlist) fragments.push(`${counts.watchlist} en watchlist`);
  if (!fragments.length) fragments.push("sin ideas activas");
  const warnings = advisorWarningLabels(briefing);
  if (warnings.length) fragments.push(`alertas: ${warnings.join(", ")}`);
  return fragments.join(" | ");
}

function advisorReasonView(row) {
  const raw = String((row && row.reason) || "").trim();
  if (!raw) return { body: "-", chips: [] };
  if (!raw.includes("|")) return { body: raw, chips: [] };
  const parts = raw.split("|").map((part) => part.trim()).filter(Boolean);
  const body = [];
  const chips = [];
  for (const part of parts) {
    const tokens = (part.includes("=") && /\s/.test(part)) ? part.split(/\s+/).filter(Boolean) : [part];
    let consumed = false;
    for (const token of tokens) {
      const eq = token.indexOf("=");
      if (eq <= 0) continue;
      consumed = true;
      const key = token.slice(0, eq).trim().toLowerCase();
      const val = token.slice(eq + 1).trim();
      if (key === "dd20") body.push(`Drawdown 20d ${val}`);
      else if (key === "consensus") body.push(`Consenso ${val}`);
      else if (key === "status") body.push(`Filtro ${val}`);
      else if (key === "gate") chips.push(`gate ${val}`);
      else if (key === "sector") chips.push(`sector ${val}`);
      else if (key === "liq") chips.push(`liq ${val}`);
      else if (key === "refs") chips.push(`refs ${val}`);
      else if (["risk", "value", "momentum", "catalyst", "expert"].includes(key)) chips.push(`${key} ${val}`);
    }
    if (!consumed && !body.length) {
      body.push(part === "new" ? "Idea nueva" : humanizeEnum(part));
    }
  }
  return {
    body: body.length ? body.join(" | ") : raw,
    chips,
  };
}

export async function loadAdvisorTeaser() {
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

function advisorResolveBucket(row) {
  const explicit = String((row && row.action_bucket) || "").trim().toLowerCase();
  if (explicit) return explicit;
  const status = String((row && row.status) || "").trim().toLowerCase();
  const signalSide = String((row && row.signal_side) || "").trim().toLowerCase();
  const symbol = String((row && row.symbol) || "").trim();
  if (!symbol) return "review";
  if (status === "watchlist") return "wait";
  if (signalSide === "sell") return "sell";
  return "buy";
}

function advisorActionLabel(bucket) {
  const key = String(bucket || "").trim().toLowerCase();
  if (key === "buy") return "Comprar";
  if (key === "sell") return "Reducir / vender";
  if (key === "wait") return "Esperar";
  return "Revisar";
}

function advisorFriendlyFlag(flag) {
  const key = String(flag || "").trim().toUpperCase();
  const labels = {
    CASHFLOW_IMPORT_MISSING: "Faltan movimientos",
    EVIDENCE_CONFLICT: "Evidencia en conflicto",
    EVIDENCE_INSUFFICIENT: "Falta evidencia",
    NO_WEEKLY_RUN: "Falta corrida semanal",
    ORDERS_INCOMPLETE: "Movimientos incompletos",
    ORDERS_NONE: "Sin movimientos cargados",
    QUALITY_INFERENCE_WARN: "Datos incompletos",
    SELL_EXIT: "Salida fuerte",
    SELL_TRIM: "Reducir posicion",
  };
  return labels[key] || humanizeEnum(key);
}

function advisorConfidenceLabel(row) {
  const status = String((row && row.status) || "").trim().toLowerCase();
  const refs = Number((row && row.evidence_count) || 0);
  if (status === "actionable" && refs >= 2) return "Alta";
  if (status === "watchlist" || refs === 0) return "Baja";
  return "Media";
}

function advisorShortReason(row) {
  const explicit = String((row && row.short_reason) || "").trim();
  if (explicit) return explicit;
  const bucket = advisorResolveBucket(row);
  const refs = Number((row && row.evidence_count) || 0);
  if (bucket === "sell") return "Reducir posicion para bajar riesgo o deterioro.";
  if (bucket === "wait") {
    return refs > 0
      ? "Idea en seguimiento; todavia no esta lista para operar."
      : "Seguir en observacion hasta tener mas evidencia.";
  }
  if (bucket === "review" && !String((row && row.symbol) || "").trim()) {
    return String((row && row.reason) || (row && row.title) || "Hay una validacion pendiente antes de operar.").trim();
  }
  if (bucket === "buy") {
    return refs >= 2
      ? "Compra posible, pero requiere validacion antes de operar."
      : "Idea interesante, pero falta confirmacion antes de operar.";
  }
  const parsed = advisorReasonView(row);
  return parsed.body || String((row && row.reason) || "").trim() || "-";
}

function advisorIsBlocking(row) {
  if (row && row.is_blocking != null) return Boolean(row.is_blocking);
  return !String((row && row.symbol) || "").trim() || String((row && row.status) || "").toLowerCase() === "blocked";
}

function advisorReconciliationTitle(row) {
  const issue = String((row && row.issue_code) || "").trim().toUpperCase();
  if (issue === "MISSING_IMPORTED_MOVEMENT") return "Registrar movimiento faltante";
  if (issue === "INFERENCE_PARTIAL") return "Confirmar flujo externo";
  if (issue === "ORDERS_INCOMPLETE") return "Revisar ordenes incompletas";
  if (issue === "OPERATIONAL_FEE_OR_TAX") return "Marcar movimiento interno";
  return humanizeEnum(issue || String((row && row.resolution_type) || "revision"));
}

function advisorReconciliationNextStep(row) {
  const resolution = String((row && row.resolution_type) || "").trim().toLowerCase();
  if (resolution === "manual_adjustment") return "Si confirmas que fue un aporte o retiro, acepta el ajuste auditado.";
  if (resolution === "ignore_internal") return "Marcado como interno para que deje de bloquear la inferencia.";
  if (resolution === "review_orders") return "Completa snapshots u ordenes faltantes y luego marca el caso como revisado.";
  if (resolution === "import") return "Registra la importacion o confirma el movimiento en Calidad.";
  return "Revisa el caso en Calidad antes de operar.";
}

function reconciliationActionLabel(row) {
  const t = String((row && row.resolution_type) || "");
  if (t === "manual_adjustment") return "Aceptar ajuste";
  if (t === "ignore_internal") return "Marcar interno";
  if (t === "review_orders") return "Marcar revisado";
  if (t === "import") return "Registrar importación";
  return "Aplicar";
}

function advisorReconciliationBlockers(reconciliationPayload) {
  const rows = ((reconciliationPayload && reconciliationPayload.rows) || []).slice(0, 3);
  return rows.map((row, idx) => {
    const resolution = String((row && row.resolution_type) || "").trim().toLowerCase();
    const interval = row.interval || {};
    const dateLabel = fmtDateShort(interval.end_snapshot_date || interval.base_snapshot_date || reconciliationPayload && reconciliationPayload.run && reconciliationPayload.run.as_of || "");
    const status = resolution === "review_orders" ? "warn" : "conditional";
    const qualityFlags = []
      .concat(row.issue_code ? [String(row.issue_code)] : [])
      .concat((row.source_basis || []).map((bit) => String(bit)));
    return {
      status,
      symbol: null,
      title: advisorReconciliationTitle(row),
      reason: row.reason || "Hay un caso pendiente de conciliacion que afecta la decision de hoy.",
      short_reason: row.reason || "Hay un caso pendiente de conciliacion.",
      action_bucket: "review",
      is_blocking: true,
      evidence_count: 0,
      quality_flags: qualityFlags,
      next_step: advisorReconciliationNextStep(row),
      suggested_amount_ars: row.suggested_amount_ars,
      priority_rank: idx + 1,
      reconciliation_proposal_id: row.id,
      reconciliation_resolution_type: row.resolution_type,
      reconciliation_issue_code: row.issue_code,
      reconciliation_date_label: dateLabel,
      reconciliation_cta_label: reconciliationActionLabel(row),
      reconciliation_href: "/quality",
      reconciliation_confidence: row.confidence,
      reconciliation_impact: row.impact_on_inference,
    };
  });
}

function advisorVisibleActions(briefing) {
  return ((briefing && briefing.recommendations) || []).filter((row) => !advisorIsBlocking(row));
}

function advisorBlockingItems(briefing, reconciliationPayload) {
  const reconBlockers = advisorReconciliationBlockers(reconciliationPayload);
  const merged = []
    .concat((briefing && briefing.recommendations) || [])
    .concat((briefing && briefing.watchlist) || []);
  const hideGenericInference = reconBlockers.length > 0;
  const seen = new Set();
  const briefingBlockers = merged.filter((row) => advisorIsBlocking(row)).filter((row) => {
    if (hideGenericInference && !String((row && row.symbol) || "").trim()) {
      const title = String((row && row.title) || "").trim().toLowerCase();
      if (title === "revisar calidad de inferencia" || title === "cobertura de cashflows incompleta") {
        return false;
      }
    }
    const key = `${row && row.symbol ? row.symbol : "system"}:${row && row.title ? row.title : row && row.reason ? row.reason : ""}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  return reconBlockers.concat(briefingBlockers).slice(0, 3);
}

function advisorWatchlistItems(briefing) {
  return ((briefing && briefing.watchlist) || []).filter((row) => !advisorIsBlocking(row));
}

function advisorPrimaryHeadline(briefing, actions, blockers) {
  const counts = { buy: 0, sell: 0 };
  for (const row of actions || []) {
    const bucket = advisorResolveBucket(row);
    if (bucket === "buy") counts.buy += 1;
    if (bucket === "sell") counts.sell += 1;
  }
  if (blockers.length && !actions.length) return `${blockers.length} validaciones antes de operar`;
  if (counts.sell > 0) return `${counts.sell} posiciones para reducir o vender`;
  if (counts.buy > 0) return `${counts.buy} oportunidades para revisar hoy`;
  if (advisorWatchlistItems(briefing).length > 0) return "Hoy conviene esperar";
  return "Sin acciones urgentes para hoy";
}

function advisorAnalysisDetails(row) {
  if (advisorIsBlocking(row) && !String((row && row.symbol) || "").trim()) return "";
  const reason = advisorReasonView(row);
  const chips = []
    .concat((reason.chips || []).map((chip) => `<span class="quality-code">${escHtml(chip)}</span>`))
    .concat((row.quality_flags || []).map((flag) => `<span class="quality-code">${escHtml(advisorFriendlyFlag(flag))}</span>`));
  const details = [];
  if (reason.body && reason.body !== advisorShortReason(row)) details.push(`<div class="muted">${escHtml(reason.body)}</div>`);
  if (chips.length) details.push(`<div class="quality-code-row">${chips.join("")}</div>`);
  if (row.score_total != null || row.score_version) {
    const meta = [];
    if (row.score_total != null) meta.push(`score ${fmtNum.format(row.score_total)}`);
    if (row.score_version) meta.push(String(row.score_version));
    details.push(`<div class="muted">${escHtml(meta.join(" | "))}</div>`);
  }
  if (!details.length) return "";
  return `
    <details class="advisor-inline-details">
      <summary>Ver analisis</summary>
      <div class="advisor-inline-content">${details.join("")}</div>
    </details>
  `;
}

function advisorItemCard(row) {
  const bucket = advisorResolveBucket(row);
  const systemItem = !String(row.symbol || "").trim();
  const status = advisorIsBlocking(row) && bucket === "review"
    ? advisorStatusInfo("warn")
    : advisorStatusInfo(row.status);
  const title = String(row.symbol || row.title || "Sistema");
  const meta = [];
  if (!systemItem) {
    meta.push(`<span class="quality-code">${escHtml(advisorActionLabel(bucket))}</span>`);
    meta.push(`<span class="quality-code">Confianza ${escHtml(advisorConfidenceLabel(row))}</span>`);
    if (row.evidence_count != null) meta.push(`<span class="quality-code">refs ${escHtml(row.evidence_count)}</span>`);
    if (row.entry_low != null && row.entry_high != null) meta.push(`<span class="quality-code">entrada ${escHtml(fmtARS.format(row.entry_low))} - ${escHtml(fmtARS.format(row.entry_high))}</span>`);
    if (row.suggested_amount_ars != null) meta.push(`<span class="quality-code">monto ${escHtml(fmtARS.format(row.suggested_amount_ars))}</span>`);
  } else if (row.reconciliation_date_label) {
    meta.push(`<span class="quality-code">Fecha ${escHtml(row.reconciliation_date_label)}</span>`);
    if (row.suggested_amount_ars != null) meta.push(`<span class="quality-code">monto ${escHtml(fmtARS.format(row.suggested_amount_ars))}</span>`);
    if (row.reconciliation_impact === "clears_warning") meta.push(`<span class="quality-code">Destraba inferencia</span>`);
  }
  const actions = [];
  if (row.reconciliation_proposal_id != null) {
    actions.push(`<button class="seg" type="button" data-advisor-recon-apply="${escAttr(row.reconciliation_proposal_id)}">${escHtml(row.reconciliation_cta_label || "Resolver")}</button>`);
    actions.push(`<a class="seg advisor-inline-link" href="${escAttr(row.reconciliation_href || "/quality")}">Abrir calidad</a>`);
  }
  return `
    <div class="advisor-item advisor-action-card advisor-bucket-${escHtml(bucket)}${advisorIsBlocking(row) ? " advisor-item-blocking" : ""}${systemItem ? " advisor-item-system" : ""}">
      <div class="advisor-action-head">
        <div>
          <div class="advisor-action-kicker">${escHtml(systemItem ? "Bloqueo" : advisorActionLabel(bucket))}${!systemItem && row.priority_rank != null ? ` #${escHtml(row.priority_rank)}` : ""}</div>
          <div class="advisor-item-title">${escHtml(title)}</div>
        </div>
        <span class="${status.className}">${escHtml(status.label)}</span>
      </div>
      <div class="advisor-item-body advisor-action-summary">${escHtml(advisorShortReason(row))}</div>
      ${meta.length ? `<div class="quality-code-row">${meta.join("")}</div>` : ""}
      <div class="advisor-item-next"><strong>Proximo paso:</strong> ${escHtml(row.next_step || "Sin accion inmediata.")}</div>
      ${actions.length ? `<div class="cashflow-row-actions advisor-item-actions">${actions.join("")}</div>` : ""}
      ${advisorAnalysisDetails(row)}
    </div>
  `;
}

function renderAdvisorDecisionHero(briefing, latestRun, selectedCadence, reconciliationPayload) {
  const headlineEl = el("advisorDecisionHeadline");
  const copyEl = el("advisorDecisionCopy");
  const statusEl = el("advisorDecisionStatus");
  const metaEl = el("advisorDecisionMeta");
  const countsEl = el("advisorDecisionCounts");
  const focusEl = el("advisorDecisionFocus");
  if (!headlineEl || !copyEl || !statusEl || !metaEl || !countsEl || !focusEl) return;
  if (!briefing) {
    statusEl.className = "badge";
    statusEl.textContent = "-";
    headlineEl.textContent = "Todavia no hay briefings publicados";
    copyEl.textContent = "Genera un briefing diario o semanal para activar esta vista.";
    metaEl.textContent = "Sin datos.";
    countsEl.innerHTML = advisorMetricTile("Comprar", "0") + advisorMetricTile("Reducir/Vender", "0") + advisorMetricTile("Revisar", "0");
    focusEl.textContent = "Todavia no hay una accion concreta para priorizar.";
    return;
  }

  const actions = advisorVisibleActions(briefing);
  const blockers = advisorBlockingItems(briefing, reconciliationPayload);
  const watchlist = advisorWatchlistItems(briefing);
  const info = advisorStatusInfo(briefing.status);
  const counts = {
    buy: actions.filter((row) => advisorResolveBucket(row) === "buy").length,
    sell: actions.filter((row) => advisorResolveBucket(row) === "sell").length,
    review: blockers.length + actions.filter((row) => advisorResolveBucket(row) === "review").length,
  };
  const primaryRow = blockers[0] || actions[0] || watchlist[0] || null;
  const metaBits = [
    selectedCadence === "weekly" ? "Vista semanal" : "Vista de hoy",
    `Corte ${fmtDateShort(briefing.as_of)}`,
    `Publicado ${fmtDateTimeShort(briefing.created_at_utc)}`,
    advisorPolicyLabel(briefing.source_policy),
  ];
  if (selectedCadence === "weekly" && briefing.opportunity_run_id != null) metaBits.push(`run ${briefing.opportunity_run_id}`);
  else if (latestRun && latestRun.id != null) metaBits.push(`run ${latestRun.id}`);

  statusEl.className = info.className;
  statusEl.textContent = info.label;
  headlineEl.textContent = advisorPrimaryHeadline(briefing, actions, blockers);
  if (primaryRow) {
    const title = String(primaryRow.symbol || primaryRow.title || "Sistema");
    copyEl.textContent = `${advisorActionLabel(advisorResolveBucket(primaryRow))}: ${title}. ${advisorShortReason(primaryRow)}`;
    focusEl.innerHTML = `<strong>Siguiente paso:</strong> ${escHtml(primaryRow.next_step || "Sin accion inmediata.")}`;
  } else {
    copyEl.textContent = "No hay acciones listas para ejecutar; revisa el seguimiento y la salud del asesor.";
    focusEl.textContent = "Siguiente paso: esperar el proximo briefing o completar informacion faltante.";
  }
  metaEl.textContent = metaBits.join(" | ");
  countsEl.innerHTML = [
    advisorMetricTile("Comprar", String(counts.buy), counts.buy ? "positive" : ""),
    advisorMetricTile("Reducir/Vender", String(counts.sell), counts.sell ? "negative" : ""),
    advisorMetricTile("Revisar", String(counts.review), counts.review ? "warn" : ""),
  ].join("");
}

function renderAdvisorBlockers(briefing, reconciliationPayload) {
  const card = el("advisorBlockersCard");
  const metaEl = el("advisorBlockersMeta");
  const hintEl = el("advisorBlockersHint");
  const root = el("advisorBlockers");
  if (!card || !metaEl || !root) return;
  const blockers = briefing ? advisorBlockingItems(briefing, reconciliationPayload) : advisorReconciliationBlockers(reconciliationPayload);
  if (!blockers.length) {
    card.hidden = true;
    return;
  }
  card.hidden = false;
  const resolvable = blockers.filter((row) => row.reconciliation_proposal_id != null).length;
  metaEl.textContent = blockers.length === 1 ? "1 validacion cambia la decision de hoy." : `${blockers.length} validaciones cambian la decision de hoy.`;
  if (hintEl) {
    if (resolvable > 0) {
      hintEl.hidden = false;
      hintEl.textContent = resolvable === 1 ? "Puedes resolver 1 bloqueo desde aqui." : `Puedes resolver ${resolvable} bloqueos desde aqui.`;
    } else {
      hintEl.hidden = true;
      hintEl.textContent = "";
    }
  }
  root.innerHTML = blockers.map((row) => advisorItemCard(row)).join("");
}

function renderAdvisorActions(briefing, reconciliationPayload) {
  const metaEl = el("advisorActionsMeta");
  const root = el("advisorRecommendations");
  if (!metaEl || !root) return;
  if (!briefing) {
    metaEl.textContent = "Sin briefing disponible.";
    root.innerHTML = `<div class="hint">No hay recomendaciones para mostrar.</div>`;
    return;
  }
  const actions = advisorVisibleActions(briefing);
  const blockers = advisorBlockingItems(briefing, reconciliationPayload);
  if (!actions.length) {
    metaEl.textContent = blockers.length
      ? "Primero hay que resolver bloqueos antes de tomar una accion."
      : "No hay acciones listas en esta vista.";
    root.innerHTML = `<div class="hint">${blockers.length ? "Resuelve primero el bloque de 'Antes de operar'." : "No hay compras o ventas listas para priorizar."}</div>`;
    return;
  }
  metaEl.textContent = actions.length === 1 ? "1 accion para priorizar." : `${actions.length} acciones para priorizar.`;
  root.innerHTML = actions.map((row) => advisorItemCard(row)).join("");
}

function renderAdvisorWatchlist(briefing) {
  const metaEl = el("advisorWatchlistMeta");
  const root = el("advisorWatchlist");
  if (!metaEl || !root) return;
  const rows = briefing ? advisorWatchlistItems(briefing) : [];
  metaEl.textContent = rows.length ? `${rows.length} en seguimiento` : "Sin pendientes";
  if (!rows.length) {
    root.innerHTML = `<div class="hint">No hay watchlist pendiente.</div>`;
    return;
  }
  const preview = rows.slice(0, 3);
  const extra = rows.length > preview.length ? `<div class="hint">Hay ${rows.length - preview.length} ideas mas en seguimiento.</div>` : "";
  root.innerHTML = preview.map((row) => advisorItemCard(row)).join("") + extra;
}

function renderAdvisorQuality(briefing) {
  const metaEl = el("advisorQualityMeta");
  const root = el("advisorQuality");
  if (!metaEl || !root) return;
  const quality = (briefing && briefing.quality) ? briefing.quality : {};
  const rows = quality.rows || [];
  const warningRows = rows.filter((row) => ["warn", "blocked", "error"].includes(String((row && row.kind) || "").toLowerCase()));
  metaEl.textContent = warningRows.length ? `${warningRows.length} alertas` : "Sin alertas criticas";
  if (!rows.length) {
    root.innerHTML = `<div class="hint">Sin senales de calidad disponibles.</div>`;
    return;
  }
  root.innerHTML = rows.map((row) => {
    const info = advisorStatusInfo(row.kind);
    const sources = (row.sources || []).map((src) => `<span class="quality-code">${escHtml(src)}</span>`).join("");
    return `
      <div class="advisor-quality-row">
        <div>
          <div class="advisor-item-title">${escHtml(row.label || "-")}</div>
          <div class="muted">${escHtml(row.detail || "-")}</div>
          ${sources ? `<div class="quality-code-row">${sources}</div>` : ""}
        </div>
        <span class="${info.className}">${escHtml(row.value || info.label)}</span>
      </div>
    `;
  }).join("");
}

function renderAdvisorLinks(briefing, latestRun) {
  const root = el("advisorLinks");
  if (!root) return;
  if (!briefing) {
    root.innerHTML = "";
    return;
  }
  const links = ((briefing.links || {}).latest_reports) || {};
  const items = [];
  if (links.analysis) items.push(`<span class="quality-code">Analisis: ${escHtml(links.analysis)}</span>`);
  if (links.macro) items.push(`<span class="quality-code">Macro: ${escHtml(links.macro)}</span>`);
  if (links.opportunities) items.push(`<span class="quality-code">Oportunidades: ${escHtml(links.opportunities)}</span>`);
  if (links.followup) items.push(`<span class="quality-code">Seguimiento: ${escHtml(links.followup)}</span>`);
  if (latestRun && latestRun.id != null) items.push(`<span class="quality-code">Run ${escHtml(latestRun.id)}</span>`);
  root.innerHTML = items.length ? `<div class="advisor-pill-row">${items.join("")}</div>` : "";
}

function renderAdvisorHistory(rows) {
  const metaEl = el("advisorHistoryMeta");
  const root = el("advisorHistory");
  if (!metaEl || !root) return;
  const list = rows || [];
  metaEl.textContent = list.length ? `${list.length} briefings guardados` : "Sin historial";
  if (!list.length) {
    root.innerHTML = `<div class="hint">Sin historial de briefings.</div>`;
    return;
  }
  const head = `
    <th>Fecha</th>
    <th>Cadencia</th>
    <th>Estado</th>
    <th>Run</th>
    <th>Log</th>
  `;
  const body = list.map((row) => {
    const status = advisorStatusInfo(row.status);
    return `
      <tr>
        <td>${escHtml(fmtDateShort(row.as_of || "-"))}</td>
        <td>${escHtml(humanizeEnum(row.cadence || "-"))}</td>
        <td><span class="${status.className}">${escHtml(status.label)}</span></td>
        <td class="num">${row.opportunity_run_id == null ? "-" : escHtml(row.opportunity_run_id)}</td>
        <td class="num">${row.advisor_log_id == null ? "-" : escHtml(row.advisor_log_id)}</td>
      </tr>
    `;
  }).join("");
  root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

let advisorPageState = null;

function applyAdvisorViewSelection() {
  if (!advisorPageState) return;
  const daily = advisorPageState.daily;
  const weekly = advisorPageState.weekly;
  let selected = advisorPageState.selected;
  if (selected === "weekly" && !weekly) selected = daily ? "daily" : "weekly";
  if (selected === "daily" && !daily) selected = weekly ? "weekly" : "daily";
  advisorPageState.selected = selected;

  const todayBtn = el("advisorViewToday");
  const weeklyBtn = el("advisorViewWeekly");
  if (todayBtn) {
    const active = selected === "daily";
    todayBtn.classList.toggle("is-active", active);
    todayBtn.setAttribute("aria-selected", active ? "true" : "false");
    todayBtn.disabled = !daily;
  }
  if (weeklyBtn) {
    const active = selected === "weekly";
    weeklyBtn.classList.toggle("is-active", active);
    weeklyBtn.setAttribute("aria-selected", active ? "true" : "false");
    weeklyBtn.disabled = !weekly;
  }

  const briefing = selected === "weekly" ? weekly : daily;
  renderAdvisorDecisionHero(briefing, advisorPageState.latestRun, selected, advisorPageState.reconciliation);
  renderAdvisorBlockers(briefing, advisorPageState.reconciliation);
  renderAdvisorActions(briefing, advisorPageState.reconciliation);
  renderAdvisorWatchlist(briefing);
  renderAdvisorQuality(briefing);
  renderAdvisorLinks(briefing, advisorPageState.latestRun);
  renderAdvisorHistory(advisorPageState.history || []);
}

export async function loadAdvisorPage() {
  const [dailyOut, weeklyOut, historyOut, runOut, reconciliationOut] = await Promise.all([
    fetchJSON("/api/advisor/latest?cadence=daily").catch(() => ({ briefing: null })),
    fetchJSON("/api/advisor/latest?cadence=weekly").catch(() => ({ briefing: null })),
    fetchJSON("/api/advisor/history?limit=30").catch(() => ({ rows: [] })),
    fetchJSON("/api/advisor/opportunities/latest").catch(() => ({ run: null })),
    fetchJSON("/api/reconciliation/open").catch(() => ({ rows: [], run: null })),
  ]);
  advisorPageState = {
    daily: dailyOut && dailyOut.briefing,
    weekly: weeklyOut && weeklyOut.briefing,
    history: (historyOut && historyOut.rows) || [],
    latestRun: runOut && runOut.run,
    reconciliation: reconciliationOut || { rows: [], run: null },
    selected: (dailyOut && dailyOut.briefing) ? "daily" : "weekly",
  };

  const todayBtn = el("advisorViewToday");
  const weeklyBtn = el("advisorViewWeekly");
  if (todayBtn && !todayBtn.dataset.bound) {
    todayBtn.dataset.bound = "1";
    todayBtn.addEventListener("click", () => {
      if (!advisorPageState || !advisorPageState.daily) return;
      advisorPageState.selected = "daily";
      applyAdvisorViewSelection();
    });
  }
  if (weeklyBtn && !weeklyBtn.dataset.bound) {
    weeklyBtn.dataset.bound = "1";
    weeklyBtn.addEventListener("click", () => {
      if (!advisorPageState || !advisorPageState.weekly) return;
      advisorPageState.selected = "weekly";
      applyAdvisorViewSelection();
    });
  }
  const blockersRoot = el("advisorBlockers");
  if (blockersRoot && !blockersRoot.dataset.bound) {
    blockersRoot.dataset.bound = "1";
    blockersRoot.addEventListener("click", async (ev) => {
      const applyBtn = ev.target && ev.target.closest("[data-advisor-recon-apply]");
      if (!applyBtn) return;
      const proposalId = Number(applyBtn.getAttribute("data-advisor-recon-apply") || 0);
      if (!proposalId) return;
      const hintEl = el("advisorBlockersHint");
      try {
        await fetchJSON("/api/reconciliation/apply", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ proposal_id: proposalId }),
        });
        advisorPageState.reconciliation = await fetchJSON("/api/reconciliation/open").catch(() => ({ rows: [], run: null }));
        if (hintEl) {
          hintEl.hidden = false;
          hintEl.textContent = "Bloqueo actualizado. Si el briefing sigue mostrando un warning general, se levantara en la proxima corrida.";
        }
        applyAdvisorViewSelection();
      } catch (e) {
        if (hintEl) {
          hintEl.hidden = false;
          hintEl.textContent = `No se pudo actualizar el bloqueo: ${e && e.message ? e.message : "error"}`;
        }
      }
    });
  }
  applyAdvisorViewSelection();
}
