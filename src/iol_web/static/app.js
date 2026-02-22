(function () {
  const fmtARS = new Intl.NumberFormat("es-AR", {
    style: "currency",
    currency: "ARS",
    maximumFractionDigits: 0,
  });
  const fmtUSD = new Intl.NumberFormat("es-AR", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  });
  const fmtNum = new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 });

  function el(id) { return document.getElementById(id); }

  function signClass(v) {
    if (v == null) return "";
    if (v > 0) return "pos";
    if (v < 0) return "neg";
    return "";
  }

  function fmtPct(v) {
    if (v == null) return "-";
    return (v >= 0 ? "+" : "") + fmtNum.format(v) + "%";
  }

  function fmtDeltaARS(v) {
    if (v == null) return "-";
    return (v >= 0 ? "+" : "-") + fmtARS.format(Math.abs(v));
  }

  function fmtDelta(v, fmtCur) {
    if (v == null) return "-";
    const f = fmtCur || fmtARS;
    return (v >= 0 ? "+" : "-") + f.format(Math.abs(v));
  }

  /* ── Counter animation ── */
  function animateValue(element, endVal, formatter, duration) {
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
  function relativeTime(isoStr) {
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

  async function fetchJSON(url, options) {
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

  function formatByCurrency(currencyCode, value) {
    if (currencyCode === "dolar_Estadounidense" || currencyCode === "USD") {
      return fmtUSD.format(value || 0);
    }
    return fmtARS.format(value || 0);
  }

  function labelCurrency(currencyCode) {
    if (currencyCode === "dolar_Estadounidense" || currencyCode === "USD") return "USD";
    if (currencyCode === "peso_Argentino" || currencyCode === "ARS") return "ARS";
    return "unknown";
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

  function fmtMonthLabel(yyyyMM) {
    const s = String(yyyyMM || "");
    if (s.length !== 7) return s || "-";
    const y = s.slice(0, 4);
    const m = parseInt(s.slice(5, 7), 10);
    const names = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];
    const name = (m >= 1 && m <= 12) ? names[m - 1] : s.slice(5, 7);
    return `${name} ${y}`;
  }

  function renderInflationCompare(targetId, rows, stale, availableTo, projectionUsed, projectionSourceMonth) {
    const root = el(targetId);
    if (!root) return;

    const head = `
      <th>Mes</th>
      <th>Snapshots</th>
      <th class="num">Retorno</th>
      <th class="num">IPC</th>
      <th class="num">Real</th>
    `;

    const body = (rows || []).map((r, idx) => {
      const month = fmtMonthLabel(r.month);
      const snaps = (r.from && r.to) ? `${r.from} \u2192 ${r.to}` : "-";
      const p = (r.portfolio_pct == null) ? "-" : fmtPct(r.portfolio_pct);
      const i = (r.inflation_pct == null) ? "-" : (fmtPct(r.inflation_pct) + (r.inflation_projected ? " (est.)" : ""));
      const real = (r.real_pct == null) ? "-" : fmtPct(r.real_pct);
      const cls = signClass(r.real_pct);
      return `
        <tr style="animation: fade-in 0.3s ease ${idx * 30}ms both;">
          <td>${month}</td>
          <td class="muted">${snaps}</td>
          <td class="num">${p}</td>
          <td class="num">${i}</td>
          <td class="num ${cls}">${real}</td>
        </tr>
      `;
    }).join("");

    root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;

    const hint = el("inflationHint");
    if (hint) {
      const bits = [];
      if (availableTo) bits.push(`IPC disponible hasta ${fmtMonthLabel(availableTo)}`);
      if (projectionUsed && projectionSourceMonth) bits.push(`mes actual estimado con ${fmtMonthLabel(projectionSourceMonth)}`);
      if (stale) bits.push("usando cach\u00e9 local (sin red)");
      if (bits.length) {
        hint.style.display = "block";
        hint.textContent = "Inflaci\u00f3n: " + bits.join(" | ") + ".";
      } else hint.style.display = "none";
    }
  }

  let chartTotal = null;
  let chartAlloc = null;
  let chartHistory = null;
  let chartInflationSeries = null;
  let chartInflationAnnual = null;
  let assetCtx = { latestYear: null, latestMonth: null, years: [] };

  function buildLineChart(canvas, labels, values) {
    const ctx = canvas.getContext("2d");
    // Multi-stop gradient fill
    const gradient = ctx.createLinearGradient(0, 0, 0, canvas.parentElement.clientHeight || 260);
    gradient.addColorStop(0, "rgba(103,232,249,0.25)");
    gradient.addColorStop(0.5, "rgba(103,232,249,0.08)");
    gradient.addColorStop(1, "rgba(103,232,249,0.0)");

    return new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: "Total (ARS)",
          data: values,
          borderColor: "rgba(103,232,249,0.92)",
          backgroundColor: gradient,
          tension: 0.3,
          fill: true,
          pointRadius: 0,
          pointHoverRadius: 5,
          pointHoverBackgroundColor: "rgba(103,232,249,1)",
          pointHoverBorderColor: "#fff",
          pointHoverBorderWidth: 2,
          borderWidth: 2.5,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { intersect: false, mode: "index" },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "rgba(10,16,32,0.90)",
            borderColor: "rgba(103,232,249,0.30)",
            borderWidth: 1,
            titleFont: { family: "Inter", weight: "600" },
            bodyFont: { family: "Inter" },
            padding: 12,
            cornerRadius: 12,
            callbacks: {
              label: (ctx) => fmtARS.format(ctx.parsed.y || 0),
            },
          },
        },
        scales: {
          x: { ticks: { color: "rgba(255,255,255,0.55)", font: { size: 11 } }, grid: { color: "rgba(255,255,255,0.05)" } },
          y: { ticks: { color: "rgba(255,255,255,0.55)", font: { size: 11 }, callback: (v) => fmtARS.format(v) }, grid: { color: "rgba(255,255,255,0.05)" } },
        },
      },
    });
  }

  function buildMultiLineChart(canvas, labels, datasets, yTickFormatter) {
    const ctx = canvas.getContext("2d");
    return new Chart(ctx, {
      type: "line",
      data: { labels, datasets: datasets || [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { intersect: false, mode: "index" },
        plugins: {
          legend: { display: true, labels: { color: "rgba(255,255,255,0.72)", font: { family: "Inter", size: 12 } } },
          tooltip: {
            backgroundColor: "rgba(10,16,32,0.90)",
            borderColor: "rgba(103,232,249,0.25)",
            borderWidth: 1,
            titleFont: { family: "Inter", weight: "600" },
            bodyFont: { family: "Inter" },
            padding: 12,
            cornerRadius: 12,
            callbacks: {
              label: (c) => {
                const v = c.parsed.y;
                const fmt = yTickFormatter || ((x) => fmtNum.format(x));
                return `${c.dataset.label}: ${v == null ? "-" : fmt(v)}`;
              },
            },
          },
        },
        scales: {
          x: { ticks: { color: "rgba(255,255,255,0.55)", font: { size: 11 } }, grid: { color: "rgba(255,255,255,0.05)" } },
          y: {
            ticks: {
              color: "rgba(255,255,255,0.55)",
              font: { size: 11 },
              callback: (v) => (yTickFormatter ? yTickFormatter(v) : fmtNum.format(v)),
            },
            grid: { color: "rgba(255,255,255,0.05)" },
          },
        },
      },
    });
  }

  function buildBarChart(canvas, labels, datasets, yTickFormatter, tooltipAfterBody) {
    const ctx = canvas.getContext("2d");
    return new Chart(ctx, {
      type: "bar",
      data: { labels, datasets: datasets || [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: true, labels: { color: "rgba(255,255,255,0.72)", font: { family: "Inter", size: 12 } } },
          tooltip: {
            backgroundColor: "rgba(10,16,32,0.90)",
            borderColor: "rgba(103,232,249,0.25)",
            borderWidth: 1,
            titleFont: { family: "Inter", weight: "600" },
            bodyFont: { family: "Inter" },
            padding: 12,
            cornerRadius: 12,
            callbacks: {
              label: (c) => {
                const v = c.parsed.y;
                const fmt = yTickFormatter || ((x) => fmtPct(x));
                return `${c.dataset.label}: ${v == null ? "-" : fmt(v)}`;
              },
              afterBody: tooltipAfterBody || undefined,
            },
          },
        },
        scales: {
          x: { ticks: { color: "rgba(255,255,255,0.55)", font: { size: 11 } }, grid: { color: "rgba(255,255,255,0.05)" } },
          y: {
            ticks: {
              color: "rgba(255,255,255,0.55)",
              font: { size: 11 },
              callback: (v) => (yTickFormatter ? yTickFormatter(v) : fmtNum.format(v) + "%"),
            },
            grid: { color: "rgba(255,255,255,0.05)" },
          },
        },
      },
    });
  }

  function buildPieChart(canvas, labels, values) {
    const ctx = canvas.getContext("2d");
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
    const colors = labels.map((_, i) => palette[i % palette.length]);
    return new Chart(ctx, {
      type: "doughnut",
      data: {
        labels,
        datasets: [{
          data: values,
          backgroundColor: colors,
          borderColor: "rgba(10,16,32,0.8)",
          borderWidth: 2,
          hoverOffset: 8,
        }],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "rgba(10,16,32,0.90)",
            borderColor: "rgba(103,232,249,0.25)",
            borderWidth: 1,
            titleFont: { family: "Inter", weight: "600" },
            bodyFont: { family: "Inter" },
            padding: 12,
            cornerRadius: 12,
            callbacks: {
              label: (ctx) => `${ctx.label}: ${fmtARS.format(ctx.parsed || 0)}`,
            },
          },
        },
        cutout: "65%",
      },
    });
  }

  function renderAllocLegend(containerId, labels, values, colors) {
    const root = el(containerId);
    if (!root) return;
    const total = (values || []).reduce((a, b) => a + (Number(b) || 0), 0);
    const rows = labels.map((name, i) => {
      const v = Number(values[i] || 0);
      const pct = total > 0 ? (v / total * 100.0) : 0;
      return `
        <div class="row">
          <div class="dot" style="background:${colors[i]};"></div>
          <div class="name">${name}</div>
          <div class="meta">${fmtARS.format(v)}<br/>${fmtNum.format(pct)}%</div>
        </div>
      `;
    }).join("");
    root.innerHTML = rows;
  }

  function putReturnCard(mainId, subId, block) {
    const n = el(mainId);
    if (n) {
      const mainPct = (block && block.real_pct != null) ? block.real_pct : null;
      n.textContent = mainPct == null ? "-" : fmtPct(mainPct);
      n.className = "v " + signClass(mainPct);
    }
    const s = el(subId);
    if (s) {
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

  function renderMonthVsInflationKpi(kpi) {
    const v = el("kpiMonthVsInflValue");
    const s = el("kpiMonthVsInflSub");
    const b = el("kpiMonthVsInflBadge");
    if (!v || !s || !b) return;

    const status = (kpi && kpi.status) ? String(kpi.status) : "";
    v.className = "v";
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
      v.className = "v " + signClass(kpi.net_pct);
      s.textContent = `Neto ${net} | IPC no disponible`;
      return;
    }

    const real = (kpi.real_vs_inflation_pct != null) ? Number(kpi.real_vs_inflation_pct) : null;
    const net = (kpi.net_pct != null) ? fmtPct(kpi.net_pct) : "-";
    const ipc = (kpi.inflation_pct != null)
      ? (fmtPct(kpi.inflation_pct) + (kpi.inflation_projected ? " (est.)" : ""))
      : "-";

    v.textContent = real == null ? "-" : fmtPct(real);
    v.className = "kpi-value " + signClass(real);
    s.textContent = `Neto ${net} | IPC ${ipc}`;

    if (kpi.beats_inflation == null || real == null) {
      b.style.display = "inline-flex";
      b.className = "kpi-status-badge kpi-status-neutral";
      b.textContent = "Sin senal";
      return;
    }

    b.style.display = "inline-flex";
    if (kpi.beats_inflation) {
      b.className = "kpi-status-badge kpi-status-up";
      b.textContent = "▲ Le ganas al IPC";
    } else {
      b.className = "kpi-status-badge kpi-status-down";
      b.textContent = "▼ Debajo del IPC";
    }
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
          label: "Costo operativo probable",
          className: "row-badge row-badge-operational",
          detail: "Residual chico vs volumen operado (\u22643%).",
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
        correction: "row-badge row-badge-correction",
        operational_cost_probable: "row-badge row-badge-operational",
        rotation_probable: "row-badge row-badge-rotation",
        external_flow_probable: "row-badge row-badge-external",
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
      const warnBadge = hasWarn
        ? `<span class="row-badge row-badge-correction">${warnTxt}</span>`
        : `<span class="muted">-</span>`;
      const kindCell = kind.className
        ? `<span class="${kind.className}">${kind.label}</span>`
        : (kind.label || "-");
      const kindDetail = kind.detail ? `<div class="row-subhint">${kind.detail}</div>` : "";
      return `
        <tr>
          <td>${r.flow_date || "-"}</td>
          <td>${kindCell}${kindDetail}</td>
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

  async function refreshManualCashflows() {
    if (!el("tblCashflowsManual")) return;
    try {
      const rows = await fetchJSON("/api/cashflows/manual");
      renderManualCashflows(rows);
    } catch (e) {
      const root = el("tblCashflowsManual");
      if (root) root.innerHTML = `<div class="hint">No se pudieron cargar los ajustes manuales.</div>`;
    }
  }

  async function refreshAutoCashflows(days) {
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

  async function loadDashboard(rangeDays) {
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

    // Animate total value
    const kpiTotalEl = el("kpiTotal");
    if (kpiTotalEl) {
      animateValue(kpiTotalEl, snap.total_value || 0, (v) => fmtARS.format(v), 900);
    }

    if (el("kpiDate")) {
      let meta = `Snapshot: ${snap.snapshot_date}`;
      if (snap.retrieved_at) {
        const d = new Date(snap.retrieved_at);
        if (!isNaN(d.getTime())) meta += ` - ${d.toLocaleString("es-AR")}`;
      }
      el("kpiDate").textContent = meta;
    }

    // Updated badge with relative time
    const updBadge = el("updatedBadge");
    const updText = el("updatedText");
    if (updBadge && updText && snap.retrieved_at) {
      const rel = relativeTime(snap.retrieved_at);
      if (rel) {
        updText.textContent = `Actualizado ${rel}`;
        updBadge.style.display = "inline-flex";
      }
    }

    // Cash KPIs
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
    }

    putReturnCard("retWeekly", "retWeeklySub", ret.weekly);
    putReturnCard("retMonthly", "retMonthlySub", ret.monthly);
    putReturnCard("retYtd", "retYtdSub", ret.ytd);
    putReturnCard("retYearly", "retYearlySub", ret.yearly);
    renderMonthVsInflationKpi(monthlyKpi);
    renderHeroMonthInsights(monthlyKpi);

    // Keep compact inflation card aligned with the same monthly KPI used in the hero cards.
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
      // Reuse palette order from buildPieChart by computing it here too.
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
    ]);
  }

  async function loadAssetsPage() {
    const latest = await fetchJSON("/api/latest");
    if (!latest || !latest.snapshot) return;
    const snap = latest.snapshot;
    if (el("assetsSnap")) el("assetsSnap").textContent = snap.snapshot_date;

    let assets = latest.assets || [];
    const filterInput = el("assetsFilter");
    const sortSel = el("assetsSort");

    function sortAssets(kind) {
      const k = kind || "total_value_desc";
      const parts = k.split("_");
      const field = parts.slice(0, -1).join("_");
      const dir = parts[parts.length - 1];
      const m = (a) => (a[field] == null ? 0 : Number(a[field]));
      assets.sort((a, b) => dir === "asc" ? (m(a) - m(b)) : (m(b) - m(a)));
    }

    function render(list) {
      const root = el("assetsTable");
      if (!root) return;
      const head = `
        <th>S\u00edmbolo</th>
        <th>Desc</th>
        <th>Tipo</th>
        <th>Mercado</th>
        <th class="num">Valor</th>
        <th class="num">D\u00eda</th>
        <th class="num">PnL</th>
      `;
      const body = (list || []).map((r, idx) => {
        const dv = r.daily_var_points;
        const ga = r.gain_amount;
        return `
          <tr style="animation: fade-in 0.3s ease ${idx * 25}ms both;">
            <td>${r.symbol || "-"}</td>
            <td class="muted">${(r.description || "").slice(0, 48)}</td>
            <td class="muted">${r.type || "-"}</td>
            <td class="muted">${r.market || "-"}</td>
            <td class="num">${fmtARS.format(r.total_value || 0)}</td>
            <td class="num ${signClass(dv)}">${fmtDeltaARS(dv)}</td>
            <td class="num ${signClass(ga)}">${fmtDeltaARS(ga)}</td>
          </tr>
        `;
      }).join("");
      root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
    }

    function apply() {
      const q = (filterInput && filterInput.value || "").trim().toLowerCase();
      const kind = sortSel ? sortSel.value : "total_value_desc";
      sortAssets(kind);
      let list = assets;
      if (q) {
        list = assets.filter(a => {
          const s = ((a.symbol || "") + " " + (a.description || "") + " " + (a.type || "")).toLowerCase();
          return s.includes(q);
        });
      }
      render(list);
    }

    if (filterInput) filterInput.addEventListener("input", apply);
    if (sortSel) sortSel.addEventListener("change", apply);
    apply();
  }

  async function loadHistoryPage() {
    const series = await fetchJSON("/api/snapshots");
    const labels = series.map(x => x.date);
    const values = series.map(x => x.total_value);
    const canvas = el("chartHistory");
    if (!canvas || !labels.length) return;
    if (chartHistory) chartHistory.destroy();
    chartHistory = buildLineChart(canvas, labels, values);

    // Inflation comparison series (index base 100)
    const c2 = el("chartInflationSeries");
    if (c2) {
      try {
        const cmp = await fetchJSON("/api/compare/inflation/series");
        const labels2 = cmp.labels || [];
        const pIdx = cmp.portfolio_index || [];
        const iIdx = cmp.inflation_index || [];
        if (chartInflationSeries) chartInflationSeries.destroy();
        if (labels2.length) {
          const ds = [
            {
              label: "Portfolio (base 100)",
              data: pIdx,
              borderColor: "rgba(103,232,249,0.92)",
              backgroundColor: "rgba(103,232,249,0.10)",
              tension: 0.22,
              fill: false,
              pointRadius: 0,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(103,232,249,1)",
              borderWidth: 2.5,
            },
            {
              label: "Inflaci\u00f3n (base 100)",
              data: iIdx,
              borderColor: "rgba(252,211,77,0.90)",
              backgroundColor: "rgba(252,211,77,0.10)",
              tension: 0.0,
              fill: false,
              pointRadius: 0,
              pointHoverRadius: 5,
              pointHoverBackgroundColor: "rgba(252,211,77,1)",
              borderWidth: 2.5,
            },
          ];
          chartInflationSeries = buildMultiLineChart(c2, labels2, ds, (v) => fmtNum.format(v));
        }

        const hint = el("inflationSeriesHint");
        if (hint) {
          const bits = [];
          if (cmp && cmp.inflation_available_to) bits.push(`IPC disponible hasta ${fmtMonthLabel(cmp.inflation_available_to)}`);
          if (cmp && cmp.projection_used) bits.push("incluye estimaci\u00f3n (est.) para el mes actual");
          hint.style.display = bits.length ? "block" : "none";
          hint.textContent = bits.length ? bits.join(" | ") : "";
        }
      } catch (_) {
        const hint = el("inflationSeriesHint");
        if (hint) {
          hint.style.display = "block";
          hint.textContent = "No se pudo cargar la comparativa con inflaci\u00f3n.";
        }
      }
    }

    // Annual bars
    const c3 = el("chartInflationAnnual");
    if (c3) {
      try {
        const ann = await fetchJSON("/api/compare/inflation/annual?years=10");
        const rows = ann.rows || [];
        const labels3 = rows.map(r => r.label);
        const p = rows.map(r => r.portfolio_pct);
        const inf = rows.map(r => r.inflation_pct);
        if (chartInflationAnnual) chartInflationAnnual.destroy();
        if (labels3.length) {
          const ds = [
            { label: "Portfolio %", data: p, backgroundColor: "rgba(103,232,249,0.55)", borderColor: "rgba(103,232,249,0.9)", borderWidth: 1, borderRadius: 4 },
            { label: "IPC %", data: inf, backgroundColor: "rgba(252,211,77,0.45)", borderColor: "rgba(252,211,77,0.85)", borderWidth: 1, borderRadius: 4 },
          ];
          chartInflationAnnual = buildBarChart(
            c3,
            labels3,
            ds,
            (v) => fmtNum.format(v) + "%",
            (ctx) => {
              if (!ctx || !ctx.length) return [];
              const idx = ctx[0].dataIndex;
              const r = rows[idx] || {};
              const out = [];
              if (r.real_pct != null) out.push(`Real: ${fmtPct(r.real_pct)}`);
              if (r.from && r.to) out.push(`Rango: ${r.from} \u2192 ${r.to}`);
              if (r.partial) out.push("Nota: a\u00f1o parcial (seg\u00fan snapshots)");
              if (r.inflation_projected) out.push("IPC: incluye estimaci\u00f3n (est.)");
              return out;
            }
          );
        }

        const hint = el("inflationAnnualHint");
        if (hint) {
          const bits = [];
          if (ann && ann.inflation_available_to) bits.push(`IPC disponible hasta ${fmtMonthLabel(ann.inflation_available_to)}`);
          if (ann && ann.projection_used) bits.push("incluye estimaci\u00f3n (est.) para el mes actual");
          hint.style.display = bits.length ? "block" : "none";
          hint.textContent = bits.length ? bits.join(" | ") : "";
        }
      } catch (_) {
        const hint = el("inflationAnnualHint");
        if (hint) {
          hint.style.display = "block";
          hint.textContent = "No se pudo cargar la comparativa anual con inflaci\u00f3n.";
        }
      }
    }
  }

  function initRangeButtons() {
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

  function initAllocControls() {
    const groupSel = el("allocGroupBy");
    const cashChk = el("allocCash");
    if (groupSel) groupSel.addEventListener("change", () => loadDashboard(getActiveRange()));
    if (cashChk) cashChk.addEventListener("change", () => loadDashboard(getActiveRange()));
  }

  function initCashflowControls() {
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

  function initAssetPerformanceControls() {
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

  function getAssetPeriod() {
    try {
      const v1 = (localStorage.getItem("assetPeriod") || "").trim().toLowerCase();
      if (["daily", "weekly", "monthly", "yearly", "accumulated"].includes(v1)) return v1;
      const legacy = (localStorage.getItem("moversPeriod") || "").trim().toLowerCase();
      if (legacy) {
        const mapped = (legacy === "ytd") ? "accumulated" : legacy;
        if (["daily", "weekly", "monthly", "yearly", "accumulated"].includes(mapped)) {
          localStorage.setItem("assetPeriod", mapped);
          localStorage.removeItem("moversPeriod");
          return mapped;
        }
      }
    } catch (_) { }
    return "daily";
  }

  function setAssetPeriod(v) {
    try { localStorage.setItem("assetPeriod", String(v || "daily")); } catch (_) { }
  }

  function initAssetPickerControls() {
    const prev = el("assetPickPrev");
    const next = el("assetPickNext");
    if (prev) prev.addEventListener("click", () => moveAssetPicker(-1));
    if (next) next.addEventListener("click", () => moveAssetPicker(+1));
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

  function getAssetMonth(defaultMonth) {
    try {
      const current = parseInt(localStorage.getItem("assetMonth") || "", 10);
      if (current >= 1 && current <= 12) return current;
      const legacy = parseInt(localStorage.getItem("moversMonth") || "", 10);
      if (legacy >= 1 && legacy <= 12) {
        localStorage.setItem("assetMonth", String(legacy));
        localStorage.removeItem("moversMonth");
        return legacy;
      }
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
      else {
        const legacy = parseInt(localStorage.getItem("moversYear") || "", 10);
        if (!isNaN(legacy)) {
          y = legacy;
          localStorage.setItem("assetYear", String(legacy));
          localStorage.removeItem("moversYear");
        }
      }
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

  function getActiveRange() {
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

  function initEvolutionModeControls() {
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

  document.addEventListener("DOMContentLoaded", () => {
    initRangeButtons();
    initEvolutionModeControls();
    initAllocControls();
    initCashflowControls();
    initAssetPerformanceControls();
    initAssetPickerControls();
    if (el("chartTotal")) loadDashboard(getActiveRange());
    if (el("assetsTable")) loadAssetsPage();
    if (el("chartHistory")) loadHistoryPage();
  });
})();
