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

  async function fetchJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    return await r.json();
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

    const body = (rows || []).map((r) => {
      const metric = r[metricKey];
      const pct = (metricKey === "delta_value") ? r["delta_pct"] : null;
      const metricText = fmtDelta(metric, fmtCur);
      const pctText = (pct == null) ? "" : ` <span class="muted">(${fmtPct(pct)})</span>`;
      return `
        <tr>
          <td>${r.symbol || "-"}</td>
          <td class="muted">${(r.description || "").slice(0, 42)}</td>
          <td class="num">${fmtCur.format(r.total_value || 0)}</td>
          <td class="num ${signClass(metric)}">${metricText}${pctText}</td>
        </tr>
      `;
    }).join("");

    root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
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

    const body = (rows || []).map((r) => {
      const month = fmtMonthLabel(r.month);
      const snaps = (r.from && r.to) ? `${r.from} \u2192 ${r.to}` : "-";
      const p = (r.portfolio_pct == null) ? "-" : fmtPct(r.portfolio_pct);
      const i = (r.inflation_pct == null) ? "-" : (fmtPct(r.inflation_pct) + (r.inflation_projected ? " (est.)" : ""));
      const real = (r.real_pct == null) ? "-" : fmtPct(r.real_pct);
      const cls = signClass(r.real_pct);
      return `
        <tr>
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
  let moversCtx = { latestYear: null, latestMonth: null, years: [] };

  function buildLineChart(canvas, labels, values) {
    const ctx = canvas.getContext("2d");
    return new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: [{
          label: "Total (ARS)",
          data: values,
          borderColor: "rgba(103,232,249,0.9)",
          backgroundColor: "rgba(103,232,249,0.12)",
          tension: 0.22,
          fill: true,
          pointRadius: 0,
          borderWidth: 2,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (ctx) => fmtARS.format(ctx.parsed.y || 0),
            },
          },
        },
        scales: {
          x: { ticks: { color: "rgba(255,255,255,0.65)" }, grid: { color: "rgba(255,255,255,0.07)" } },
          y: { ticks: { color: "rgba(255,255,255,0.65)", callback: (v) => fmtARS.format(v) }, grid: { color: "rgba(255,255,255,0.07)" } },
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
        plugins: {
          legend: { display: true, labels: { color: "rgba(255,255,255,0.72)" } },
          tooltip: {
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
          x: { ticks: { color: "rgba(255,255,255,0.65)" }, grid: { color: "rgba(255,255,255,0.07)" } },
          y: {
            ticks: {
              color: "rgba(255,255,255,0.65)",
              callback: (v) => (yTickFormatter ? yTickFormatter(v) : fmtNum.format(v)),
            },
            grid: { color: "rgba(255,255,255,0.07)" },
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
          legend: { display: true, labels: { color: "rgba(255,255,255,0.72)" } },
          tooltip: {
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
          x: { ticks: { color: "rgba(255,255,255,0.65)" }, grid: { color: "rgba(255,255,255,0.07)" } },
          y: {
            ticks: {
              color: "rgba(255,255,255,0.65)",
              callback: (v) => (yTickFormatter ? yTickFormatter(v) : fmtNum.format(v) + "%"),
            },
            grid: { color: "rgba(255,255,255,0.07)" },
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
        }],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (ctx) => `${ctx.label}: ${fmtARS.format(ctx.parsed || 0)}`,
            },
          },
        },
        cutout: "62%",
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

  async function loadDashboard(rangeDays) {
    const [latest, ret] = await Promise.all([
      fetchJSON("/api/latest"),
      fetchJSON("/api/returns"),
    ]);

    if (!latest || !latest.snapshot) {
      const hint = el("noDataHint");
      if (hint) hint.style.display = "block";
      return;
    }

    const snap = latest.snapshot;
    if (el("kpiTotal")) el("kpiTotal").textContent = fmtARS.format(snap.total_value || 0);
    if (el("kpiDate")) {
      let meta = `Snapshot: ${snap.snapshot_date}`;
      if (snap.retrieved_at) {
        const d = new Date(snap.retrieved_at);
        if (!isNaN(d.getTime())) meta += ` - ${d.toLocaleString("es-AR")}`;
      }
      el("kpiDate").textContent = meta;
    }

    const daily = ret.daily || {};
    if (el("kpiDailyDelta")) {
      el("kpiDailyDelta").textContent = fmtDeltaARS(daily.delta);
      el("kpiDailyDelta").className = "kpi-value " + signClass(daily.delta);
    }
    if (el("kpiDailyPct")) el("kpiDailyPct").textContent = daily.pct == null ? "-" : fmtPct(daily.pct);

    function putRet(id, block) {
      const n = el(id);
      if (!n) return;
      const pct = (block && block.pct != null) ? block.pct : null;
      n.textContent = pct == null ? "-" : fmtPct(pct);
      n.className = "v " + signClass(pct);
    }
    putRet("retWeekly", ret.weekly);
    putRet("retMonthly", ret.monthly);
    putRet("retYtd", ret.ytd);
    putRet("retYearly", ret.yearly);
    const rh = el("returnsHint");
    if (rh) {
      const missing = ["weekly", "monthly", "yearly"].some((k) => (ret[k] && ret[k].pct != null) ? false : true);
      if (missing) {
        rh.style.display = "block";
        rh.textContent = "Para ver retornos por per\u00edodo necesit\u00e1s m\u00e1s de 1 snapshot. Deja corriendo el scheduler (guarda al cierre) o ejecuta `iol snapshot run` en distintos d\u00edas.";
      } else {
        rh.style.display = "none";
      }
    }

    // Monthly inflation compare (calendar month)
    const cmpRoot = el("tblInflationCompare");
    if (cmpRoot) cmpRoot.innerHTML = `<div class="hint">Cargando inflaci\u00f3n...</div>`;
    try {
      const cmp = await fetchJSON("/api/compare/inflation?months=12");
      const rows = (cmp && cmp.rows) ? cmp.rows : [];
      renderInflationCompare(
        "tblInflationCompare",
        rows,
        !!(cmp && cmp.stale),
        (cmp && cmp.inflation_available_to) ? cmp.inflation_available_to : null,
        !!(cmp && cmp.projection_used),
        (cmp && cmp.projection_source_month) ? cmp.projection_source_month : null
      );

      // KPI: pick latest month where both are available
      let pick = null;
      for (let i = rows.length - 1; i >= 0; i--) {
        const r = rows[i];
        if (r && r.portfolio_pct != null && r.inflation_pct != null && r.real_pct != null) { pick = r; break; }
      }
      if (el("inflMonthly")) el("inflMonthly").textContent = pick ? (fmtPct(pick.inflation_pct) + (pick.inflation_projected ? " (est.)" : "")) : "-";
      if (el("realMonthly")) {
        el("realMonthly").textContent = pick ? fmtPct(pick.real_pct) : "-";
        el("realMonthly").className = "v " + signClass(pick ? pick.real_pct : null);
      }
    } catch (_) {
      // Keep dashboard functional even if inflation endpoint fails.
      if (el("inflMonthly")) el("inflMonthly").textContent = "-";
      if (el("realMonthly")) el("realMonthly").textContent = "-";
      if (cmpRoot) {
        cmpRoot.innerHTML = `<div class="hint">No se pudo cargar la inflaci\u00f3n. Prob\u00e1 abrir <code>/api/compare/inflation?months=12</code> y revis\u00e1 los logs del contenedor <code>web</code>.</div>`;
      }
    }

    const series = await fetchJSON("/api/snapshots");
    const labelsAll = series.map(x => x.date);
    const valuesAll = series.map(x => x.total_value);
    const years = Array.from(
      new Set(labelsAll.map(d => parseInt(String(d).slice(0, 4), 10)).filter(n => !isNaN(n)))
    ).sort((a, b) => a - b);

    const latestDate = String(snap.snapshot_date || "");
    const latestYear = parseInt(latestDate.slice(0, 4), 10);
    const latestMonth = parseInt(latestDate.slice(5, 7), 10);
    moversCtx = { latestYear: latestYear || null, latestMonth: latestMonth || null, years };

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

    const period = getMoversPeriod();
    function buildMoversUrl(curr) {
      const c = curr || "peso_Argentino";
      let url = `/api/movers?kind=period&period=${encodeURIComponent(period)}&limit=10&metric=pnl&currency=${encodeURIComponent(c)}`;
      if (period === "monthly") {
        const m = getMoversMonth(latestMonth || 1);
        url = `/api/movers?kind=period&period=monthly&month=${encodeURIComponent(m)}&year=${encodeURIComponent(latestYear)}&limit=10&metric=pnl&currency=${encodeURIComponent(c)}`;
      } else if (period === "yearly") {
        const y = getMoversYear(latestYear || new Date().getFullYear(), years);
        url = `/api/movers?kind=period&period=yearly&year=${encodeURIComponent(y)}&limit=10&metric=pnl&currency=${encodeURIComponent(c)}`;
      }
      return url;
    }

    const [moversARS, moversUSD] = await Promise.all([
      fetchJSON(buildMoversUrl("peso_Argentino")),
      fetchJSON(buildMoversUrl("dolar_Estadounidense")),
    ]);
    updateMoversPicker(period, latestYear, latestMonth, years);
    if (period !== "daily" && moversARS && moversARS.from == null) {
      const msg = (period === "monthly")
        ? "No hay snapshots para ese a\u00f1o."
        : (period === "yearly")
          ? "No hay snapshots para ese a\u00f1o."
          : "No hay snapshots suficientes para ese per\u00edodo. Deja corriendo el scheduler o ejecuta `iol snapshot run` en distintos d\u00edas.";
      const a = el("tblGainersDaily");
      const b = el("tblLosersDaily");
      if (a) a.innerHTML = `<div class="hint">${msg}</div>`;
      if (b) b.innerHTML = `<div class="hint">${msg}</div>`;
      const au = el("tblGainersDailyUSD");
      const bu = el("tblLosersDailyUSD");
      if (au) au.innerHTML = `<div class="hint">${msg}</div>`;
      if (bu) bu.innerHTML = `<div class="hint">${msg}</div>`;
      const usdGrid = el("moversUsdGrid");
      if (usdGrid) usdGrid.style.display = "none";
    } else {
      renderTable("tblGainersDaily", moversARS.gainers || [], "delta_value", "PnL", "ARS");
      renderTable("tblLosersDaily", moversARS.losers || [], "delta_value", "PnL", "ARS");

      const usdGrid = el("moversUsdGrid");
      const hasUsd = (moversUSD && ((moversUSD.gainers || []).length || (moversUSD.losers || []).length));
      if (usdGrid) usdGrid.style.display = hasUsd ? "" : "none";
      if (hasUsd) {
        renderTable("tblGainersDailyUSD", moversUSD.gainers || [], "delta_value", "PnL", "USD");
        renderTable("tblLosersDailyUSD", moversUSD.losers || [], "delta_value", "PnL", "USD");
      }
    }

    const mh = el("moversHint");
    if (mh) {
      const warns = (moversARS && moversARS.warnings) ? moversARS.warnings : [];
      if (warns && warns.length) {
        const from = moversARS.from;
        const to = moversARS.to;
        const cmd = (from && to) ? `iol snapshot backfill --from ${from} --to ${to}` : "iol snapshot backfill --from YYYY-MM-DD --to YYYY-MM-DD";
        const msg = warns.includes("ORDERS_NONE")
          ? `No hay operaciones terminadas en la DB para este per\u00edodo. Si vendiste activos, corr\u00e9 un backfill: <code>${cmd}</code> y luego ejecut\u00e1 <code>iol snapshot run</code> (o dej\u00e1 el scheduler).`
          : `Hay operaciones incompletas (faltan lado/monto/fecha). Corr\u00e9 un backfill: <code>${cmd}</code> y luego ejecut\u00e1 <code>iol snapshot run</code> (o dej\u00e1 el scheduler).`;
        mh.style.display = "block";
        mh.innerHTML = msg;
      } else {
        mh.style.display = "none";
      }
    }

    const map = { daily: "d\u00eda", weekly: "semana", monthly: "mes", yearly: "a\u00f1o", ytd: "ytd" };
    const t = map[period] || period;
    const badge = el("moversPeriodBadge");
    if (badge) badge.textContent = t;

    const moversTotal = await fetchJSON("/api/movers?kind=total&limit=10");
    renderTable("tblGainersTotal", moversTotal.gainers || [], "gain_amount", "PnL");
    renderTable("tblLosersTotal", moversTotal.losers || [], "gain_amount", "PnL");

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
      const body = (list || []).map((r) => {
        const dv = r.daily_var_points;
        const ga = r.gain_amount;
        return `
          <tr>
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
              borderWidth: 2,
            },
            {
              label: "Inflaci\u00f3n (base 100)",
              data: iIdx,
              borderColor: "rgba(252,211,77,0.90)",
              backgroundColor: "rgba(252,211,77,0.10)",
              tension: 0.0,
              fill: false,
              pointRadius: 0,
              borderWidth: 2,
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
            { label: "Portfolio %", data: p, backgroundColor: "rgba(103,232,249,0.55)", borderColor: "rgba(103,232,249,0.9)", borderWidth: 1 },
            { label: "IPC %", data: inf, backgroundColor: "rgba(252,211,77,0.45)", borderColor: "rgba(252,211,77,0.85)", borderWidth: 1 },
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

  function initMoversControls() {
    const wrap = el("moversPeriodButtons");
    if (!wrap) return;

    const active = getMoversPeriod();
    Array.from(wrap.querySelectorAll("button")).forEach((b) => {
      b.classList.toggle("on", b.getAttribute("data-period") === active);
    });

    wrap.addEventListener("click", (ev) => {
      const btn = ev.target && ev.target.closest("button");
      if (!btn) return;
      const p = (btn.getAttribute("data-period") || "daily").toLowerCase();
      setMoversPeriod(p);
      Array.from(wrap.querySelectorAll("button")).forEach((b) => b.classList.remove("on"));
      btn.classList.add("on");
      loadDashboard(getActiveRange());
    });
  }

  function getMoversPeriod() {
    try {
      const v = (localStorage.getItem("moversPeriod") || "daily").trim().toLowerCase();
      if (["daily", "weekly", "monthly", "yearly", "ytd"].includes(v)) return v;
    } catch (_) {}
    return "daily";
  }

  function setMoversPeriod(v) {
    try { localStorage.setItem("moversPeriod", String(v || "daily")); } catch (_) {}
  }

  function initMoversPickerControls() {
    const prev = el("moversPickPrev");
    const next = el("moversPickNext");
    if (prev) prev.addEventListener("click", () => movePicker(-1));
    if (next) next.addEventListener("click", () => movePicker(+1));
  }

  function movePicker(dir) {
    const period = getMoversPeriod();
    if (period === "monthly") {
      const baseMonth = moversCtx.latestMonth || 1;
      let m = getMoversMonth(baseMonth);
      m = m + dir;
      if (m < 1) m = 12;
      if (m > 12) m = 1;
      setMoversMonth(m);
      loadDashboard(getActiveRange());
      return;
    }
    if (period === "yearly") {
      const years = moversCtx.years || [];
      if (!years.length) return;
      let y = getMoversYear(moversCtx.latestYear || years[years.length - 1], years);
      const idx = years.indexOf(y);
      const ni = idx + dir;
      if (ni < 0 || ni >= years.length) return;
      setMoversYear(years[ni]);
      loadDashboard(getActiveRange());
    }
  }

  function updateMoversPicker(period, latestYear, latestMonth, years) {
    const picker = el("moversPicker");
    const label = el("moversPickLabel");
    const prev = el("moversPickPrev");
    const next = el("moversPickNext");
    if (!picker || !label || !prev || !next) return;

    if (period === "monthly") {
      picker.style.display = "inline-flex";
      const m = getMoversMonth(latestMonth || 1);
      setMoversMonth(m);
      const names = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];
      label.textContent = `${names[m - 1]} ${latestYear}`;
      prev.disabled = false;
      next.disabled = false;
      return;
    }

    if (period === "yearly") {
      picker.style.display = "inline-flex";
      const ys = Array.isArray(years) ? years : [];
      const y = getMoversYear(latestYear || new Date().getFullYear(), ys);
      setMoversYear(y);
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

  function getMoversMonth(defaultMonth) {
    try {
      const n = parseInt(localStorage.getItem("moversMonth") || "", 10);
      if (n >= 1 && n <= 12) return n;
    } catch (_) {}
    return (defaultMonth >= 1 && defaultMonth <= 12) ? defaultMonth : 1;
  }

  function setMoversMonth(v) {
    try { localStorage.setItem("moversMonth", String(parseInt(v, 10) || 1)); } catch (_) {}
  }

  function getMoversYear(defaultYear, years) {
    let y = defaultYear;
    try {
      const n = parseInt(localStorage.getItem("moversYear") || "", 10);
      if (!isNaN(n)) y = n;
    } catch (_) {}
    if (Array.isArray(years) && years.length) {
      if (years.includes(y)) return y;
      return years[years.length - 1];
    }
    return y;
  }

  function setMoversYear(v) {
    try { localStorage.setItem("moversYear", String(parseInt(v, 10) || new Date().getFullYear())); } catch (_) {}
  }

  function getActiveRange() {
    const wrap = el("rangeButtons");
    if (!wrap) return "30";
    const on = wrap.querySelector("button.on");
    return on ? on.getAttribute("data-range") : "30";
  }

  document.addEventListener("DOMContentLoaded", () => {
    initRangeButtons();
    initAllocControls();
    initMoversControls();
    initMoversPickerControls();
    if (el("chartTotal")) loadDashboard(getActiveRange());
    if (el("assetsTable")) loadAssetsPage();
    if (el("chartHistory")) loadHistoryPage();
  });
})();
