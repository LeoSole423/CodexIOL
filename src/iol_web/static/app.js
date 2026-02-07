(function () {
  const fmtARS = new Intl.NumberFormat("es-AR", {
    style: "currency",
    currency: "ARS",
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

  async function fetchJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    return await r.json();
  }

  function renderTable(targetId, rows, metricKey, metricLabel) {
    const root = el(targetId);
    if (!root) return;

    const head = `
      <th>Símbolo</th>
      <th>Desc</th>
      <th class="num">Valor</th>
      <th class="num">${metricLabel || (metricKey === "daily_var_points" ? "Día" : "PnL")}</th>
    `;

    const body = (rows || []).map((r) => {
      const metric = r[metricKey];
      const pct = (metricKey === "delta_value") ? r["delta_pct"] : null;
      const metricText = fmtDeltaARS(metric);
      const pctText = (pct == null) ? "" : ` <span class="muted">(${fmtPct(pct)})</span>`;
      return `
        <tr>
          <td>${r.symbol || "-"}</td>
          <td class="muted">${(r.description || "").slice(0, 42)}</td>
          <td class="num">${fmtARS.format(r.total_value || 0)}</td>
          <td class="num ${signClass(metric)}">${metricText}${pctText}</td>
        </tr>
      `;
    }).join("");

    root.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  }

  let chartTotal = null;
  let chartAlloc = null;
  let chartHistory = null;
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
    const latest = await fetchJSON("/api/latest");
    const ret = await fetchJSON("/api/returns");

    if (!latest || !latest.snapshot) {
      const hint = el("noDataHint");
      if (hint) hint.style.display = "block";
      return;
    }

    const snap = latest.snapshot;
    if (el("kpiTotal")) el("kpiTotal").textContent = fmtARS.format(snap.total_value || 0);
    if (el("kpiDate")) el("kpiDate").textContent = `Snapshot: ${snap.snapshot_date}`;

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
    let moversUrl = `/api/movers?kind=period&period=${encodeURIComponent(period)}&limit=10`;
    if (period === "monthly") {
      const m = getMoversMonth(latestMonth || 1);
      moversUrl = `/api/movers?kind=period&period=monthly&month=${encodeURIComponent(m)}&year=${encodeURIComponent(latestYear)}&limit=10`;
    } else if (period === "yearly") {
      const y = getMoversYear(latestYear || new Date().getFullYear(), years);
      moversUrl = `/api/movers?kind=period&period=yearly&year=${encodeURIComponent(y)}&limit=10`;
    }

    const moversDaily = await fetchJSON(moversUrl);
    updateMoversPicker(period, latestYear, latestMonth, years);
    if (period !== "daily" && moversDaily && moversDaily.from == null) {
      const msg = (period === "monthly")
        ? "No hay snapshots para ese mes."
        : (period === "yearly")
          ? "No hay snapshots para ese año."
          : "No hay snapshots suficientes para ese período. Deja corriendo el scheduler o ejecuta `iol snapshot run` en distintos días.";
      const a = el("tblGainersDaily");
      const b = el("tblLosersDaily");
      if (a) a.innerHTML = `<div class="hint">${msg}</div>`;
      if (b) b.innerHTML = `<div class="hint">${msg}</div>`;
    } else {
      renderTable("tblGainersDaily", moversDaily.gainers || [], "delta_value", "Cambio");
      renderTable("tblLosersDaily", moversDaily.losers || [], "delta_value", "Cambio");
    }

    const map = { daily: "día", weekly: "semana", monthly: "mes", yearly: "año", ytd: "ytd" };
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
        <th>Símbolo</th>
        <th>Desc</th>
        <th>Tipo</th>
        <th>Mercado</th>
        <th class="num">Valor</th>
        <th class="num">Día</th>
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
