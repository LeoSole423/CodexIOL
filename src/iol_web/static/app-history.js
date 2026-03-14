import { el, fetchJSON, fmtNum, fmtPct, fmtMonthLabel } from "./app-utils.js";
import { buildLineChart, buildMultiLineChart, buildBarChart } from "./app-charts.js";

let chartHistory = null;
let chartInflationSeries = null;
let chartInflationAnnual = null;

export async function loadHistoryPage() {
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
