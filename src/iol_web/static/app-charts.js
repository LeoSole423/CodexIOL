import { fmtARS, fmtNum, fmtPct } from "./app-utils.js";

export function buildLineChart(canvas, labels, values) {
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

export function buildMultiLineChart(canvas, labels, datasets, yTickFormatter) {
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

export function buildBarChart(canvas, labels, datasets, yTickFormatter, tooltipAfterBody) {
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

export function buildPieChart(canvas, labels, values) {
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

export function renderAllocLegend(containerId, labels, values, colors) {
  const root = document.getElementById(containerId);
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
