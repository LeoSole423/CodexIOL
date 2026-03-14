import { el, fetchJSON, fmtARS, fmtDeltaARS, signClass } from "./app-utils.js";

export async function loadAssetsPage() {
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
