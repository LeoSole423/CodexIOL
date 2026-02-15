from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def parse_iso_date(v: Optional[str], default: Optional[str] = None) -> str:
    vv = (v or "").strip()
    if vv:
        date.fromisoformat(vv)
        return vv
    if default:
        return default
    return date.today().isoformat()


def panel_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        rows = payload.get("titulos") or payload.get("items") or []
        if isinstance(rows, list):
            return [dict(x) for x in rows if isinstance(x, dict)]
    return []


def _extract_bid_ask_from_puntas(puntas: Any) -> Tuple[Optional[float], Optional[float]]:
    if not isinstance(puntas, list) or not puntas:
        return None, None
    p0 = puntas[0]
    if not isinstance(p0, dict):
        return None, None
    bid = _safe_float(p0.get("precioCompra"))
    ask = _safe_float(p0.get("precioVenta"))
    return bid, ask


def compute_spread_pct(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None
    return (ask - bid) / mid * 100.0


def snapshot_row_from_quote(snapshot_date: str, symbol: str, quote: Dict[str, Any], market: str = "bcba") -> Dict[str, Any]:
    bid, ask = _extract_bid_ask_from_puntas(quote.get("puntas"))
    last_price = _safe_float(quote.get("ultimoPrecio"))
    spread_pct = compute_spread_pct(bid, ask)
    daily_var_pct = _safe_float(quote.get("variacionPorcentual"))
    if daily_var_pct is None:
        daily_var_pct = _safe_float(quote.get("variacionDiaria"))
    operations_count = _safe_float(quote.get("cantidadOperaciones"))
    volume_amount = _safe_float(quote.get("volumenOperado"))
    if volume_amount is None:
        volume_amount = _safe_float(quote.get("montoOperado"))
    if volume_amount is None:
        volume_amount = _safe_float(quote.get("volumenNominal"))
    return {
        "snapshot_date": snapshot_date,
        "symbol": symbol,
        "market": market,
        "last_price": last_price,
        "bid": bid,
        "ask": ask,
        "spread_pct": spread_pct,
        "daily_var_pct": daily_var_pct,
        "operations_count": operations_count,
        "volume_amount": volume_amount,
        "source": "quote",
    }


def snapshot_row_from_panel(snapshot_date: str, row: Dict[str, Any], market: str = "bcba") -> Optional[Dict[str, Any]]:
    symbol = (row.get("simbolo") or row.get("symbol") or "").strip()
    if not symbol:
        return None
    bid, ask = _extract_bid_ask_from_puntas(row.get("puntas"))
    last_price = _safe_float(row.get("ultimoPrecio"))
    spread_pct = compute_spread_pct(bid, ask)
    daily_var_pct = _safe_float(row.get("variacionPorcentual"))
    if daily_var_pct is None:
        daily_var_pct = _safe_float(row.get("variacionDiaria"))
    operations_count = _safe_float(row.get("cantidadOperaciones"))
    volume_amount = _safe_float(row.get("volumenOperado"))
    if volume_amount is None:
        volume_amount = _safe_float(row.get("montoOperado"))
    if volume_amount is None:
        volume_amount = _safe_float(row.get("volumenNominal"))
    return {
        "snapshot_date": snapshot_date,
        "symbol": symbol,
        "market": market,
        "last_price": last_price,
        "bid": bid,
        "ask": ask,
        "spread_pct": spread_pct,
        "daily_var_pct": daily_var_pct,
        "operations_count": operations_count,
        "volume_amount": volume_amount,
        "source": "panel_quotes",
    }


def latest_metrics_by_symbol(rows: Sequence[Dict[str, Any]], as_of: str) -> Dict[str, Dict[str, Any]]:
    by_symbol: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        d = str(r.get("snapshot_date") or "")
        s = str(r.get("symbol") or "")
        if not d or not s or d > as_of:
            continue
        cur = by_symbol.get(s)
        if cur is None:
            by_symbol[s] = dict(r)
            continue
        # Prefer most recent date; if same date prefer quote source.
        if d > str(cur.get("snapshot_date") or ""):
            by_symbol[s] = dict(r)
            continue
        if d == str(cur.get("snapshot_date") or ""):
            src = str(r.get("source") or "")
            cur_src = str(cur.get("source") or "")
            if src == "quote" and cur_src != "quote":
                by_symbol[s] = dict(r)
    return by_symbol


def price_series_by_symbol(rows: Sequence[Dict[str, Any]], as_of: str) -> Dict[str, List[Tuple[str, float]]]:
    by: Dict[str, List[Tuple[str, float]]] = {}
    for r in rows:
        d = str(r.get("snapshot_date") or "")
        if not d or d > as_of:
            continue
        s = str(r.get("symbol") or "")
        p = _safe_float(r.get("last_price"))
        if not s or p is None or p <= 0:
            continue
        by.setdefault(s, []).append((d, float(p)))
    for s in by:
        by[s].sort(key=lambda x: x[0])
    return by


def _price_on_or_before(series: Sequence[Tuple[str, float]], target: str) -> Optional[float]:
    out = None
    for d, p in series:
        if d <= target:
            out = p
        else:
            break
    return out


def _rolling_prices(series: Sequence[Tuple[str, float]], as_of: str, n: int) -> List[float]:
    vals = [p for d, p in series if d <= as_of]
    if n <= 0:
        return vals
    return vals[-n:]


def drawdown_pct(series: Sequence[Tuple[str, float]], as_of: str) -> Optional[float]:
    vals = _rolling_prices(series, as_of, 20)
    if not vals:
        return None
    cur = vals[-1]
    mx = max(vals)
    if mx <= 0:
        return None
    return (cur / mx - 1.0) * 100.0


def value_score(series: Sequence[Tuple[str, float]], as_of: str) -> float:
    vals = _rolling_prices(series, as_of, 28)
    if not vals:
        return 50.0
    cur = vals[-1]
    mean = sum(vals) / float(len(vals))
    if mean <= 0:
        return 50.0
    dev = (cur / mean - 1.0) * 100.0
    # Cheaper vs recent mean -> higher score.
    return clamp(50.0 - dev * 2.0, 0.0, 100.0)


def momentum_score(series: Sequence[Tuple[str, float]], as_of: str) -> float:
    d = date.fromisoformat(as_of)
    p_now = _price_on_or_before(series, as_of)
    if p_now is None or p_now <= 0:
        return 50.0
    p_7 = _price_on_or_before(series, (d - timedelta(days=7)).isoformat())
    p_28 = _price_on_or_before(series, (d - timedelta(days=28)).isoformat())
    r7 = 0.0
    r28 = 0.0
    if p_7 is not None and p_7 > 0:
        r7 = (p_now / p_7 - 1.0) * 100.0
    if p_28 is not None and p_28 > 0:
        r28 = (p_now / p_28 - 1.0) * 100.0
    return clamp(50.0 + r7 * 2.0 + r28 * 1.0, 0.0, 100.0)


def evidence_stats(rows: Sequence[Dict[str, Any]], as_of: str) -> Dict[str, Any]:
    d_asof = date.fromisoformat(as_of)
    recent_45: List[Dict[str, Any]] = []
    recent_14: List[Dict[str, Any]] = []
    for r in rows:
        raw = str(r.get("retrieved_at_utc") or "")
        if len(raw) < 10:
            continue
        try:
            rd = date.fromisoformat(raw[:10])
        except Exception:
            continue
        delta = (d_asof - rd).days
        if delta < 0:
            continue
        if delta <= 45:
            recent_45.append(dict(r))
        if delta <= 14:
            recent_14.append(dict(r))

    def _conf_points(conf: str) -> int:
        c = (conf or "").strip().lower()
        if c == "high":
            return 3
        if c == "medium":
            return 2
        if c == "low":
            return 1
        return 0

    catalyst_raw = 0.0
    has_thesis = False
    for r in recent_45:
        p = _conf_points(str(r.get("confidence") or ""))
        if p >= 2:
            has_thesis = True
        # Recent evidence weights more.
        raw = str(r.get("retrieved_at_utc") or "")
        rd = date.fromisoformat(raw[:10])
        delta = (d_asof - rd).days
        rec = 1.0 if delta <= 14 else 0.5
        catalyst_raw += float(p) * rec
    catalyst = clamp(catalyst_raw * 15.0, 0.0, 100.0)

    # "Unresolved conflict": same non-empty conflict_key with distinct claims in last 45d.
    by_key: Dict[str, set] = {}
    for r in recent_45:
        k = str(r.get("conflict_key") or "").strip()
        if not k:
            continue
        claim = str(r.get("claim") or "").strip()
        if not claim:
            continue
        by_key.setdefault(k, set()).add(claim)
    unresolved = any(len(v) > 1 for v in by_key.values())
    if unresolved:
        catalyst = clamp(catalyst - 30.0, 0.0, 100.0)

    has_recent_catalyst = any(
        (str(r.get("confidence") or "").strip().lower() in ("medium", "high")) for r in recent_14
    )
    return {
        "catalyst_score": catalyst,
        "has_thesis": has_thesis,
        "has_recent_catalyst": has_recent_catalyst,
        "unresolved_conflict": unresolved,
    }


def allocate_with_caps(raw_weights: Dict[str, float], caps: Dict[str, float]) -> Dict[str, float]:
    symbols = [s for s, w in raw_weights.items() if float(w or 0.0) > 0.0]
    if not symbols:
        return {}

    rem = set(symbols)
    alloc = {s: 0.0 for s in symbols}
    remaining_total = 100.0
    weights = {s: max(0.0, float(raw_weights.get(s) or 0.0)) for s in symbols}
    caps_n = {s: max(0.0, float(caps.get(s) or 0.0)) for s in symbols}

    while rem and remaining_total > 1e-9:
        total_w = sum(weights[s] for s in rem)
        if total_w <= 1e-12:
            break
        hit = []
        for s in list(rem):
            proposed = remaining_total * weights[s] / total_w
            cap = caps_n[s]
            if proposed >= cap - 1e-9:
                alloc[s] = cap
                remaining_total -= cap
                hit.append(s)
        if not hit:
            for s in rem:
                alloc[s] = remaining_total * weights[s] / total_w
            remaining_total = 0.0
            break
        for s in hit:
            rem.discard(s)

    # Normalize tiny numeric drift.
    for s in alloc:
        alloc[s] = max(0.0, float(alloc[s]))
    return alloc


@dataclass
class OpportunityCandidate:
    symbol: str
    candidate_type: str
    score_total: float
    score_risk: float
    score_value: float
    score_momentum: float
    score_catalyst: float
    entry_low: Optional[float]
    entry_high: Optional[float]
    suggested_weight_pct: Optional[float]
    suggested_amount_ars: Optional[float]
    reason_summary: str
    risk_flags_json: str
    filters_passed: int
    current_weight_pct: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "candidate_type": self.candidate_type,
            "score_total": self.score_total,
            "score_risk": self.score_risk,
            "score_value": self.score_value,
            "score_momentum": self.score_momentum,
            "score_catalyst": self.score_catalyst,
            "entry_low": self.entry_low,
            "entry_high": self.entry_high,
            "suggested_weight_pct": self.suggested_weight_pct,
            "suggested_amount_ars": self.suggested_amount_ars,
            "reason_summary": self.reason_summary,
            "risk_flags_json": self.risk_flags_json,
            "filters_passed": self.filters_passed,
            "current_weight_pct": self.current_weight_pct,
        }


def build_candidates(
    as_of: str,
    mode: str,
    budget_ars: float,
    top_n: int,
    portfolio_total_ars: float,
    holdings_value_by_symbol: Dict[str, float],
    latest_metrics: Dict[str, Dict[str, Any]],
    series_by_symbol: Dict[str, List[Tuple[str, float]]],
    evidence_by_symbol: Dict[str, List[Dict[str, Any]]],
) -> List[OpportunityCandidate]:
    mode_n = (mode or "").strip().lower()
    out: List[OpportunityCandidate] = []
    for symbol, m in latest_metrics.items():
        in_port = float(holdings_value_by_symbol.get(symbol, 0.0) or 0.0) > 0.0
        candidate_type = None
        if mode_n == "new":
            if not in_port:
                candidate_type = "new"
        elif mode_n == "rebuy":
            if in_port:
                candidate_type = "rebuy"
        else:  # both
            candidate_type = "rebuy" if in_port else "new"
        if candidate_type is None:
            continue

        s = series_by_symbol.get(symbol, [])
        ev = evidence_by_symbol.get(symbol, [])
        evs = evidence_stats(ev, as_of)
        dd = drawdown_pct(s, as_of)
        v_score = value_score(s, as_of)
        m_score = momentum_score(s, as_of)
        c_score = float(evs["catalyst_score"])
        cur_w = 0.0
        if portfolio_total_ars > 0:
            cur_w = float(holdings_value_by_symbol.get(symbol, 0.0) or 0.0) / float(portfolio_total_ars) * 100.0

        flags: List[str] = []
        hard_ok = True
        risk_penalty = 0.0

        bid = _safe_float(m.get("bid"))
        ask = _safe_float(m.get("ask"))
        spread = _safe_float(m.get("spread_pct"))
        ops = _safe_float(m.get("operations_count"))

        if bid is not None and ask is not None and bid > 0 and ask > 0:
            spread = spread if spread is not None else compute_spread_pct(bid, ask)
            if spread is not None and spread > 2.5:
                hard_ok = False
                flags.append("LIQUIDITY_SPREAD")
            elif spread is not None and spread > 1.5:
                risk_penalty += 10.0
        elif ops is not None:
            if ops <= 0:
                hard_ok = False
                flags.append("LIQUIDITY_NO_OPS")
            elif ops < 5:
                risk_penalty += 10.0
        else:
            flags.append("LIQUIDITY_UNKNOWN")
            risk_penalty += 20.0

        if cur_w >= 15.0:
            hard_ok = False
            flags.append("CONCENTRATION_MAX")
        elif cur_w > 12.0:
            risk_penalty += 15.0

        if dd is not None and dd < -25.0 and not bool(evs["has_recent_catalyst"]):
            hard_ok = False
            flags.append("DRAWDOWN_EXTREME")
        elif dd is not None and dd < -20.0:
            risk_penalty += 10.0

        if bool(evs["unresolved_conflict"]):
            flags.append("EVIDENCE_CONFLICT")
            risk_penalty += 10.0

        # Rebuy rule = buy the dip + thesis valid + no unresolved conflict.
        if candidate_type == "rebuy":
            dip_ok = dd is not None and dd <= -8.0
            thesis_ok = bool(evs["has_thesis"]) and not bool(evs["unresolved_conflict"])
            if not dip_ok or not thesis_ok:
                continue

        risk_score = clamp(100.0 - risk_penalty, 0.0, 100.0)
        total = 0.35 * risk_score + 0.20 * v_score + 0.35 * m_score + 0.10 * c_score

        last_price = _safe_float(m.get("last_price"))
        entry_low = None
        entry_high = None
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            entry_low = mid * 0.99
            entry_high = ask * 1.01
        elif last_price is not None and last_price > 0:
            entry_low = last_price * 0.99
            entry_high = last_price * 1.01

        reason = (
            f"{candidate_type} | dd20={dd:.2f}% "
            if dd is not None
            else f"{candidate_type} | dd20=NA "
        )
        reason += f"| risk={risk_score:.1f} value={v_score:.1f} momentum={m_score:.1f} catalyst={c_score:.1f}"
        if flags:
            reason += f" | flags={','.join(flags)}"

        out.append(
            OpportunityCandidate(
                symbol=symbol,
                candidate_type=candidate_type,
                score_total=float(total),
                score_risk=float(risk_score),
                score_value=float(v_score),
                score_momentum=float(m_score),
                score_catalyst=float(c_score),
                entry_low=entry_low,
                entry_high=entry_high,
                suggested_weight_pct=None,
                suggested_amount_ars=None,
                reason_summary=reason,
                risk_flags_json=str(flags).replace("'", '"'),
                filters_passed=1 if hard_ok else 0,
                current_weight_pct=cur_w,
            )
        )

    out.sort(key=lambda c: c.score_total, reverse=True)

    operable = [c for c in out if c.filters_passed == 1 and c.score_total >= 50.0]
    if not operable:
        operable = [c for c in out if c.filters_passed == 1]
    selected = operable[: int(top_n)]
    if not selected:
        return out

    # Sizing with caps.
    multipliers: Dict[str, float] = {}
    caps_budget_weight: Dict[str, float] = {}
    for c in selected:
        if c.score_total >= 80.0:
            mult = 1.5
        elif c.score_total >= 65.0:
            mult = 1.0
        elif c.score_total >= 50.0:
            mult = 0.5
        else:
            mult = 0.0
        multipliers[c.symbol] = mult

        max_additional_portfolio_pct = max(0.0, 15.0 - c.current_weight_pct)
        if c.candidate_type == "new":
            max_additional_portfolio_pct = min(max_additional_portfolio_pct, 8.0)
        if budget_ars <= 0 or portfolio_total_ars <= 0:
            cap_w = 0.0
        else:
            max_amount = portfolio_total_ars * max_additional_portfolio_pct / 100.0
            cap_w = max(0.0, min(100.0, max_amount / budget_ars * 100.0))
        caps_budget_weight[c.symbol] = cap_w

    alloc = allocate_with_caps(multipliers, caps_budget_weight)
    alloc_by_symbol = {k: float(v) for k, v in alloc.items()}

    for c in selected:
        w = alloc_by_symbol.get(c.symbol)
        if w is None:
            continue
        c.suggested_weight_pct = float(w)
        c.suggested_amount_ars = float(budget_ars) * float(w) / 100.0

    return out


def report_markdown(
    run: Dict[str, Any],
    candidates: Sequence[Dict[str, Any]],
) -> str:
    created = str(run.get("created_at_utc") or "-")
    as_of = str(run.get("as_of") or "-")
    mode = str(run.get("mode") or "-")
    budget = _safe_float(run.get("budget_ars")) or 0.0
    top_n = int(run.get("top_n") or 0)
    rows = list(candidates or [])
    top = [r for r in rows if int(r.get("filters_passed") or 0) == 1][:top_n]
    lines: List[str] = []
    lines.append("# Oportunidades de Portafolio (Semanal)")
    lines.append("")
    lines.append(f"- `created_at_utc`: {created}")
    lines.append(f"- `as_of`: {as_of}")
    lines.append(f"- `mode`: {mode}")
    lines.append(f"- `budget_ars`: {budget:,.2f}".replace(",", "."))
    lines.append("")
    lines.append("## Top candidatos operables")
    if not top:
        lines.append("- Sin candidatos operables para este run.")
        return "\n".join(lines) + "\n"

    lines.append("")
    lines.append("| Symbol | Tipo | Score | Entry Low | Entry High | Weight % | Amount ARS |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in top:
        lines.append(
            "| {sym} | {typ} | {sc:.2f} | {el} | {eh} | {w} | {amt} |".format(
                sym=r.get("symbol"),
                typ=r.get("candidate_type"),
                sc=float(r.get("score_total") or 0.0),
                el=("-" if r.get("entry_low") is None else f"{float(r.get('entry_low')):.2f}"),
                eh=("-" if r.get("entry_high") is None else f"{float(r.get('entry_high')):.2f}"),
                w=("-" if r.get("suggested_weight_pct") is None else f"{float(r.get('suggested_weight_pct')):.2f}"),
                amt=("-" if r.get("suggested_amount_ars") is None else f"{float(r.get('suggested_amount_ars')):.2f}"),
            )
        )
    lines.append("")
    lines.append("## Razones y riesgos")
    for r in top:
        lines.append(f"- **{r.get('symbol')}**: {r.get('reason_summary')}")
    lines.append("")
    lines.append("Nota: esto no ejecuta ordenes reales; usar simulacion y confirmacion explicita.")
    return "\n".join(lines) + "\n"
