from __future__ import annotations

import json
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


def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
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


def _parse_notes_json(v: Any) -> Dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return dict(v)
    s = str(v).strip()
    if not s:
        return {}
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return {}
    return {}


_CRYPTO_SYMBOL_HINTS = {
    "IBIT",
    "ETHA",
    "FBTC",
    "BITB",
    "ARKB",
    "EZBC",
    "GBTC",
    "ETHE",
    "BTCO",
}


_SECTOR_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "technology": (
        "software",
        "semiconductor",
        "chip",
        "cloud",
        "artificial intelligence",
        "ai ",
        "technology",
        "internet",
        "data center",
        "cyber",
    ),
    "healthcare": (
        "pharma",
        "pharmaceutical",
        "biotech",
        "biotechnology",
        "medical",
        "healthcare",
        "drug",
        "hospital",
    ),
    "financials": (
        "bank",
        "banking",
        "finance",
        "financial",
        "insurance",
        "credit",
        "payment",
        "asset management",
    ),
    "energy": (
        "oil",
        "gas",
        "energy",
        "renewable",
        "utility",
        "utilities",
    ),
    "industrials": (
        "industrial",
        "aerospace",
        "defense",
        "railroad",
        "transport",
        "logistics",
        "machinery",
    ),
    "consumer": (
        "retail",
        "consumer",
        "apparel",
        "food",
        "beverage",
        "restaurant",
        "travel",
        "hotel",
        "automotive",
        "auto",
    ),
    "materials": (
        "mining",
        "steel",
        "chemical",
        "materials",
        "metals",
    ),
    "real_estate": (
        "real estate",
        "reit",
        "property",
    ),
    "telecom": (
        "telecom",
        "telecommunications",
        "wireless",
        "media",
        "broadband",
    ),
    "crypto": (
        "bitcoin",
        "btc",
        "ethereum",
        "ether",
        "crypto",
        "blockchain",
        "digital asset",
    ),
}


def _is_crypto_symbol_hint(symbol: str) -> bool:
    s = (symbol or "").strip().upper()
    if not s:
        return False
    if s in _CRYPTO_SYMBOL_HINTS:
        return True
    if s.startswith("BTC") or s.startswith("ETH"):
        return True
    return False


def _sector_hits_from_text(text: str) -> Dict[str, int]:
    t = (text or "").strip().lower()
    out: Dict[str, int] = {}
    if not t:
        return out
    for sector, words in _SECTOR_KEYWORDS.items():
        hits = 0
        for w in words:
            if w in t:
                hits += 1
        if hits > 0:
            out[sector] = int(hits)
    return out


def _infer_sector_bucket(symbol: str, rows: Sequence[Dict[str, Any]]) -> str:
    if _is_crypto_symbol_hint(symbol):
        return "crypto"
    agg: Dict[str, int] = {}
    for r in rows:
        notes = _parse_notes_json(r.get("notes"))
        hint = str(notes.get("sector_hint") or notes.get("sic_description") or "").strip().lower()
        chunks = [
            hint,
            str(r.get("claim") or ""),
            str(r.get("query") or ""),
            str(r.get("source_name") or ""),
        ]
        merged = " | ".join(chunks)
        hits = _sector_hits_from_text(merged)
        for k, v in hits.items():
            agg[k] = int(agg.get(k, 0)) + int(v)
    if not agg:
        return "unknown"
    best_sector = "unknown"
    best_score = -1
    for k, v in agg.items():
        if v > best_score:
            best_sector = k
            best_score = int(v)
    return best_sector


def _liquidity_score(
    *,
    spread_pct: Optional[float],
    operations_count: Optional[float],
    volume_amount: Optional[float],
) -> float:
    score = 50.0
    if spread_pct is not None:
        score += clamp((2.5 - float(spread_pct)) * 12.0, -20.0, 20.0)
    if operations_count is not None:
        ops = max(0.0, float(operations_count))
        score += clamp((ops - 5.0) * 0.8, -15.0, 20.0)
    if volume_amount is not None:
        vol = max(0.0, float(volume_amount))
        score += clamp((vol / 100000.0) * 10.0, -10.0, 20.0)
    return clamp(score, 0.0, 100.0)


def evidence_stats(rows: Sequence[Dict[str, Any]], as_of: str) -> Dict[str, Any]:
    d_asof = date.fromisoformat(as_of)
    recent_45: List[Dict[str, Any]] = []
    recent_14: List[Dict[str, Any]] = []
    freshest_age_days: Optional[int] = None
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
        if freshest_age_days is None or delta < freshest_age_days:
            freshest_age_days = int(delta)
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
    trusted_ref_keys = set()
    fresh_trusted_ref_keys = set()
    trusted_rows: List[Dict[str, Any]] = []
    fresh_trusted_rows: List[Dict[str, Any]] = []
    stances: List[float] = []
    has_bullish = False
    has_bearish = False
    has_neutral = False

    for r in recent_45:
        n = _parse_notes_json(r.get("notes"))
        tier = str(n.get("source_tier") or "").strip().lower()
        if tier not in ("official", "reuters"):
            continue
        trusted_rows.append(r)
        key = (
            str(r.get("source_name") or "").strip().lower(),
            str(r.get("source_url") or "").strip().lower(),
            str(r.get("published_date") or "").strip().lower(),
        )
        trusted_ref_keys.add(key)
        raw = str(r.get("retrieved_at_utc") or "")
        rd = date.fromisoformat(raw[:10])
        age_days = (d_asof - rd).days
        max_age = 120 if tier == "official" else 10
        if age_days <= max_age:
            fresh_trusted_rows.append(r)
            fresh_trusted_ref_keys.add(key)
        stance = str(n.get("stance") or "").strip().lower()
        conf = str(r.get("confidence") or "").strip().lower()
        conf_mult = 1.0 if conf == "high" else (0.6 if conf == "medium" else 0.3)
        if age_days <= max_age:
            if stance == "bullish":
                stances.append(1.0 * conf_mult)
                has_bullish = True
            elif stance == "bearish":
                stances.append(-1.0 * conf_mult)
                has_bearish = True
            else:
                stances.append(0.0)
                has_neutral = True

    expert_signal = 50.0
    if stances:
        avg = sum(stances) / float(len(stances))
        expert_signal = clamp(50.0 + avg * 50.0, 0.0, 100.0)
    trusted_refs_count = len(trusted_ref_keys)
    fresh_trusted_refs_count = len(fresh_trusted_ref_keys)

    if fresh_trusted_refs_count <= 0:
        consensus_state = "insufficient"
    elif has_bullish and has_bearish:
        consensus_state = "conflict"
    elif (has_bullish and has_neutral) or (has_bearish and has_neutral):
        consensus_state = "mixed"
    elif has_bullish or has_bearish or has_neutral:
        consensus_state = "aligned"
    else:
        consensus_state = "insufficient"

    evidence_summary = {
        "trusted_refs_count": trusted_refs_count,
        "trusted_rows_count": len(trusted_rows),
        "fresh_trusted_refs_count": fresh_trusted_refs_count,
        "fresh_trusted_rows_count": len(fresh_trusted_rows),
        "consensus_state": consensus_state,
        "expert_signal_score": expert_signal,
        "freshest_age_days": freshest_age_days,
    }
    return {
        "catalyst_score": catalyst,
        "has_thesis": has_thesis,
        "has_recent_catalyst": has_recent_catalyst,
        "unresolved_conflict": bool(unresolved or consensus_state == "conflict"),
        "trusted_refs_count": trusted_refs_count,
        "fresh_trusted_refs_count": fresh_trusted_refs_count,
        "expert_signal_score": expert_signal,
        "consensus_state": consensus_state,
        "freshest_age_days": freshest_age_days,
        "evidence_summary_json": json.dumps(evidence_summary, ensure_ascii=True, sort_keys=True),
    }


def _percentile_score(values: Sequence[float], value: float) -> float:
    vals = sorted(float(v) for v in values)
    if not vals:
        return 50.0
    if len(vals) == 1:
        return 100.0
    last_idx = 0
    for idx, cur in enumerate(vals):
        if float(cur) <= float(value):
            last_idx = idx
        else:
            break
    return clamp((float(last_idx) / float(len(vals) - 1)) * 100.0, 0.0, 100.0)


def summarize_run_metrics(candidates: Sequence["OpportunityCandidate"]) -> Dict[str, Any]:
    rows = list(candidates or [])
    scores = [float(c.score_total) for c in rows]
    mean_score = sum(scores) / float(len(scores)) if scores else 0.0
    variance = sum((s - mean_score) ** 2 for s in scores) / float(len(scores)) if scores else 0.0
    stddev = variance ** 0.5
    operable = [c for c in rows if str(c.candidate_status) == "operable"]
    watchlist = [c for c in rows if str(c.candidate_status) == "watchlist"]
    manual = [c for c in rows if str(c.candidate_status) == "manual_review"]
    rejected = [c for c in rows if str(c.candidate_status) == "rejected"]
    buy_signals = [c for c in rows if str(c.signal_side) == "buy"]
    sell_signals = [c for c in rows if str(c.signal_side) == "sell"]
    fresh_refs = 0
    freshest_days: List[int] = []
    for c in rows:
        summary = _parse_notes_json(c.evidence_summary_json)
        fresh_refs += int(summary.get("fresh_trusted_refs_count") or 0)
        age = summary.get("freshest_age_days")
        if age is not None:
            try:
                freshest_days.append(int(age))
            except Exception:
                pass
    total = len(rows)
    return {
        "candidate_count": total,
        "operable_count": len(operable),
        "watchlist_count": len(watchlist),
        "manual_review_count": len(manual),
        "rejected_count": len(rejected),
        "buy_signal_count": len(buy_signals),
        "sell_signal_count": len(sell_signals),
        "score_mean": mean_score,
        "score_stddev": stddev,
        "score_dispersion": (max(scores) - min(scores)) if scores else 0.0,
        "operable_ratio": (len(operable) / float(total)) if total else 0.0,
        "watchlist_ratio": (len(watchlist) / float(total)) if total else 0.0,
        "rejected_ratio": (len(rejected) / float(total)) if total else 0.0,
        "fresh_evidence_ratio": (sum(1 for c in rows if int(c.trusted_refs_count or 0) > 0) / float(total)) if total else 0.0,
        "fresh_trusted_refs_total": fresh_refs,
        "latest_evidence_age_days_avg": (sum(freshest_days) / float(len(freshest_days))) if freshest_days else None,
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
    signal_side: str
    signal_family: str
    score_version: str
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
    expert_signal_score: float
    trusted_refs_count: int
    consensus_state: str
    decision_gate: str
    candidate_status: str
    evidence_summary_json: str
    liquidity_score: float
    sector_bucket: str
    is_crypto_proxy: int
    holding_context_json: str
    score_features_json: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "candidate_type": self.candidate_type,
            "signal_side": self.signal_side,
            "signal_family": self.signal_family,
            "score_version": self.score_version,
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
            "expert_signal_score": self.expert_signal_score,
            "trusted_refs_count": self.trusted_refs_count,
            "consensus_state": self.consensus_state,
            "decision_gate": self.decision_gate,
            "candidate_status": self.candidate_status,
            "evidence_summary_json": self.evidence_summary_json,
            "liquidity_score": self.liquidity_score,
            "sector_bucket": self.sector_bucket,
            "is_crypto_proxy": self.is_crypto_proxy,
            "holding_context_json": self.holding_context_json,
            "score_features_json": self.score_features_json,
        }


def resolve_conflicts(
    candidates: List["OpportunityCandidate"],
    target_weights_by_symbol: Optional[Dict[str, float]] = None,
) -> List["OpportunityCandidate"]:
    """Post-process candidates to resolve simultaneous buy/sell for the same symbol.

    For each symbol with both a buy and a sell candidate, applies a priority hierarchy:
    1. Portfolio vs structural target: if current_weight is meaningfully below target,
       suppress sells; if meaningfully above, suppress buys. If within ±2pp, suppress both.
    2. Score dominance (when no target available): keep the signal whose score exceeds
       the other by at least 20 points; otherwise suppress both (no conviction).

    Suppressed candidates are kept in the list with candidate_status="suppressed" and
    a resolution_reason field appended to their reason_summary for auditability.
    """
    from collections import defaultdict

    by_symbol: Dict[str, List["OpportunityCandidate"]] = defaultdict(list)
    for c in candidates:
        by_symbol[c.symbol].append(c)

    result: List["OpportunityCandidate"] = []
    for symbol, group in by_symbol.items():
        buys = [c for c in group if c.signal_side == "buy"]
        sells = [c for c in group if c.signal_side == "sell"]

        if not buys or not sells:
            # No conflict — but still apply target-aware suppression for lone signals
            if target_weights_by_symbol:
                target_w = float((target_weights_by_symbol or {}).get(symbol, 0.0) or 0.0)
                if target_w > 0.0:
                    cur_w = float(group[0].current_weight_pct)
                    deviation = cur_w - target_w
                    for c in group:
                        if c.signal_side == "sell" and deviation <= 2.0:
                            # Symbol is on-track or underweight — no reason to sell
                            c.candidate_status = "suppressed"
                            c.reason_summary = c.reason_summary + f" | resolution=target_no_sell(cur={cur_w:.1f}%,tgt={target_w:.1f}%)"
                        elif c.signal_side == "buy" and deviation >= 2.0:
                            # Symbol is overweight — no reason to buy more
                            c.candidate_status = "suppressed"
                            c.reason_summary = c.reason_summary + f" | resolution=target_no_buy(cur={cur_w:.1f}%,tgt={target_w:.1f}%)"
            result.extend(group)
            continue

        best_buy = max(buys, key=lambda c: c.score_total)
        best_sell = max(sells, key=lambda c: c.score_total)

        target_w = float((target_weights_by_symbol or {}).get(symbol, 0.0) or 0.0)
        cur_w = float(best_buy.current_weight_pct)
        suppress_buys = False
        suppress_sells = False
        reason = ""

        if target_w > 0.0:
            deviation = cur_w - target_w
            if deviation < -2.0:
                # Underweight vs target → suppress sells
                suppress_sells = True
                reason = f"target_underweight(cur={cur_w:.1f}%,tgt={target_w:.1f}%)"
            elif deviation > 2.0:
                # Overweight vs target → suppress buys
                suppress_buys = True
                reason = f"target_overweight(cur={cur_w:.1f}%,tgt={target_w:.1f}%)"
            else:
                # Within ±2pp of target → no action needed, suppress both
                suppress_buys = True
                suppress_sells = True
                reason = f"target_on_track(cur={cur_w:.1f}%,tgt={target_w:.1f}%)"
        else:
            diff = abs(best_buy.score_total - best_sell.score_total)
            if diff >= 20.0:
                if best_buy.score_total >= best_sell.score_total:
                    suppress_sells = True
                    reason = f"score_dominant_buy(buy={best_buy.score_total:.1f},sell={best_sell.score_total:.1f})"
                else:
                    suppress_buys = True
                    reason = f"score_dominant_sell(buy={best_buy.score_total:.1f},sell={best_sell.score_total:.1f})"
            else:
                suppress_buys = True
                suppress_sells = True
                reason = f"suppressed_tie(buy={best_buy.score_total:.1f},sell={best_sell.score_total:.1f})"

        for c in group:
            suppressed = (c.signal_side == "buy" and suppress_buys) or (c.signal_side == "sell" and suppress_sells)
            if suppressed:
                c.candidate_status = "suppressed"
                c.reason_summary = c.reason_summary + f" | resolution={reason}"
            result.append(c)

    result.sort(
        key=lambda c: (
            0 if c.candidate_status not in ("suppressed", "rejected") else 1,
            -float(c.score_total),
            str(c.symbol),
        )
    )
    return result


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
    holdings_context_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None,
    min_trusted_refs: int = 0,
    apply_expert_overlay: bool = True,
    conflict_mode: str = "manual_review",
    exclude_crypto_new: bool = False,
    min_volume_amount: float = 0.0,
    min_operations: int = 0,
    liquidity_priority: bool = True,
    max_per_sector: int = 0,
    weights: Optional[Dict[str, float]] = None,
    thresholds: Optional[Dict[str, Any]] = None,
    score_version: str = "baseline_v1",
    target_weights_by_symbol: Optional[Dict[str, float]] = None,
    min_actionable_score: float = 0.0,
) -> List[OpportunityCandidate]:
    mode_n = (mode or "").strip().lower()
    weight_cfg = dict(weights or {"risk": 0.35, "value": 0.20, "momentum": 0.35, "catalyst": 0.10})
    threshold_cfg = dict(thresholds or {})
    trim_weight_pct = float(threshold_cfg.get("trim_weight_pct", 12.0) or 12.0)
    exit_weight_pct = float(threshold_cfg.get("exit_weight_pct", 15.0) or 15.0)
    sell_momentum_max = float(threshold_cfg.get("sell_momentum_max", 35.0) or 35.0)
    exit_momentum_max = float(threshold_cfg.get("exit_momentum_max", 20.0) or 20.0)
    concentration_pct_max = float(threshold_cfg.get("concentration_pct_max", 15.0) or 15.0)
    drawdown_exclusion_pct = float(threshold_cfg.get("drawdown_exclusion_pct", -25.0) or -25.0)
    rebuy_dip_threshold_pct = float(threshold_cfg.get("rebuy_dip_threshold_pct", -8.0) or -8.0)
    liquidity_floor = float(threshold_cfg.get("liquidity_floor", 40.0) or 40.0)
    sell_conflict_exit = bool(threshold_cfg.get("sell_conflict_exit", True))
    staged: List[Dict[str, Any]] = []
    for symbol, m in latest_metrics.items():
        holding_ctx = dict((holdings_context_by_symbol or {}).get(symbol) or {})
        in_port = float(holdings_value_by_symbol.get(symbol, 0.0) or 0.0) > 0.0
        families: List[Tuple[str, str]] = []
        if mode_n == "new":
            if not in_port:
                families.append(("buy", "new"))
        elif mode_n == "rebuy":
            if in_port:
                families.append(("buy", "rebuy"))
        else:  # both
            if in_port:
                families.append(("buy", "rebuy"))
                families.append(("sell", "trim"))
            else:
                families.append(("buy", "new"))
        if not families:
            continue

        s = series_by_symbol.get(symbol, [])
        ev = evidence_by_symbol.get(symbol, [])
        evs = evidence_stats(ev, as_of)
        sector_bucket = _infer_sector_bucket(symbol, ev)
        is_crypto = bool(_is_crypto_symbol_hint(symbol) or sector_bucket == "crypto")
        dd = drawdown_pct(s, as_of)
        v_score_raw = value_score(s, as_of)
        m_score_raw = momentum_score(s, as_of)
        c_score_raw = float(evs["catalyst_score"])
        expert_score = float(evs.get("expert_signal_score") or 50.0)
        c_final_raw = 0.6 * c_score_raw + 0.4 * expert_score if apply_expert_overlay else c_score_raw
        trusted_refs_count = int(evs.get("fresh_trusted_refs_count") or evs.get("trusted_refs_count") or 0)
        consensus_state = str(evs.get("consensus_state") or "insufficient")
        decision_gate = "manual_review" if (consensus_state == "conflict" and conflict_mode == "manual_review") else "auto"
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
        volume_amount = _safe_float(m.get("volume_amount"))
        liq_score = _liquidity_score(
            spread_pct=spread,
            operations_count=ops,
            volume_amount=volume_amount,
        )

        if bid is not None and ask is not None and bid > 0 and ask > 0:
            spread = spread if spread is not None else compute_spread_pct(bid, ask)
            if spread is not None and spread > 2.5:
                hard_ok = False
                flags.append("LIQUIDITY_SPREAD")
            elif spread is not None and spread > 1.5:
                risk_penalty += 10.0
        if ops is not None:
            if ops <= 0:
                hard_ok = False
                flags.append("LIQUIDITY_NO_OPS")
            elif ops < 5:
                risk_penalty += 10.0
        else:
            flags.append("LIQUIDITY_UNKNOWN")
            risk_penalty += 20.0

        if int(min_operations) > 0 and ops is not None and ops < float(min_operations):
            hard_ok = False
            flags.append("LIQUIDITY_LOW_OPS")
        if float(min_volume_amount) > 0 and volume_amount is not None and volume_amount < float(min_volume_amount):
            hard_ok = False
            flags.append("LIQUIDITY_LOW_VOLUME")
        if bool(liquidity_priority):
            if liq_score < 40.0:
                risk_penalty += 15.0
            elif liq_score < 55.0:
                risk_penalty += 5.0

        target_w_struct = float((target_weights_by_symbol or {}).get(symbol, 0.0) or 0.0)
        effective_conc_max = max(concentration_pct_max, target_w_struct + 5.0) if target_w_struct > 0.0 else concentration_pct_max
        if cur_w >= effective_conc_max:
            hard_ok = False
            flags.append("CONCENTRATION_MAX")
        elif target_w_struct > 0.0 and cur_w > target_w_struct + 3.0:
            risk_penalty += 10.0
            flags.append("OVERWEIGHT_TARGET")
        elif cur_w > max(0.0, concentration_pct_max - 3.0):
            risk_penalty += 15.0

        if dd is not None and dd < drawdown_exclusion_pct and not bool(evs["has_recent_catalyst"]):
            hard_ok = False
            flags.append("DRAWDOWN_EXTREME")
        elif dd is not None and dd < -20.0:
            risk_penalty += 10.0

        if bool(evs["unresolved_conflict"]):
            flags.append("EVIDENCE_CONFLICT")
            risk_penalty += 10.0
        if int(min_trusted_refs) > 0 and trusted_refs_count < int(min_trusted_refs):
            flags.append("EVIDENCE_INSUFFICIENT")

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

        gain_pct = float(holding_ctx.get("gain_pct") or 0.0)
        holding_age_days = _safe_int(holding_ctx.get("age_days")) or 0
        concentration_score = clamp(cur_w / max(1.0, concentration_pct_max) * 100.0, 0.0, 100.0)
        evidence_quality_score = clamp(
            trusted_refs_count * 20.0
            + (15.0 if bool(evs.get("has_thesis")) else -10.0)
            - (20.0 if bool(evs.get("unresolved_conflict")) else 0.0),
            0.0,
            100.0,
        )
        thesis_deterioration_score = clamp(
            (100.0 - m_score_raw) * 0.55
            + (25.0 if bool(evs.get("unresolved_conflict")) else 0.0)
            + (15.0 if not bool(evs.get("has_recent_catalyst")) else 0.0)
            + (10.0 if dd is not None and dd < -12.0 else 0.0),
            0.0,
            100.0,
        )

        for signal_side, signal_family in families:
            local_flags = list(flags)
            local_hard_ok = hard_ok
            current_entry_low = entry_low
            current_entry_high = entry_high

            if bool(exclude_crypto_new) and signal_side == "buy" and signal_family == "new" and is_crypto:
                local_hard_ok = False
                local_flags.append("CRYPTO_EXCLUDED")

            if signal_side == "buy" and signal_family == "rebuy":
                dip_ok = dd is not None and dd <= rebuy_dip_threshold_pct
                thesis_ok = bool(evs["has_thesis"])
                if not dip_ok or not thesis_ok:
                    continue

            sell_trigger = False
            if signal_side == "sell":
                if not in_port:
                    continue
                current_entry_low = None
                current_entry_high = None
                trim_trigger = bool(cur_w >= trim_weight_pct or m_score_raw <= sell_momentum_max or evidence_quality_score < 40.0)
                exit_trigger = bool(
                    cur_w >= exit_weight_pct
                    or m_score_raw <= exit_momentum_max
                    or (sell_conflict_exit and bool(evs.get("unresolved_conflict")))
                    or (dd is not None and dd < -20.0)
                    or liq_score < liquidity_floor
                )
                if exit_trigger:
                    signal_family = "exit"
                    sell_trigger = True
                    local_flags.append("SELL_EXIT")
                elif trim_trigger:
                    signal_family = "trim"
                    sell_trigger = True
                    local_flags.append("SELL_TRIM")
                if not sell_trigger:
                    continue
                local_hard_ok = True
                if liq_score < max(10.0, liquidity_floor * 0.6):
                    local_hard_ok = False
                    local_flags.append("SELL_LIQUIDITY_BLOCK")

            risk_score_raw = clamp(100.0 - risk_penalty, 0.0, 100.0)
            score_value_raw = float(v_score_raw)
            score_momentum_raw = float(m_score_raw)
            score_catalyst_raw = float(c_final_raw)
            if signal_side == "sell":
                risk_score_raw = clamp(concentration_score * 0.4 + thesis_deterioration_score * 0.4 + (100.0 - liq_score) * 0.2, 0.0, 100.0)
                score_value_raw = clamp(50.0 + gain_pct * 2.0 + concentration_score * 0.15, 0.0, 100.0)
                score_momentum_raw = clamp(100.0 - m_score_raw, 0.0, 100.0)
                score_catalyst_raw = clamp(thesis_deterioration_score, 0.0, 100.0)

            staged.append(
                {
                    "symbol": symbol,
                    "candidate_type": signal_family,
                    "signal_side": signal_side,
                    "signal_family": signal_family,
                    "score_version": score_version,
                    "entry_low": current_entry_low,
                    "entry_high": current_entry_high,
                    "flags": list(dict.fromkeys(local_flags)),
                    "filters_passed": 1 if local_hard_ok else 0,
                    "current_weight_pct": cur_w,
                    "expert_signal_score": expert_score,
                    "trusted_refs_count": trusted_refs_count,
                    "consensus_state": consensus_state,
                    "decision_gate": decision_gate,
                    "evidence_summary_json": str(evs.get("evidence_summary_json") or "{}"),
                    "liquidity_score": float(liq_score),
                    "sector_bucket": sector_bucket,
                    "is_crypto_proxy": 1 if is_crypto else 0,
                    "score_risk_raw": float(risk_score_raw),
                    "score_value_raw": float(score_value_raw),
                    "score_momentum_raw": float(score_momentum_raw),
                    "score_catalyst_raw": float(score_catalyst_raw),
                    "score_catalyst_base_raw": float(c_score_raw),
                    "score_features_json": json.dumps(
                        {
                            "risk": float(risk_score_raw),
                            "value": float(score_value_raw),
                            "momentum": float(score_momentum_raw),
                            "catalyst": float(score_catalyst_raw),
                            "liquidity": float(liq_score),
                            "concentration": float(concentration_score),
                            "evidence_quality": float(evidence_quality_score),
                            "thesis_deterioration": float(thesis_deterioration_score),
                        },
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                    "holding_context_json": json.dumps(
                        {
                            "current_weight_pct": cur_w,
                            "gain_pct": gain_pct,
                            "gain_amount": float(holding_ctx.get("gain_amount") or 0.0),
                            "age_days": int(holding_age_days),
                            "quantity": float(holding_ctx.get("quantity") or 0.0),
                            "concentration_pct": cur_w,
                        },
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                    "dd": dd,
                }
            )

    if not staged:
        return []

    risk_values = [float(r["score_risk_raw"]) for r in staged]
    value_values = [float(r["score_value_raw"]) for r in staged]
    momentum_values = [float(r["score_momentum_raw"]) for r in staged]
    catalyst_values = [float(r["score_catalyst_raw"]) for r in staged]

    out: List[OpportunityCandidate] = []
    for r in staged:
        risk_score = _percentile_score(risk_values, float(r["score_risk_raw"]))
        v_score = _percentile_score(value_values, float(r["score_value_raw"]))
        m_score = _percentile_score(momentum_values, float(r["score_momentum_raw"]))
        c_final = _percentile_score(catalyst_values, float(r["score_catalyst_raw"]))
        total = (
            float(weight_cfg.get("risk", 0.35)) * risk_score
            + float(weight_cfg.get("value", 0.20)) * v_score
            + float(weight_cfg.get("momentum", 0.35)) * m_score
            + float(weight_cfg.get("catalyst", 0.10)) * c_final
        )

        flags = list(r.get("flags") or [])
        candidate_status = "operable"
        if int(r.get("filters_passed") or 0) != 1:
            candidate_status = "rejected"
        elif str(r.get("decision_gate") or "").strip().lower() == "manual_review":
            candidate_status = "manual_review"
        elif str(r.get("signal_side") or "buy") == "buy" and (
            "EVIDENCE_INSUFFICIENT" in flags or str(r.get("consensus_state") or "") == "insufficient"
        ):
            candidate_status = "watchlist"
        elif float(min_actionable_score) > 0.0 and total < float(min_actionable_score):
            candidate_status = "watchlist"

        dd = r.get("dd")
        reason = (
            f"{r.get('candidate_type')} | dd20={float(dd):.2f}% "
            if dd is not None
            else f"{r.get('candidate_type')} | dd20=NA "
        )
        reason += (
            f"| risk={risk_score:.1f} value={v_score:.1f} momentum={m_score:.1f} "
            f"catalyst={c_final:.1f} catalyst_base={float(r.get('score_catalyst_base_raw') or 0.0):.1f} "
            f"expert={float(r.get('expert_signal_score') or 0.0):.1f}"
        )
        reason += (
            f" | refs={int(r.get('trusted_refs_count') or 0)} consensus={r.get('consensus_state')} "
            f"gate={r.get('decision_gate')} status={candidate_status} side={r.get('signal_side')} liq={float(r.get('liquidity_score') or 0.0):.1f} "
            f"sector={r.get('sector_bucket')}"
        )
        if flags:
            reason += f" | flags={','.join(flags)}"

        out.append(
            OpportunityCandidate(
                symbol=str(r.get("symbol") or ""),
                candidate_type=str(r.get("candidate_type") or "new"),
                signal_side=str(r.get("signal_side") or "buy"),
                signal_family=str(r.get("signal_family") or r.get("candidate_type") or "new"),
                score_version=str(r.get("score_version") or score_version),
                score_total=float(total),
                score_risk=float(risk_score),
                score_value=float(v_score),
                score_momentum=float(m_score),
                score_catalyst=float(c_final),
                entry_low=r.get("entry_low"),
                entry_high=r.get("entry_high"),
                suggested_weight_pct=None,
                suggested_amount_ars=None,
                reason_summary=reason,
                risk_flags_json=str(flags).replace("'", '"'),
                filters_passed=int(r.get("filters_passed") or 0),
                current_weight_pct=float(r.get("current_weight_pct") or 0.0),
                expert_signal_score=float(r.get("expert_signal_score") or 0.0),
                trusted_refs_count=int(r.get("trusted_refs_count") or 0),
                consensus_state=str(r.get("consensus_state") or "insufficient"),
                decision_gate=str(r.get("decision_gate") or "auto"),
                candidate_status=candidate_status,
                evidence_summary_json=str(r.get("evidence_summary_json") or "{}"),
                liquidity_score=float(r.get("liquidity_score") or 0.0),
                sector_bucket=str(r.get("sector_bucket") or "unknown"),
                is_crypto_proxy=int(r.get("is_crypto_proxy") or 0),
                holding_context_json=str(r.get("holding_context_json") or "{}"),
                score_features_json=str(r.get("score_features_json") or "{}"),
            )
        )

    out.sort(
        key=lambda c: (
            -float(c.score_total),
            -(float(c.liquidity_score) if liquidity_priority else 0.0),
            -float(c.trusted_refs_count),
            str(c.symbol),
        )
    )

    operable = [c for c in out if c.candidate_status == "operable" and c.signal_side == "buy" and c.score_total >= 50.0]
    if not operable:
        operable = [c for c in out if c.candidate_status == "operable" and c.signal_side == "buy"]
    selected: List[OpportunityCandidate] = []
    sector_cap = int(max_per_sector)
    sector_counts: Dict[str, int] = {}
    for c in operable:
        bucket = str(c.sector_bucket or "unknown").strip().lower()
        if sector_cap > 0 and bucket and bucket != "unknown":
            cur = int(sector_counts.get(bucket, 0))
            if cur >= sector_cap:
                continue
            sector_counts[bucket] = cur + 1
        selected.append(c)
        if len(selected) >= int(top_n):
            break
    if len(selected) < int(top_n):
        for c in operable:
            if c in selected:
                continue
            selected.append(c)
            if len(selected) >= int(top_n):
                break
    selected_symbols = {str(c.symbol) for c in selected}
    status_order = {
        "operable_selected_buy": 0,
        "operable_sell": 1,
        "operable": 2,
        "manual_review": 3,
        "watchlist": 4,
        "rejected": 5,
    }

    def _rank_bucket(candidate: OpportunityCandidate) -> int:
        if candidate.candidate_status == "operable" and candidate.signal_side == "buy" and candidate.symbol in selected_symbols:
            return status_order["operable_selected_buy"]
        if candidate.candidate_status == "operable" and candidate.signal_side == "sell":
            return status_order["operable_sell"]
        return status_order.get(str(candidate.candidate_status or "watchlist"), 6)

    if not selected:
        out.sort(
            key=lambda c: (
                _rank_bucket(c),
                -float(c.score_total),
                -(float(c.liquidity_score) if liquidity_priority else 0.0),
                -float(c.trusted_refs_count),
                str(c.symbol),
            )
        )
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
        if c.signal_side == "buy" and c.candidate_type == "new":
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

    out.sort(
        key=lambda c: (
            _rank_bucket(c),
            -float(c.score_total),
            -(float(c.liquidity_score) if liquidity_priority else 0.0),
            -float(c.trusted_refs_count),
            str(c.symbol),
        )
    )

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
    run_metrics = _parse_notes_json(run.get("run_metrics_json"))
    operable = [
        r for r in rows if str(r.get("candidate_status") or "").strip().lower() == "operable"
    ][:top_n]
    watchlist = [
        r for r in rows if str(r.get("candidate_status") or "").strip().lower() in ("watchlist", "manual_review")
    ][:top_n]
    rejected = [
        r for r in rows if str(r.get("candidate_status") or "").strip().lower() == "rejected"
    ][:top_n]
    lines: List[str] = []
    lines.append("# Oportunidades de Portafolio (Semanal)")
    lines.append("")
    lines.append(f"- `created_at_utc`: {created}")
    lines.append(f"- `as_of`: {as_of}")
    lines.append(f"- `mode`: {mode}")
    lines.append(f"- `budget_ars`: {budget:,.2f}".replace(",", "."))
    if run.get("pipeline_warnings_json"):
        lines.append(f"- `pipeline_warnings_json`: {run.get('pipeline_warnings_json')}")
    if run_metrics:
        lines.append(
            "- `run_metrics`: dispersion={disp:.2f} | operable_ratio={opr:.1%} | watchlist_ratio={wr:.1%} | fresh_evidence_ratio={fr:.1%}".format(
                disp=float(run_metrics.get("score_dispersion") or 0.0),
                opr=float(run_metrics.get("operable_ratio") or 0.0),
                wr=float(run_metrics.get("watchlist_ratio") or 0.0),
                fr=float(run_metrics.get("fresh_evidence_ratio") or 0.0),
            )
        )
    lines.append("")
    lines.append("## Operables")
    if not operable:
        lines.append("- Sin candidatos operables para este run.")
    else:
        lines.append("")
        lines.append("| Symbol | Tipo | Estado | Score | Entry Low | Entry High | Weight % | Amount ARS | refs frescas | consenso |")
        lines.append("|---|---:|---|---:|---:|---:|---:|---:|---:|---|")
        for r in operable:
            lines.append(
                "| {sym} | {typ} | {status} | {sc:.2f} | {el} | {eh} | {w} | {amt} | {refs} | {consensus} |".format(
                    sym=r.get("symbol"),
                    typ=r.get("candidate_type"),
                    status=r.get("candidate_status") or "operable",
                    sc=float(r.get("score_total") or 0.0),
                    el=("-" if r.get("entry_low") is None else f"{float(r.get('entry_low')):.2f}"),
                    eh=("-" if r.get("entry_high") is None else f"{float(r.get('entry_high')):.2f}"),
                    w=("-" if r.get("suggested_weight_pct") is None else f"{float(r.get('suggested_weight_pct')):.2f}"),
                    amt=("-" if r.get("suggested_amount_ars") is None else f"{float(r.get('suggested_amount_ars')):.2f}"),
                    refs=int(r.get("trusted_refs_count") or 0),
                    consensus=str(r.get("consensus_state") or "-"),
                )
            )

    lines.append("")
    lines.append("## Watchlist por falta de evidencia o revisión manual")
    if not watchlist:
        lines.append("- Sin candidatos en watchlist.")
    else:
        for r in watchlist:
            lines.append(
                "- **{sym}**: estado=`{status}` consensus=`{consensus}` refs={refs} motivo={reason}".format(
                    sym=r.get("symbol"),
                    status=r.get("candidate_status") or "watchlist",
                    consensus=r.get("consensus_state") or "insufficient",
                    refs=int(r.get("trusted_refs_count") or 0),
                    reason=r.get("reason_summary") or "-",
                )
            )
    lines.append("")
    lines.append("## Rechazados por riesgo/liquidez")
    if not rejected:
        lines.append("- Sin rechazados destacados.")
    else:
        for r in rejected:
            lines.append(f"- **{r.get('symbol')}**: {r.get('reason_summary')}")
    lines.append("")
    lines.append("## Razones y riesgos")
    for r in operable:
        lines.append(f"- **{r.get('symbol')}**: {r.get('reason_summary')}")
    lines.append("")
    lines.append("Nota: esto no ejecuta ordenes reales; usar simulacion y confirmacion explicita.")
    return "\n".join(lines) + "\n"
