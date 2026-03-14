import json
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import typer

CONFIDENCE_LEVELS = {"low", "medium", "high"}
OPP_MODES = {"new", "rebuy", "both"}
OPP_UNIVERSES = {"bcba_cedears"}
SOURCE_POLICIES = {"strict_official_reuters"}
CONFLICT_MODES = {"manual_review"}
VARIANT_SELECTORS = {"active", "challenger", "both"}


def normalize_enum(value: str, label: str, allowed: set) -> str:
    v = (value or "").strip().lower()
    if v not in allowed:
        allowed_txt = "|".join(sorted(allowed))
        raise typer.BadParameter(f"{label} must be {allowed_txt}")
    return v


def latest_snapshot_date(conn) -> Optional[str]:
    row = conn.execute("SELECT snapshot_date FROM portfolio_snapshots ORDER BY snapshot_date DESC LIMIT 1").fetchone()
    return str(row["snapshot_date"]) if row and row["snapshot_date"] else None


def load_holdings_map_from_context(ctx_payload: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for r in ((ctx_payload or {}).get("assets") or {}).get("rows") or []:
        sym = str(r.get("symbol") or "").strip()
        if not sym:
            continue
        out[sym] = float(r.get("total_value") or 0.0)
    return out


def load_holdings_context_from_db(conn, as_of: str) -> Dict[str, Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT snapshot_date, symbol, quantity, last_price, ppc, total_value, gain_pct, gain_amount
        FROM portfolio_assets
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date) FROM portfolio_assets WHERE snapshot_date <= ?
        )
        """,
        (str(as_of),),
    ).fetchall()
    first_seen_rows = conn.execute(
        """
        SELECT symbol, MIN(snapshot_date) AS first_seen
        FROM portfolio_assets
        WHERE snapshot_date <= ?
          AND COALESCE(total_value, 0) > 0
        GROUP BY symbol
        """,
        (str(as_of),),
    ).fetchall()
    first_seen = {str(r["symbol"] or ""): str(r["first_seen"] or "") for r in first_seen_rows}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        symbol = str(row["symbol"] or "").strip().upper()
        if not symbol:
            continue
        age_days = 0
        try:
            first = first_seen.get(symbol)
            if first:
                age_days = max(0, (date.fromisoformat(str(as_of)) - date.fromisoformat(first)).days)
        except Exception:
            age_days = 0
        out[symbol] = {
            "quantity": float(row["quantity"] or 0.0),
            "last_price": float(row["last_price"] or 0.0),
            "ppc": float(row["ppc"] or 0.0) if row["ppc"] is not None else None,
            "total_value": float(row["total_value"] or 0.0),
            "gain_pct": float(row["gain_pct"] or 0.0) if row["gain_pct"] is not None else 0.0,
            "gain_amount": float(row["gain_amount"] or 0.0) if row["gain_amount"] is not None else 0.0,
            "age_days": int(age_days),
        }
    return out


def load_market_snapshot_rows(conn, as_of: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT snapshot_date, symbol, market, last_price, bid, ask, spread_pct, daily_var_pct, operations_count, volume_amount, source
        FROM market_symbol_snapshots
        WHERE snapshot_date <= ?
        ORDER BY snapshot_date ASC
        """,
        (as_of,),
    ).fetchall()
    return [dict(r) for r in rows]


def load_evidence_rows_grouped(conn, as_of: str, lookback_days: int = 60) -> Dict[str, List[Dict[str, Any]]]:
    d = date.fromisoformat(as_of)
    cutoff = (d - timedelta(days=int(lookback_days))).isoformat() + "T00:00:00Z"
    rows = conn.execute(
        """
        SELECT symbol, query, source_name, source_url, published_date, retrieved_at_utc, claim,
               confidence, date_confidence, notes, conflict_key
        FROM advisor_evidence
        WHERE retrieved_at_utc >= ?
        ORDER BY retrieved_at_utc DESC
        """,
        (cutoff,),
    ).fetchall()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        sym = str(r["symbol"])
        out.setdefault(sym, []).append(dict(r))
    return out


def pick_symbols_for_web_link(
    holdings_map: Dict[str, float],
    prelim_candidates: List[Dict[str, Any]],
    top_k: int,
) -> List[str]:
    chosen: List[str] = []
    for s in sorted(holdings_map.keys()):
        if s and s not in chosen:
            chosen.append(s)
    operable = [r for r in prelim_candidates if int(r.get("filters_passed") or 0) == 1]
    operable.sort(
        key=lambda r: (
            -float(r.get("score_total") or 0.0),
            -float(r.get("liquidity_score") or 0.0),
            -float(r.get("trusted_refs_count") or 0.0),
            str(r.get("symbol") or ""),
        )
    )
    for r in operable[: int(top_k)]:
        sym = str(r.get("symbol") or "").strip().upper()
        if sym and sym not in chosen:
            chosen.append(sym)
    return chosen


def store_evidence_rows(
    conn,
    rows: List[Dict[str, Any]],
    *,
    confidence_levels: set,
    utc_now_iso,
) -> int:
    inserted = 0
    for r in rows:
        sym = str(r.get("symbol") or "").strip().upper()
        query_v = str(r.get("query") or "").strip()
        source_name_v = str(r.get("source_name") or "").strip()
        source_url_v = str(r.get("source_url") or "").strip()
        claim_v = str(r.get("claim") or "").strip()
        conf_v = str(r.get("confidence") or "").strip().lower()
        date_conf_v = str(r.get("date_confidence") or "").strip().lower()
        if (
            not sym
            or not query_v
            or not source_name_v
            or not source_url_v
            or not claim_v
            or conf_v not in confidence_levels
            or date_conf_v not in confidence_levels
        ):
            continue
        notes_v = r.get("notes")
        if isinstance(notes_v, (dict, list)):
            notes_v = json.dumps(notes_v, ensure_ascii=True, sort_keys=True)
        conn.execute(
            """
            INSERT INTO advisor_evidence (
                created_at, symbol, query, source_name, source_url, published_date, retrieved_at_utc,
                claim, confidence, date_confidence, notes, conflict_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(r.get("retrieved_at_utc") or utc_now_iso()),
                sym,
                query_v,
                source_name_v,
                source_url_v,
                r.get("published_date"),
                str(r.get("retrieved_at_utc") or utc_now_iso()),
                claim_v,
                conf_v,
                date_conf_v,
                notes_v,
                r.get("conflict_key"),
            ),
        )
        inserted += 1
    return inserted
