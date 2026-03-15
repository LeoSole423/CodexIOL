"""Engine signal accuracy analysis.

Evaluates how often each engine's predictions proved correct by comparing
past signals against observed market returns in market_symbol_snapshots.

Accuracy definitions:
  Regime:     bull → expected positive average return; bear/crisis → negative return
  Macro:      AR stress >65 → CEDEARs (market='cedear') should outperform BCBA locals
  SmartMoney: accumulate conviction>=70 → symbol price up in lookahead window
  Strategy:   BUY actions in engine_strategy_runs → positive forward return
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from typing import Any, Dict, List, Optional


_DEFAULT_LOOKAHEAD = 20  # trading days ≈ 1 month


# ── Market return helpers ────────────────────────────────────────────────────

def _market_return(conn: sqlite3.Connection, date_from: str, date_to: str) -> Optional[float]:
    """Average return (%) across all symbols available on both dates.

    Returns None if fewer than 5 common symbols found.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.symbol,
               (b.last_price - a.last_price) / a.last_price * 100 AS ret
        FROM market_symbol_snapshots a
        JOIN market_symbol_snapshots b
          ON a.symbol = b.symbol
        WHERE a.snapshot_date = (
                SELECT MAX(snapshot_date) FROM market_symbol_snapshots
                WHERE snapshot_date <= ?
              )
          AND b.snapshot_date = (
                SELECT MAX(snapshot_date) FROM market_symbol_snapshots
                WHERE snapshot_date <= ?
              )
          AND a.last_price > 0
          AND b.last_price > 0
        """,
        (date_from, date_to),
    )
    rows = cur.fetchall()
    if len(rows) < 5:
        return None
    return sum(r[1] for r in rows) / len(rows)


def _market_return_by_segment(
    conn: sqlite3.Connection, date_from: str, date_to: str, segment: str
) -> Optional[float]:
    """Average return for a specific market segment ('cedear' or 'bcba')."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.symbol,
               (b.last_price - a.last_price) / a.last_price * 100 AS ret
        FROM market_symbol_snapshots a
        JOIN market_symbol_snapshots b
          ON a.symbol = b.symbol
        WHERE a.market = ? AND b.market = ?
          AND a.snapshot_date = (
                SELECT MAX(snapshot_date) FROM market_symbol_snapshots
                WHERE snapshot_date <= ?
              )
          AND b.snapshot_date = (
                SELECT MAX(snapshot_date) FROM market_symbol_snapshots
                WHERE snapshot_date <= ?
              )
          AND a.last_price > 0
          AND b.last_price > 0
        """,
        (segment, segment, date_from, date_to),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    return sum(r[1] for r in rows) / len(rows)


def _symbol_return(
    conn: sqlite3.Connection, symbol: str, date_from: str, date_to: str
) -> Optional[float]:
    """Forward return (%) for a single symbol."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.last_price, b.last_price
        FROM market_symbol_snapshots a
        JOIN market_symbol_snapshots b ON a.symbol = b.symbol
        WHERE a.symbol = ?
          AND a.snapshot_date = (
                SELECT MAX(snapshot_date) FROM market_symbol_snapshots
                WHERE symbol = ? AND snapshot_date <= ?
              )
          AND b.snapshot_date = (
                SELECT MAX(snapshot_date) FROM market_symbol_snapshots
                WHERE symbol = ? AND snapshot_date <= ?
              )
          AND a.last_price > 0 AND b.last_price > 0
        LIMIT 1
        """,
        (symbol, symbol, date_from, symbol, date_to),
    )
    row = cur.fetchone()
    if not row:
        return None
    return (row[1] - row[0]) / row[0] * 100


def _add_days(date_str: str, days: int) -> str:
    return (date.fromisoformat(date_str) + timedelta(days=days)).isoformat()


def _outcome_already_recorded(conn: sqlite3.Connection, engine_name: str, as_of: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM engine_signal_outcomes WHERE engine_name=? AND as_of=? LIMIT 1",
        (engine_name, as_of),
    ).fetchone()
    return row is not None


def _insert_outcome(
    conn: sqlite3.Connection,
    engine_name: str,
    as_of: str,
    signal_summary: str,
    lookahead_days: int,
    outcome_date: Optional[str],
    outcome_return_pct: Optional[float],
    signal_correct: Optional[int],
    notes: Optional[str] = None,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO engine_signal_outcomes
            (engine_name, as_of, signal_summary, lookahead_days,
             outcome_date, outcome_return_pct, signal_correct, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (engine_name, as_of, signal_summary, lookahead_days,
         outcome_date, outcome_return_pct, signal_correct, notes),
    )


# ── Per-engine outcome computation ──────────────────────────────────────────

def _compute_regime_outcomes(conn: sqlite3.Connection, lookahead_days: int) -> int:
    """Evaluate cached regime signals against average market return."""
    today = date.today().isoformat()
    cutoff = _add_days(today, -lookahead_days)  # only signals old enough to have outcomes

    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of, regime, confidence, regime_score
        FROM engine_regime_snapshots
        WHERE as_of <= ?
        ORDER BY as_of
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    inserted = 0

    for as_of, regime, confidence, regime_score in rows:
        if _outcome_already_recorded(conn, "regime", as_of):
            continue

        outcome_date = _add_days(as_of, lookahead_days)
        ret = _market_return(conn, as_of, outcome_date)

        signal_correct = None
        if ret is not None:
            if regime == "bull":
                signal_correct = 1 if ret > 0 else 0
            elif regime in ("bear", "crisis"):
                signal_correct = 1 if ret < 0 else 0
            # "sideways" — no directional bet, skip correctness check

        _insert_outcome(
            conn, "regime", as_of,
            f"{regime} conf={confidence:.2f} score={regime_score:.0f}",
            lookahead_days, outcome_date, ret, signal_correct,
        )
        inserted += 1

    conn.commit()
    return inserted


def _compute_macro_outcomes(conn: sqlite3.Connection, lookahead_days: int) -> int:
    """Evaluate macro signals: AR stress >65 → CEDEARs should outperform BCBA locals."""
    today = date.today().isoformat()
    cutoff = _add_days(today, -lookahead_days)

    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of, argentina_macro_stress, global_risk_on
        FROM engine_macro_snapshots
        WHERE as_of <= ?
        ORDER BY as_of
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    inserted = 0

    for as_of, ar_stress, global_risk_on in rows:
        if _outcome_already_recorded(conn, "macro", as_of):
            continue

        outcome_date = _add_days(as_of, lookahead_days)
        cedear_ret = _market_return_by_segment(conn, as_of, outcome_date, "cedear")
        bcba_ret = _market_return_by_segment(conn, as_of, outcome_date, "bcba")

        signal_correct = None
        ret = None
        if cedear_ret is not None and bcba_ret is not None:
            # When stress >65, preference was CEDEARs — correct if CEDEARs > BCBA
            if ar_stress > 65:
                signal_correct = 1 if cedear_ret > bcba_ret else 0
                ret = cedear_ret - bcba_ret  # spread
            else:
                # Low stress: BCBA preferred — correct if BCBA > CEDEARs
                signal_correct = 1 if bcba_ret >= cedear_ret else 0
                ret = bcba_ret - cedear_ret
        elif cedear_ret is not None:
            ret = cedear_ret
        elif bcba_ret is not None:
            ret = bcba_ret

        _insert_outcome(
            conn, "macro", as_of,
            f"ar_stress={ar_stress:.1f} global_risk_on={global_risk_on:.1f}",
            lookahead_days, outcome_date, ret, signal_correct,
        )
        inserted += 1

    conn.commit()
    return inserted


def _compute_smart_money_outcomes(conn: sqlite3.Connection, lookahead_days: int) -> int:
    """Evaluate SmartMoney: accumulate conviction>=70 → symbol price up."""
    today = date.today().isoformat()
    cutoff = _add_days(today, -lookahead_days)

    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of, symbol, net_institutional_direction, conviction_score
        FROM engine_smart_money_snapshots
        WHERE as_of <= ? AND conviction_score >= 70
        ORDER BY as_of, symbol
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    inserted = 0

    for as_of, symbol, direction, conviction in rows:
        engine_key = f"smart_money:{symbol}"
        # Use symbol-level unique key via notes field lookup
        if conn.execute(
            "SELECT 1 FROM engine_signal_outcomes WHERE engine_name='smart_money' AND as_of=? AND signal_summary LIKE ? LIMIT 1",
            (as_of, f"%{symbol}%"),
        ).fetchone():
            continue

        outcome_date = _add_days(as_of, lookahead_days)
        ret = _symbol_return(conn, symbol, as_of, outcome_date)

        signal_correct = None
        if ret is not None:
            if direction == "accumulate":
                signal_correct = 1 if ret > 0 else 0
            elif direction == "distribute":
                signal_correct = 1 if ret < 0 else 0

        _insert_outcome(
            conn, "smart_money", as_of,
            f"{symbol} {direction} conv={conviction:.0f}",
            lookahead_days, outcome_date, ret, signal_correct,
        )
        inserted += 1

    conn.commit()
    return inserted


def _compute_strategy_outcomes(conn: sqlite3.Connection, lookahead_days: int) -> int:
    """Evaluate strategy BUY actions: were they followed by positive symbol returns?"""
    import json as _json

    today = date.today().isoformat()
    cutoff = _add_days(today, -lookahead_days)

    cur = conn.cursor()
    cur.execute(
        """
        SELECT as_of, actions_json
        FROM engine_strategy_runs
        WHERE as_of <= ?
        ORDER BY as_of
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    inserted = 0

    for as_of, actions_json in rows:
        if _outcome_already_recorded(conn, "strategy", as_of):
            continue

        try:
            actions = _json.loads(actions_json or "[]")
        except Exception:
            continue

        buy_actions = [a for a in actions if a.get("action") == "buy"]
        if not buy_actions:
            continue

        outcome_date = _add_days(as_of, lookahead_days)
        returns = []
        for action in buy_actions:
            symbol = action.get("symbol", "")
            ret = _symbol_return(conn, symbol, as_of, outcome_date)
            if ret is not None:
                returns.append(ret)

        avg_ret = sum(returns) / len(returns) if returns else None
        signal_correct = (1 if avg_ret > 0 else 0) if avg_ret is not None else None

        _insert_outcome(
            conn, "strategy", as_of,
            f"{len(buy_actions)} buys: {', '.join(a.get('symbol','') for a in buy_actions[:5])}",
            lookahead_days, outcome_date, avg_ret, signal_correct,
        )
        inserted += 1

    conn.commit()
    return inserted


# ── Public API ───────────────────────────────────────────────────────────────

def compute_signal_outcomes(
    conn: sqlite3.Connection,
    lookahead_days: int = _DEFAULT_LOOKAHEAD,
) -> Dict[str, int]:
    """Evaluate all past engine signals and insert outcomes. Returns counts per engine."""
    return {
        "regime": _compute_regime_outcomes(conn, lookahead_days),
        "macro": _compute_macro_outcomes(conn, lookahead_days),
        "smart_money": _compute_smart_money_outcomes(conn, lookahead_days),
        "strategy": _compute_strategy_outcomes(conn, lookahead_days),
    }


def get_accuracy_report(
    conn: sqlite3.Connection,
    days: int = 90,
    engine: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return accuracy summary for each engine over the last N days.

    Returns list of dicts:
      { engine, hit_rate_pct, hits, evaluated, pending, total, last_eval_date }
    """
    cutoff = _add_days(date.today().isoformat(), -days)
    engine_filter = f"AND engine_name = '{engine}'" if engine else ""

    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT engine_name,
               COUNT(*) AS total,
               SUM(CASE WHEN signal_correct = 1 THEN 1 ELSE 0 END) AS hits,
               SUM(CASE WHEN signal_correct IS NULL THEN 1 ELSE 0 END) AS pending,
               MAX(outcome_date) AS last_eval
        FROM engine_signal_outcomes
        WHERE as_of >= ? {engine_filter}
        GROUP BY engine_name
        ORDER BY engine_name
        """,
        (cutoff,),
    )
    rows = cur.fetchall()
    results = []
    for engine_name, total, hits, pending, last_eval in rows:
        evaluated = total - (pending or 0)
        hit_rate = round(hits / evaluated * 100, 1) if evaluated > 0 else None
        results.append({
            "engine": engine_name,
            "hit_rate_pct": hit_rate,
            "hits": hits or 0,
            "evaluated": evaluated,
            "pending": pending or 0,
            "total": total,
            "last_eval_date": last_eval,
        })

    # Include engines with no data yet
    all_engines = ["regime", "macro", "smart_money", "strategy"]
    found = {r["engine"] for r in results}
    for eng in all_engines:
        if eng not in found and (engine is None or engine == eng):
            results.append({
                "engine": eng,
                "hit_rate_pct": None,
                "hits": 0,
                "evaluated": 0,
                "pending": 0,
                "total": 0,
                "last_eval_date": None,
            })

    return sorted(results, key=lambda r: r["engine"])
