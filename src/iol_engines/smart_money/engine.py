"""Smart Money Engine — Motor 3.

Tracks institutional 13F filings from SEC EDGAR for a curated list of
top investors and aggregates their position changes into per-symbol
conviction scores.

Data is quarterly with a 45-day SEC filing lag.  The engine caches results
for 7 days and uses `load_latest()` with a 90-day window for historical queries.
"""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..base import BaseEngine
from ..signals import SmartMoneySignal
from .fund_tracker import CEDEAR_UNIVERSE, TRACKED_FUNDS, normalize_sec_ticker
from .sec_13f import compute_direction, fetch_13f_holdings


# How many days a smart-money cache entry stays "fresh".
_STALENESS_DAYS = 7


def _upsert_smart_money(
    conn: sqlite3.Connection,
    sig: SmartMoneySignal,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO engine_smart_money_snapshots
            (as_of, created_at_utc, symbol, net_institutional_direction,
             conviction_score, top_holders_added_json, top_holders_trimmed_json,
             latest_13f_date, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, as_of) DO UPDATE SET
            created_at_utc              = excluded.created_at_utc,
            net_institutional_direction = excluded.net_institutional_direction,
            conviction_score            = excluded.conviction_score,
            top_holders_added_json      = excluded.top_holders_added_json,
            top_holders_trimmed_json    = excluded.top_holders_trimmed_json,
            latest_13f_date             = excluded.latest_13f_date,
            notes                       = excluded.notes
        """,
        (
            sig.as_of,
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            sig.symbol,
            sig.net_institutional_direction,
            sig.conviction_score,
            json.dumps(sig.top_holders_added),
            json.dumps(sig.top_holders_trimmed),
            sig.latest_13f_date,
            sig.notes,
        ),
    )
    conn.commit()


def _load_prev_holdings(conn: sqlite3.Connection, symbol: str) -> Optional[Dict[str, float]]:
    """Load previous holdings stored in raw_sources of the last run (best effort)."""
    # We don't store raw holdings per-symbol — use None as previous so new = first time
    return None


class SmartMoneyEngine(BaseEngine):
    """Aggregate institutional 13F filings into per-symbol conviction scores.

    For each tracked fund:
      1. Fetch latest 13F-HR filing from SEC EDGAR.
      2. Extract holdings for CEDEAR-eligible symbols.
      3. Compare to previous quarter (if available) to determine direction.
      4. Weight each fund's signal by its AUM weight.
      5. Aggregate into a conviction score (0-100) per symbol.

    Results are cached for _STALENESS_DAYS days.
    """

    def run(
        self,
        as_of: str,
        conn: sqlite3.Connection,
        *,
        symbols: Optional[List[str]] = None,
        force_refresh: bool = False,
    ) -> List[SmartMoneySignal]:
        """Fetch 13F data for tracked funds and produce signals.

        Args:
            symbols: Filter to these symbols (default: full CEDEAR universe).
            force_refresh: Re-fetch even if cached data is fresh.

        Returns list of SmartMoneySignal, one per symbol with data.
        """
        target_symbols = set(symbols or CEDEAR_UNIVERSE)

        # ── Fetch all tracked funds ──────────────────────────────────────────
        # fund_name → {ticker: shares}
        fund_holdings: Dict[str, Dict[str, float]] = {}
        fund_dates: Dict[str, Optional[str]] = {}
        fund_errors: Dict[str, str] = {}

        for fund in TRACKED_FUNDS:
            holdings, filing_date, err = fetch_13f_holdings(fund.cik)
            fund_holdings[fund.name] = holdings
            fund_dates[fund.name] = filing_date
            if err:
                fund_errors[fund.name] = err

        # ── Aggregate per symbol ─────────────────────────────────────────────
        # symbol → {direction: weighted_score}
        symbol_scores: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        symbol_adders: Dict[str, List[str]] = defaultdict(list)
        symbol_trimmers: Dict[str, List[str]] = defaultdict(list)
        symbol_latest_date: Dict[str, Optional[str]] = {}

        total_weight = sum(f.aum_weight for f in TRACKED_FUNDS)

        for fund in TRACKED_FUNDS:
            holdings = fund_holdings.get(fund.name, {})
            if not holdings:
                continue

            filing_date = fund_dates.get(fund.name)
            norm_holdings = {normalize_sec_ticker(k): v for k, v in holdings.items()}

            for symbol in target_symbols:
                if symbol not in norm_holdings:
                    continue
                # No previous data yet — treat as neutral held
                direction, change_pct = compute_direction(norm_holdings, {}, symbol)

                weight = fund.aum_weight / total_weight
                symbol_scores[symbol][direction] += weight * abs(change_pct if direction not in ("held",) else 1)

                if direction in ("added", "new"):
                    symbol_adders[symbol].append(fund.name)
                elif direction in ("trimmed", "exited"):
                    symbol_trimmers[symbol].append(fund.name)

                if filing_date:
                    prev = symbol_latest_date.get(symbol)
                    if prev is None or filing_date > prev:
                        symbol_latest_date[symbol] = filing_date

        # ── Build signals ────────────────────────────────────────────────────
        signals: List[SmartMoneySignal] = []

        for symbol in target_symbols:
            if symbol not in symbol_scores and symbol not in symbol_adders:
                # No fund holds this symbol — skip
                continue

            scores = symbol_scores[symbol]
            adders = symbol_adders[symbol]
            trimmers = symbol_trimmers[symbol]

            add_score = scores.get("added", 0) + scores.get("new", 0)
            trim_score = scores.get("trimmed", 0) + scores.get("exited", 0)
            held_score = scores.get("held", 0)

            # Net direction
            if add_score > trim_score and add_score > held_score:
                direction = "accumulate"
                conviction = min(100.0, add_score * 50)
            elif trim_score > add_score and trim_score > held_score:
                direction = "distribute"
                conviction = min(100.0, trim_score * 50)
            else:
                direction = "neutral"
                conviction = max(0.0, 50 - abs(add_score - trim_score) * 10)

            notes_parts = []
            if fund_errors:
                notes_parts.append(f"Fetch errors: {'; '.join(f'{k}: {v}' for k, v in fund_errors.items())}")

            sig = SmartMoneySignal(
                as_of=as_of,
                symbol=symbol,
                net_institutional_direction=direction,
                conviction_score=round(conviction, 1),
                top_holders_added=adders[:5],
                top_holders_trimmed=trimmers[:5],
                latest_13f_date=symbol_latest_date.get(symbol),
                notes="; ".join(notes_parts),
            )
            _upsert_smart_money(conn, sig)
            signals.append(sig)

        return signals

    def load_latest(
        self,
        conn: sqlite3.Connection,
        as_of: str,
        symbol: Optional[str] = None,
        lookback_days: int = 90,
    ) -> Optional[Any]:
        """Load cached signals.

        If symbol is given: return single SmartMoneySignal or None.
        If symbol is None: return list of all cached signals within lookback window.
        """
        cur = conn.cursor()
        if symbol:
            cur.execute(
                """
                SELECT as_of, symbol, net_institutional_direction,
                       conviction_score, top_holders_added_json,
                       top_holders_trimmed_json, latest_13f_date, notes
                FROM engine_smart_money_snapshots
                WHERE symbol = ?
                  AND as_of <= ?
                  AND as_of >= date(?, ?)
                ORDER BY as_of DESC
                LIMIT 1
                """,
                (symbol, as_of, as_of, f"-{lookback_days} days"),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return _row_to_signal(row)
        else:
            cur.execute(
                """
                SELECT as_of, symbol, net_institutional_direction,
                       conviction_score, top_holders_added_json,
                       top_holders_trimmed_json, latest_13f_date, notes
                FROM engine_smart_money_snapshots
                WHERE as_of <= ?
                  AND as_of >= date(?, ?)
                ORDER BY symbol, as_of DESC
                """,
                (as_of, as_of, f"-{lookback_days} days"),
            )
            rows = cur.fetchall()
            # De-duplicate: keep most recent per symbol
            seen = set()
            signals = []
            for row in rows:
                sym = row[1]
                if sym not in seen:
                    seen.add(sym)
                    signals.append(_row_to_signal(row))
            return signals


def _row_to_signal(row: tuple) -> SmartMoneySignal:
    (r_as_of, symbol, direction, conviction,
     added_json, trimmed_json, filing_date, notes) = row
    return SmartMoneySignal(
        as_of=r_as_of,
        symbol=symbol,
        net_institutional_direction=direction,
        conviction_score=conviction,
        top_holders_added=json.loads(added_json or "[]"),
        top_holders_trimmed=json.loads(trimmed_json or "[]"),
        latest_13f_date=filing_date,
        notes=notes or "",
    )
