"""Macro Momentum Engine — Motor 2.

Fetches and aggregates:
  - Argentina: BCRA policy rate, official USD/ARS, CEDEAR FX premium
  - Global: Fed Funds Rate, US CPI YoY, VIX
  - Sentiment: aggregated from advisor_evidence table

Produces a MacroSignal and upserts it into engine_macro_snapshots.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..base import BaseEngine
from ..signals import MacroSignal
from .argentina import (
    compute_argentina_stress,
    fetch_bcra_rate,
    fetch_cedear_fx_premium,
    fetch_prev_usd_official,
    fetch_usd_official,
)
from .global_macro import compute_global_risk_on, fetch_fed_rate, fetch_us_cpi_yoy, fetch_vix
from .sentiment import compute_macro_sentiment


def _upsert_macro(conn: sqlite3.Connection, sig: MacroSignal, raw_sources: Dict) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO engine_macro_snapshots
            (as_of, created_at_utc, inflation_mom_pct, bcra_rate_pct,
             usd_ars_official, usd_ars_blue, cedear_fx_premium_pct,
             fed_rate_pct, us_cpi_yoy_pct, argentina_macro_stress,
             global_risk_on, sentiment_score, upcoming_events_json,
             raw_sources_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(as_of) DO UPDATE SET
            created_at_utc          = excluded.created_at_utc,
            inflation_mom_pct       = excluded.inflation_mom_pct,
            bcra_rate_pct           = excluded.bcra_rate_pct,
            usd_ars_official        = excluded.usd_ars_official,
            usd_ars_blue            = excluded.usd_ars_blue,
            cedear_fx_premium_pct   = excluded.cedear_fx_premium_pct,
            fed_rate_pct            = excluded.fed_rate_pct,
            us_cpi_yoy_pct          = excluded.us_cpi_yoy_pct,
            argentina_macro_stress  = excluded.argentina_macro_stress,
            global_risk_on          = excluded.global_risk_on,
            sentiment_score         = excluded.sentiment_score,
            upcoming_events_json    = excluded.upcoming_events_json,
            raw_sources_json        = excluded.raw_sources_json
        """,
        (
            sig.as_of,
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            sig.inflation_mom_pct,
            sig.bcra_rate_pct,
            sig.usd_ars_official,
            sig.usd_ars_blue,
            sig.cedear_fx_premium_pct,
            sig.fed_rate_pct,
            sig.us_cpi_yoy_pct,
            sig.argentina_macro_stress,
            sig.global_risk_on,
            sig.sentiment_score,
            json.dumps(sig.upcoming_events),
            json.dumps(raw_sources),
        ),
    )
    conn.commit()
    return cur.lastrowid or 0


class MacroMomentumEngine(BaseEngine):
    """Fetch and aggregate macroeconomic signals.

    Argentina layer: BCRA Open Data API (official USD, policy rate).
    Global layer: FRED CSV (Fed Funds, CPI, VIX).
    Sentiment: aggregated from existing advisor_evidence table.

    All external HTTP calls are optional — the engine degrades gracefully
    when network is unavailable, returning partial signals with neutral defaults.
    """

    def run(
        self,
        as_of: str,
        conn: sqlite3.Connection,
        *,
        force_refresh: bool = False,
        skip_external: bool = False,
    ) -> MacroSignal:
        """Compute macro signal for *as_of* and upsert into the DB."""
        if not force_refresh:
            cached = self.load_latest(conn, as_of)
            if cached is not None and cached.as_of == as_of:
                return cached

        notes: List[str] = []
        raw: Dict[str, Any] = {}

        # ── Argentina ────────────────────────────────────────────────────────
        bcra_rate: Optional[float] = None
        usd_official: Optional[float] = None
        cedear_premium: Optional[float] = None

        if not skip_external:
            usd_official, err = fetch_usd_official()
            if err:
                notes.append(err)
            raw["usd_ars_official"] = usd_official

            bcra_rate, err = fetch_bcra_rate()
            if err:
                notes.append(err)
            raw["bcra_rate_pct"] = bcra_rate

            prev_usd, err = fetch_prev_usd_official()
            if err:
                notes.append(err)
            raw["prev_usd_ars_official"] = prev_usd

            cedear_premium, err = fetch_cedear_fx_premium(conn, as_of, usd_official)
            if err:
                notes.append(err)
            raw["cedear_fx_premium_pct"] = cedear_premium
        else:
            notes.append("skip_external=True: BCRA/FRED calls skipped.")

        ar_stress = compute_argentina_stress(
            bcra_rate_pct=bcra_rate,
            usd_official=usd_official,
            cedear_premium=cedear_premium,
            prev_usd_official=raw.get("prev_usd_ars_official"),
        )

        # ── Global ───────────────────────────────────────────────────────────
        fed_rate: Optional[float] = None
        us_cpi: Optional[float] = None
        vix_val: Optional[float] = None

        if not skip_external:
            fed_rate, err = fetch_fed_rate()
            if err:
                notes.append(err)
            raw["fed_rate_pct"] = fed_rate

            us_cpi, err = fetch_us_cpi_yoy()
            if err:
                notes.append(err)
            raw["us_cpi_yoy_pct"] = us_cpi

            vix_val, err = fetch_vix()
            if err:
                notes.append(err)
            raw["vix"] = vix_val

        global_risk = compute_global_risk_on(fed_rate, us_cpi, vix_val)

        # ── Sentiment ────────────────────────────────────────────────────────
        sentiment = compute_macro_sentiment(conn)
        raw["sentiment_score"] = sentiment

        sig = MacroSignal(
            as_of=as_of,
            argentina_macro_stress=ar_stress,
            global_risk_on=global_risk,
            inflation_mom_pct=None,       # INDEC — Phase 3 (scraping complex)
            bcra_rate_pct=bcra_rate,
            usd_ars_official=usd_official,
            usd_ars_blue=None,            # computed from CEDEAR FX — Phase 3
            cedear_fx_premium_pct=cedear_premium,
            fed_rate_pct=fed_rate,
            us_cpi_yoy_pct=us_cpi,
            sentiment_score=sentiment,
            upcoming_events=[],           # economic calendar — Phase 3
        )

        _upsert_macro(conn, sig, {"notes": notes, **raw})
        return sig

    def load_latest(self, conn: sqlite3.Connection, as_of: str) -> Optional[MacroSignal]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT as_of, inflation_mom_pct, bcra_rate_pct, usd_ars_official,
                   usd_ars_blue, cedear_fx_premium_pct, fed_rate_pct,
                   us_cpi_yoy_pct, argentina_macro_stress, global_risk_on,
                   sentiment_score, upcoming_events_json
            FROM engine_macro_snapshots
            WHERE as_of <= ?
            ORDER BY as_of DESC
            LIMIT 1
            """,
            (as_of,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        (
            r_as_of, inflation, bcra_rate, usd_off, usd_blue,
            cedear_prem, fed_rate, us_cpi, ar_stress, global_risk,
            sentiment, events_json,
        ) = row
        return MacroSignal(
            as_of=r_as_of,
            argentina_macro_stress=ar_stress,
            global_risk_on=global_risk,
            inflation_mom_pct=inflation,
            bcra_rate_pct=bcra_rate,
            usd_ars_official=usd_off,
            usd_ars_blue=usd_blue,
            cedear_fx_premium_pct=cedear_prem,
            fed_rate_pct=fed_rate,
            us_cpi_yoy_pct=us_cpi,
            sentiment_score=sentiment,
            upcoming_events=json.loads(events_json or "[]"),
        )
