"""Portfolio Strategy Engine — Motor 5 (Meta-Engine).

Combines outputs from all 4 data engines + the existing opportunity candidates
into a concrete, IOL-constrained action plan.

Does NOT generate new opportunity candidates — it consumes the already-ranked
list from the existing advisor_opportunity_candidates table.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..base import BaseEngine
from ..signals import MacroSignal, PositionAction, RegimeSignal, StrategyActionPlan


# Keep 5% of available cash as buffer
_CASH_RESERVE = 0.05
# If AR stress exceeds this, prefer CEDEARs over local BCBA stocks
_CEDEAR_PREFERENCE_STRESS = 65.0
# Bonus score applied to CEDEARs when AR stress is high
_CEDEAR_BONUS = 5.0


def _load_latest_opportunity_candidates(
    conn: sqlite3.Connection,
    as_of: str,
    top_n: int = 20,
) -> List[Dict[str, Any]]:
    """Load the top candidates from the most recent opportunity run on/before as_of."""
    cur = conn.cursor()
    # Find the latest run
    cur.execute(
        """
        SELECT id FROM advisor_opportunity_runs
        WHERE as_of <= ? AND status = 'done'
        ORDER BY as_of DESC, id DESC
        LIMIT 1
        """,
        (as_of,),
    )
    row = cur.fetchone()
    if row is None:
        return []
    run_id = row[0]

    cur.execute(
        """
        SELECT symbol, candidate_type, signal_side, signal_family,
               score_total, suggested_weight_pct, suggested_amount_ars,
               reason_summary, sector_bucket, decision_gate, candidate_status
        FROM advisor_opportunity_candidates
        WHERE run_id = ?
        ORDER BY score_total DESC
        LIMIT ?
        """,
        (run_id, top_n),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_portfolio_cash(conn: sqlite3.Connection, as_of: str) -> Dict[str, float]:
    """Load latest available cash from portfolio_snapshots."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT cash_disponible_ars, cash_disponible_usd, total_value
        FROM portfolio_snapshots
        WHERE snapshot_date <= ?
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (as_of,),
    )
    row = cur.fetchone()
    if row is None:
        return {"cash_ars": 0.0, "cash_usd": 0.0, "total_value": 0.0}
    return {
        "cash_ars": float(row[0] or 0),
        "cash_usd": float(row[1] or 0),
        "total_value": float(row[2] or 0),
    }


def _is_cedear(symbol: str, sector_bucket: Optional[str]) -> bool:
    """Heuristic: CEDEARs are US stocks cross-listed in Argentina.
    They typically don't have the Argentine suffixes.
    """
    # This is a best-effort heuristic; could be replaced with a lookup table.
    ar_suffixes = ("AL", "AR", "D", "C1", "BA")
    if sector_bucket and "cedear" in (sector_bucket or "").lower():
        return True
    # Symbols ending in common Argentine suffixes are local stocks
    for sfx in ar_suffixes:
        if symbol.upper().endswith(sfx) and len(symbol) > len(sfx):
            return True
    # Short symbols without numbers are likely US stocks (CEDEARs)
    return len(symbol) <= 5 and symbol.isalpha()


def _upsert_strategy_run(
    conn: sqlite3.Connection,
    plan: StrategyActionPlan,
    regime_snapshot_id: Optional[int],
    macro_snapshot_id: Optional[int],
    opportunity_run_id: Optional[int],
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO engine_strategy_runs
            (created_at_utc, as_of, opportunity_run_id, regime_snapshot_id,
             macro_snapshot_id, portfolio_cash_ars, portfolio_cash_usd,
             defensive_overlay_applied, actions_json, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            plan.as_of,
            opportunity_run_id,
            regime_snapshot_id,
            macro_snapshot_id,
            plan.portfolio_cash_ars,
            plan.portfolio_cash_usd,
            int(plan.defensive_overlay_applied),
            json.dumps([a.to_dict() for a in plan.actions]),
            plan.notes,
        ),
    )
    conn.commit()
    return cur.lastrowid or 0


class PortfolioStrategyEngine(BaseEngine):
    """Combine all engine signals with real portfolio constraints to produce an action plan.

    Inputs (all optional — degrades gracefully):
      - regime_signal: to apply defensive equity reduction
      - macro_signal: to prefer CEDEARs when AR stress is high
      - opportunity_candidates: from existing advisor_opportunity_candidates table
      - portfolio cash: from portfolio_snapshots

    Output: StrategyActionPlan with concrete buy/sell amounts in ARS.
    """

    def run(
        self,
        as_of: str,
        conn: sqlite3.Connection,
        *,
        regime_signal: Optional[RegimeSignal] = None,
        macro_signal: Optional[MacroSignal] = None,
        opportunity_run_id: Optional[int] = None,
        regime_snapshot_id: Optional[int] = None,
        macro_snapshot_id: Optional[int] = None,
        top_n: int = 15,
        budget_ars: Optional[float] = None,
    ) -> StrategyActionPlan:

        # ── Load portfolio constraints ───────────────────────────────────────
        cash = _load_portfolio_cash(conn, as_of)
        available_ars = budget_ars if budget_ars is not None else cash["cash_ars"]
        spendable_ars = available_ars * (1 - _CASH_RESERVE)

        # ── Load opportunity candidates ──────────────────────────────────────
        candidates = _load_latest_opportunity_candidates(conn, as_of, top_n=top_n)

        # ── Apply regime defensive overlay ───────────────────────────────────
        defensive_applied = False
        equity_budget = spendable_ars

        if regime_signal is not None and regime_signal.defensive_weight_adjustment < 0:
            # Reduce equity budget proportionally
            equity_reduction = abs(regime_signal.defensive_weight_adjustment)
            equity_budget = spendable_ars * (1 - equity_reduction)
            defensive_applied = True

        # ── Score adjustments: prefer CEDEARs when AR stress is high ─────────
        prefer_cedear = (
            macro_signal is not None
            and macro_signal.argentina_macro_stress > _CEDEAR_PREFERENCE_STRESS
        )

        # ── Build action list ────────────────────────────────────────────────
        actions: List[PositionAction] = []
        deployed_ars = 0.0

        for c in candidates:
            symbol = c["symbol"]
            side = (c.get("signal_side") or "buy").lower()
            score = float(c.get("score_total") or 0)
            suggested_amount = float(c.get("suggested_amount_ars") or 0)
            reason = c.get("reason_summary") or ""
            sector = c.get("sector_bucket") or ""

            # Apply CEDEAR preference bonus
            is_ced = _is_cedear(symbol, sector)
            if prefer_cedear and is_ced:
                score += _CEDEAR_BONUS

            # Determine action type
            if side == "sell":
                action_type = c.get("signal_family") or "trim"  # "trim" or "exit"
                amount = suggested_amount
            else:
                action_type = "buy"
                # Cap each position to what's left in the budget
                remaining = equity_budget - deployed_ars
                if remaining <= 0:
                    break
                amount = min(suggested_amount, remaining)

            if amount <= 0:
                continue

            weight_pct = float(c.get("suggested_weight_pct") or 0)

            actions.append(PositionAction(
                symbol=symbol,
                action=action_type,
                amount_ars=round(amount, 2),
                weight_pct=round(weight_pct, 2),
                reason=reason,
                engine_source="strategy+opportunity",
                candidate_score=round(score, 2),
            ))

            if side == "buy":
                deployed_ars += amount

        notes_parts = []
        if defensive_applied:
            pct = abs(regime_signal.defensive_weight_adjustment) * 100
            notes_parts.append(
                f"Defensive overlay: {pct:.0f}% equity reduction ({regime_signal.regime} regime)."
            )
        if prefer_cedear:
            notes_parts.append(
                f"CEDEAR preference: AR stress={macro_signal.argentina_macro_stress:.0f}/100 > {_CEDEAR_PREFERENCE_STRESS:.0f}."
            )
        if not candidates:
            notes_parts.append("No opportunity candidates found — run iol advisor opportunities run first.")

        plan = StrategyActionPlan(
            as_of=as_of,
            portfolio_cash_ars=cash["cash_ars"],
            portfolio_cash_usd=cash["cash_usd"],
            actions=actions,
            total_deployed_ars=round(deployed_ars, 2),
            defensive_overlay_applied=defensive_applied,
            regime=regime_signal.regime if regime_signal else "unknown",
            notes=" ".join(notes_parts),
        )

        _upsert_strategy_run(conn, plan, regime_snapshot_id, macro_snapshot_id, opportunity_run_id)
        return plan

    def load_latest(self, conn: sqlite3.Connection, as_of: str) -> Optional[StrategyActionPlan]:
        """Load the most recent strategy run on or before as_of."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT as_of, portfolio_cash_ars, portfolio_cash_usd,
                   defensive_overlay_applied, actions_json, notes
            FROM engine_strategy_runs
            WHERE as_of <= ?
            ORDER BY as_of DESC, id DESC
            LIMIT 1
            """,
            (as_of,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        r_as_of, cash_ars, cash_usd, def_applied, actions_json, notes = row
        actions_raw = json.loads(actions_json or "[]")
        actions = [
            PositionAction(
                symbol=a["symbol"],
                action=a["action"],
                amount_ars=a["amount_ars"],
                weight_pct=a["weight_pct"],
                reason=a["reason"],
                engine_source=a["engine_source"],
                candidate_score=a["candidate_score"],
            )
            for a in actions_raw
        ]
        deployed = sum(a.amount_ars for a in actions if a.action == "buy")
        return StrategyActionPlan(
            as_of=r_as_of,
            portfolio_cash_ars=cash_ars or 0,
            portfolio_cash_usd=cash_usd or 0,
            actions=actions,
            total_deployed_ars=round(deployed, 2),
            defensive_overlay_applied=bool(def_applied),
            regime="unknown",
            notes=notes or "",
        )
