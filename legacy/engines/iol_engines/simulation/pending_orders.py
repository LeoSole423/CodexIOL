"""Helpers for realistic pending-order handling."""
from __future__ import annotations

import sqlite3
from typing import Optional

from .portfolio_sim import SimulatedPortfolio

UNFILLABLE_MIN_LOT = "unfillable_min_lot"


def buy_is_unfillable_min_lot(
    portfolio: SimulatedPortfolio,
    *,
    symbol: str,
    amount_ars: float,
    price: float,
    instrument_type: str,
    price_source: str,
    volume_amount: Optional[float] = None,
) -> bool:
    """Return True when the order budget cannot buy one whole unit net of costs."""
    if amount_ars <= 0 or price <= 0:
        return True
    fill = portfolio.cost_model.buy_fill(
        symbol=symbol,
        amount_ars=amount_ars,
        price=price,
        slippage_pct=portfolio.slippage_pct,
        cash_available=amount_ars,
        instrument_type=instrument_type,
        price_source=price_source,
        volume_amount=volume_amount,
    )
    return fill.quantity <= 0


def cancel_pending_order(
    conn: sqlite3.Connection,
    table: str,
    order_id: object,
    *,
    as_of: str,
    price: float,
    reason: str,
) -> None:
    """Mark a DB-backed pending order as cancelled with execution context."""
    if order_id is None:
        return
    conn.execute(
        f"""
        UPDATE {table}
        SET status='cancelled',
            execute_date=?,
            execute_price=?,
            cancel_reason=?
        WHERE id=? AND status='pending'
        """,
        (as_of, float(price), reason, int(order_id)),
    )
