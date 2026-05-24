"""SimulatedPortfolio — in-memory paper trading portfolio state.

Tracks cash and holdings. Prices come from market_symbol_snapshots.
No IOL API calls — purely DB-driven.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .cost_model import ExecutionCostModel, ExecutionFill


@dataclass
class Position:
    symbol: str
    quantity: float
    avg_price: float  # ARS cost basis per unit

    @property
    def cost_basis(self) -> float:
        return self.quantity * self.avg_price


@dataclass
class SimulatedPortfolio:
    cash_ars: float
    holdings: Dict[str, Position] = field(default_factory=dict)
    commission_rate: float = 0.0   # Fraction of trade value (e.g. 0.006 = 0.6%)
    commission_min: float = 0.0    # Minimum commission per trade in ARS
    slippage_pct: float = 0.0      # One-way slippage fraction (e.g. 0.002 = 0.2%)
    cost_model: Optional[ExecutionCostModel] = None

    def __post_init__(self) -> None:
        if self.cost_model is None:
            legacy = self.commission_rate if self.commission_rate > 0 else None
            self.cost_model = ExecutionCostModel.from_config(
                commission_min=self.commission_min,
                legacy_commission_rate=legacy,
            )

    def mark_to_market(self, prices: Dict[str, float]) -> float:
        """Return total portfolio value in ARS given current prices."""
        equity = sum(
            pos.quantity * prices.get(pos.symbol, pos.avg_price)
            for pos in self.holdings.values()
        )
        return self.cash_ars + equity

    def can_buy(self, amount_ars: float) -> bool:
        return self.cash_ars >= amount_ars

    def buy(
        self,
        symbol: str,
        amount_ars: float,
        price: float,
        *,
        instrument_type: str = "stock",
        price_source: str = "market_symbol_snapshots",
        volume_amount: Optional[float] = None,
    ) -> ExecutionFill:
        """Execute a buy with slippage and costs. Returns the execution fill."""
        if price <= 0:
            return self.cost_model.buy_fill(
                symbol=symbol,
                amount_ars=0.0,
                price=1.0,
                slippage_pct=0.0,
                cash_available=0.0,
                instrument_type=instrument_type,
                price_source=price_source,
                volume_amount=volume_amount,
            )
        fill = self.cost_model.buy_fill(
            symbol=symbol,
            amount_ars=amount_ars,
            price=price,
            slippage_pct=self.slippage_pct,
            cash_available=self.cash_ars,
            instrument_type=instrument_type,
            price_source=price_source,
            volume_amount=volume_amount,
        )
        if fill.quantity <= 0:
            return fill
        if symbol in self.holdings:
            pos = self.holdings[symbol]
            total_shares = pos.quantity + fill.quantity
            total_cost = pos.cost_basis + fill.total_entry_basis_ars
            self.holdings[symbol] = Position(
                symbol=symbol,
                quantity=total_shares,
                avg_price=total_cost / total_shares,
            )
        else:
            self.holdings[symbol] = Position(
                symbol=symbol, quantity=fill.quantity, avg_price=fill.total_entry_basis_ars / fill.quantity
            )
        self.cash_ars -= fill.net_cash_impact_ars
        return fill

    def sell(
        self,
        symbol: str,
        amount_ars: float,
        price: float,
        *,
        instrument_type: str = "stock",
        price_source: str = "market_symbol_snapshots",
        volume_amount: Optional[float] = None,
    ) -> ExecutionFill:
        """Execute a sell with slippage and costs. Returns the execution fill."""
        if symbol not in self.holdings or price <= 0:
            return self.cost_model.sell_fill(
                symbol=symbol,
                quantity=0.0,
                price=1.0,
                slippage_pct=0.0,
                instrument_type=instrument_type,
                price_source=price_source,
                volume_amount=volume_amount,
                cost_basis_ars=0.0,
            )
        pos = self.holdings[symbol]
        requested_qty = min(max(amount_ars, 0.0) / price, pos.quantity)
        cost_basis_ars = requested_qty * pos.avg_price
        fill = self.cost_model.sell_fill(
            symbol=symbol,
            quantity=requested_qty,
            price=price,
            slippage_pct=self.slippage_pct,
            instrument_type=instrument_type,
            price_source=price_source,
            volume_amount=volume_amount,
            cost_basis_ars=cost_basis_ars,
        )
        if fill.quantity <= 0:
            return fill
        remaining = pos.quantity - fill.quantity
        if remaining < 0.0001:
            del self.holdings[symbol]
        else:
            self.holdings[symbol] = Position(
                symbol=symbol, quantity=remaining, avg_price=pos.avg_price
            )
        self.cash_ars += fill.net_cash_impact_ars
        return fill

    def sell_quantity(
        self,
        symbol: str,
        quantity: float,
        price: float,
        *,
        instrument_type: str = "stock",
        price_source: str = "market_symbol_snapshots",
        volume_amount: Optional[float] = None,
    ) -> ExecutionFill:
        if symbol not in self.holdings:
            return self.sell(symbol, 0.0, price, instrument_type=instrument_type, price_source=price_source)
        amount = max(0.0, float(quantity or 0.0)) * price
        return self.sell(
            symbol,
            amount,
            price,
            instrument_type=instrument_type,
            price_source=price_source,
            volume_amount=volume_amount,
        )

    def position_value(self, symbol: str, price: Optional[float] = None) -> float:
        if symbol not in self.holdings:
            return 0.0
        pos = self.holdings[symbol]
        p = price if price is not None else pos.avg_price
        return pos.quantity * p

    def position_weight(self, symbol: str, total_value: float, price: Optional[float] = None) -> float:
        if total_value <= 0:
            return 0.0
        return self.position_value(symbol, price) / total_value

    @property
    def n_positions(self) -> int:
        return len(self.holdings)


# ── Price loading helpers ─────────────────────────────────────────────────────

def load_prices_for_date(conn: sqlite3.Connection, as_of: str) -> Dict[str, float]:
    """Load the latest price for each symbol on or before as_of."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, last_price
        FROM market_symbol_snapshots
        WHERE snapshot_date <= ?
          AND last_price IS NOT NULL
          AND last_price > 0
        GROUP BY symbol
        HAVING snapshot_date = MAX(snapshot_date)
        """,
        (as_of,),
    )
    return {row[0]: float(row[1]) for row in cur.fetchall()}


def load_execution_metadata_for_date(conn: sqlite3.Connection, as_of: str) -> Dict[str, Dict[str, object]]:
    """Load latest symbol metadata used by execution-cost simulation."""
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT m.symbol, m.volume_amount, pa.type
        FROM market_symbol_snapshots m
        LEFT JOIN portfolio_assets pa
          ON pa.symbol = m.symbol
         AND pa.snapshot_date = (
             SELECT MAX(snapshot_date)
             FROM portfolio_assets p2
             WHERE p2.symbol = m.symbol AND p2.snapshot_date <= ?
         )
        WHERE m.snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM market_symbol_snapshots m2
            WHERE m2.symbol = m.symbol AND m2.snapshot_date <= ?
        )
        """,
        (as_of, as_of),
    ).fetchall()
    return {
        str(row[0]): {
            "volume_amount": float(row[1]) if row[1] is not None else None,
            "instrument_type": row[2],
        }
        for row in rows
    }


def load_trading_dates(
    conn: sqlite3.Connection, date_from: str, date_to: str
) -> List[str]:
    """Return sorted list of dates that have market_symbol_snapshots data."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT snapshot_date
        FROM market_symbol_snapshots
        WHERE snapshot_date >= ? AND snapshot_date <= ?
        ORDER BY snapshot_date ASC
        """,
        (date_from, date_to),
    )
    return [row[0] for row in cur.fetchall()]
