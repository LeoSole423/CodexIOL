"""SimulatedPortfolio — in-memory paper trading portfolio state.

Tracks cash and holdings. Prices come from market_symbol_snapshots.
No IOL API calls — purely DB-driven.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


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

    def mark_to_market(self, prices: Dict[str, float]) -> float:
        """Return total portfolio value in ARS given current prices."""
        equity = sum(
            pos.quantity * prices.get(pos.symbol, pos.avg_price)
            for pos in self.holdings.values()
        )
        return self.cash_ars + equity

    def can_buy(self, amount_ars: float) -> bool:
        return self.cash_ars >= amount_ars

    def buy(self, symbol: str, amount_ars: float, price: float) -> float:
        """Execute a buy. Returns actual shares purchased."""
        if price <= 0:
            return 0.0
        shares = amount_ars / price
        if symbol in self.holdings:
            pos = self.holdings[symbol]
            total_shares = pos.quantity + shares
            total_cost = pos.cost_basis + amount_ars
            self.holdings[symbol] = Position(
                symbol=symbol,
                quantity=total_shares,
                avg_price=total_cost / total_shares,
            )
        else:
            self.holdings[symbol] = Position(
                symbol=symbol, quantity=shares, avg_price=price
            )
        self.cash_ars -= amount_ars
        return shares

    def sell(self, symbol: str, amount_ars: float, price: float) -> float:
        """Execute a sell (trim or full exit). Returns shares sold."""
        if symbol not in self.holdings or price <= 0:
            return 0.0
        pos = self.holdings[symbol]
        shares_to_sell = min(amount_ars / price, pos.quantity)
        proceeds = shares_to_sell * price
        remaining = pos.quantity - shares_to_sell
        if remaining < 0.0001:
            del self.holdings[symbol]
        else:
            self.holdings[symbol] = Position(
                symbol=symbol, quantity=remaining, avg_price=pos.avg_price
            )
        self.cash_ars += proceeds
        return shares_to_sell

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
