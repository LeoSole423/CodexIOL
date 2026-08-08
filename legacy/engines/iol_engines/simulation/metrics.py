"""Performance metrics for backtests.

All functions take a list of (date, portfolio_value) equity curve points.
"""
from __future__ import annotations

import math
from typing import List, Tuple


EquityCurve = List[Tuple[str, float]]  # [(date, value), ...]


def total_return_pct(curve: EquityCurve) -> float:
    if len(curve) < 2:
        return 0.0
    start = curve[0][1]
    end = curve[-1][1]
    if start <= 0:
        return 0.0
    return (end - start) / start * 100.0


def daily_returns(curve: EquityCurve) -> List[float]:
    returns = []
    for i in range(1, len(curve)):
        prev = curve[i - 1][1]
        curr = curve[i][1]
        if prev > 0:
            returns.append((curr - prev) / prev)
        else:
            returns.append(0.0)
    return returns


def sharpe_ratio(curve: EquityCurve, annual_risk_free: float = 0.0) -> float:
    """Annualised Sharpe ratio. risk_free is annual (e.g. 0.05 for 5%)."""
    rets = daily_returns(curve)
    if len(rets) < 2:
        return 0.0
    n = len(rets)
    mean = sum(rets) / n
    variance = sum((r - mean) ** 2 for r in rets) / (n - 1)
    std = math.sqrt(variance) if variance > 0 else 0.0
    if std == 0:
        return 0.0
    daily_rf = (1 + annual_risk_free) ** (1 / 252) - 1
    return (mean - daily_rf) / std * math.sqrt(252)


def max_drawdown_pct(curve: EquityCurve) -> float:
    """Maximum peak-to-trough drawdown as a positive percentage."""
    if len(curve) < 2:
        return 0.0
    peak = curve[0][1]
    max_dd = 0.0
    for _, value in curve:
        if value > peak:
            peak = value
        if peak > 0:
            dd = (peak - value) / peak * 100.0
            if dd > max_dd:
                max_dd = dd
    return max_dd


def win_rate(trades: List[float]) -> float:
    """Fraction of closed trades with positive PnL. trades is list of pnl amounts."""
    if not trades:
        return 0.0
    wins = sum(1 for t in trades if t > 0)
    return wins / len(trades) * 100.0


def turnover_pct(total_traded_ars: float, avg_portfolio_value: float) -> float:
    """Total ARS traded / average portfolio value (annualised turnover proxy)."""
    if avg_portfolio_value <= 0:
        return 0.0
    return total_traded_ars / avg_portfolio_value * 100.0


def build_metrics_dict(
    curve: EquityCurve,
    trade_pnls: List[float],
    total_traded_ars: float,
) -> dict:
    avg_val = sum(v for _, v in curve) / len(curve) if curve else 1.0
    return {
        "total_return_pct": round(total_return_pct(curve), 2),
        "sharpe_ratio": round(sharpe_ratio(curve), 3),
        "max_drawdown_pct": round(max_drawdown_pct(curve), 2),
        "win_rate_pct": round(win_rate(trade_pnls), 1),
        "turnover_pct": round(turnover_pct(total_traded_ars, avg_val), 1),
        "n_days": len(curve),
        "equity_curve": [{"date": d, "value": round(v, 2)} for d, v in curve],
    }
