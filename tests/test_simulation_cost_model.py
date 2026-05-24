import pytest
import sqlite3

from iol_engines.simulation.cost_model import ExecutionCostModel
from iol_engines.simulation.portfolio_sim import SimulatedPortfolio
from iol_engines.simulation.report import add_estimated_gross_return_fields, execution_quality_summary


def test_gold_stock_costs_include_commission_market_fee_and_iva():
    model = ExecutionCostModel()

    fill = model.buy_fill(
        symbol="GGAL",
        amount_ars=1_000.0,
        price=100.0,
        slippage_pct=0.0,
        cash_available=2_000.0,
        instrument_type="stock",
        volume_amount=1_000_000.0,
    )

    assert fill.quantity == 10
    assert fill.commission_ars == pytest.approx(5.0)
    assert fill.market_fee_ars == pytest.approx(0.5)
    assert fill.iva_ars == pytest.approx(1.155)
    assert fill.total_cost_ars == pytest.approx(6.655)
    assert fill.net_cash_impact_ars == pytest.approx(1_006.655)


def test_bond_costs_include_market_fee_without_iva():
    model = ExecutionCostModel()

    fill = model.buy_fill(
        symbol="AL30",
        amount_ars=1_000.0,
        price=100.0,
        slippage_pct=0.0,
        cash_available=2_000.0,
        instrument_type="bond",
        volume_amount=1_000_000.0,
    )

    assert fill.quantity == 10
    assert fill.commission_ars == pytest.approx(5.0)
    assert fill.market_fee_ars == pytest.approx(0.1)
    assert fill.iva_ars == pytest.approx(0.0)
    assert fill.total_cost_ars == pytest.approx(5.1)


def test_cost_model_flags_disable_iva_and_market_fees():
    model = ExecutionCostModel.from_config(include_iva=False, include_market_fees=False)

    fill = model.buy_fill(
        symbol="GGAL",
        amount_ars=1_000.0,
        price=100.0,
        slippage_pct=0.0,
        cash_available=2_000.0,
        instrument_type="stock",
        volume_amount=1_000_000.0,
    )

    assert fill.commission_ars == pytest.approx(5.0)
    assert fill.market_fee_ars == pytest.approx(0.0)
    assert fill.iva_ars == pytest.approx(0.0)
    assert fill.total_cost_ars == pytest.approx(5.0)


def test_sizing_never_exceeds_available_cash():
    model = ExecutionCostModel()

    fill = model.buy_fill(
        symbol="GGAL",
        amount_ars=1_000.0,
        price=100.0,
        slippage_pct=0.0,
        cash_available=1_000.0,
        instrument_type="stock",
        volume_amount=1_000_000.0,
    )

    assert fill.quantity == 9
    assert fill.net_cash_impact_ars <= 1_000.0


def test_simulated_portfolio_buy_discounts_costs_and_slippage():
    portfolio = SimulatedPortfolio(cash_ars=2_000.0, slippage_pct=0.01)

    fill = portfolio.buy("GGAL", 1_000.0, 100.0, instrument_type="stock", volume_amount=1_000_000.0)

    assert fill.quantity == 9
    assert fill.slippage_ars == pytest.approx(9.0)
    assert portfolio.cash_ars == pytest.approx(2_000.0 - fill.net_cash_impact_ars)
    assert portfolio.cash_ars >= 0.0


def test_simulated_portfolio_sell_returns_net_pnl():
    portfolio = SimulatedPortfolio(cash_ars=2_000.0, slippage_pct=0.0)
    buy = portfolio.buy("GGAL", 1_000.0, 100.0, instrument_type="stock", volume_amount=1_000_000.0)

    sell = portfolio.sell_quantity("GGAL", buy.quantity, 110.0, instrument_type="stock", volume_amount=1_000_000.0)

    assert sell.realized_pnl_ars is not None
    assert sell.realized_pnl_ars < (110.0 - 100.0) * buy.quantity
    assert "GGAL" not in portfolio.holdings


def test_execution_quality_summary_reports_sources_and_liquidity():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE trades (
            run_id INTEGER,
            gross_amount_ars REAL,
            net_amount_ars REAL,
            total_cost_ars REAL,
            cost_model_json TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO trades VALUES (1, ?, ?, ?, ?)",
        [
            (1000.0, 1000.0, 6.65, '{"price_source":"symbol_daily_ohlcv.open","liquidity_warning":null}'),
            (500.0, 500.0, 3.0, '{"price_source":"fallback_last_price","liquidity_warning":"missing_volume_amount"}'),
            (250.0, 250.0, None, None),
        ],
    )

    summary = execution_quality_summary(conn, "trades", 1)

    assert summary["trades"] == 3
    assert summary["trades_with_costs"] == 2
    assert summary["cost_coverage_pct"] == pytest.approx(66.67)
    assert summary["total_costs_ars"] == pytest.approx(9.65)
    assert summary["cost_drag_pct_gross"] == pytest.approx(0.5514)
    assert summary["price_source_counts"]["symbol_daily_ohlcv.open"] == 1
    assert summary["price_source_counts"]["fallback_last_price"] == 1
    assert summary["price_source_counts"]["missing_price_source"] == 1
    assert summary["liquidity_warning_counts"]["missing_volume_amount"] == 1


def test_estimated_gross_return_adds_cost_drag_to_net_return():
    row = {
        "initial_cash": 100_000.0,
        "total_return_pct": -1.25,
        "total_costs_ars": 500.0,
    }

    add_estimated_gross_return_fields(row, initial_key="initial_cash")

    assert row["cost_return_drag_pct_points"] == pytest.approx(0.5)
    assert row["estimated_gross_return_pct"] == pytest.approx(-0.75)
