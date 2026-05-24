import pytest

from iol_engines.simulation.cost_model import ExecutionCostModel
from iol_engines.simulation.portfolio_sim import SimulatedPortfolio


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
