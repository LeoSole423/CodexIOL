import pytest
import typer

from iol_cli.commands_orders import build_order_payload


def test_buy_limit_payload():
    payload = build_order_payload("buy", "bcba", " ggal ", 10, 1000, None, "ci", "2026-08-09T18:00:00Z", "limit")
    assert payload["mercado"] == "bCBA"
    assert payload["simbolo"] == "GGAL"
    assert payload["cantidad"] == 10
    assert payload["tipoOrden"] == "precioLimite"


def test_market_buy_by_amount():
    payload = build_order_payload("buy", "bcba", "SPY", None, None, 100000, "t0", None, "market")
    assert payload["monto"] == 100000
    assert "precio" not in payload


@pytest.mark.parametrize("kwargs", [
    dict(side="sell", quantity=None, amount=None, price=10),
    dict(side="sell", quantity=1, amount=10, price=10),
    dict(side="buy", quantity=1, amount=10, price=10),
    dict(side="buy", quantity=1, amount=None, price=None),
])
def test_invalid_orders(kwargs):
    with pytest.raises(typer.BadParameter):
        build_order_payload(market="bcba", symbol="GGAL", plazo="t0", valid_until=None, order_type="limit", **kwargs)
