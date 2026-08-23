from datetime import date, datetime, timezone

from iol_review.context import build_context, normalize_orders, normalize_portfolio, stable_hash


class Gateway:
    def account_status(self): return {"totalEnPesos": 10}
    def portfolio(self, country): return {"activos": [{"simbolo": "ggal", "cantidad": 2, "ultimoPrecio": 100, "valorizado": 200, "moneda": "ARS"}]}
    def orders(self): return []
    def optional_movements(self, as_of): return (None, "HTTP 403")


def test_normalizes_portfolio_and_cash():
    portfolio = normalize_portfolio({"activos": [{"simbolo": "aapl", "cantidad": "3", "ultimoPrecio": "10.5"}], "totalEnDolares": "40"}, "estados_unidos")
    assert portfolio["assets"][0]["symbol"] == "AAPL"
    assert portfolio["assets"][0]["quantity"] == 3.0
    assert portfolio["cash"] == [{"currency": "USD", "amount": 40.0, "reported_total": True}]


def test_normalizes_iol_nested_title():
    portfolio = normalize_portfolio({"activos": [{"titulo": {"simbolo": "gld", "descripcion": "CEDEAR ETF"}, "cantidad": 4, "valorizado": 400}]}, "argentina")
    assert portfolio["assets"][0]["symbol"] == "GLD"
    assert portfolio["assets"][0]["instrument"] == "CEDEAR ETF"


def test_context_marks_optional_movements_unavailable_and_hash_is_stable():
    context = build_context(Gateway(), date(2026, 8, 1), datetime(2026, 8, 1, tzinfo=timezone.utc))
    assert context["sources"]["movements"]["status"] == "unavailable"
    assert context["content_hash"] == stable_hash(context)


def test_orders_are_normalized_without_raw_fields():
    orders = normalize_orders([{"numero": 2, "simbolo": "GGAL", "estado": "pendiente", "sensitive": "not persisted"}])
    assert orders == [{"number": 2, "symbol": "GGAL", "market": None, "side": None, "status": "pendiente", "quantity": None, "price": None, "created_at": None}]
