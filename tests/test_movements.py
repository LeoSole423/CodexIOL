"""Tests for commands_movements.py and updated portfolio_db normalization."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import os
from pathlib import Path

import pytest

from iol_cli.db import connect, init_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _conn():
    tmp = tempfile.TemporaryDirectory()
    db_path = os.path.join(tmp.name, "test.db")
    conn = connect(db_path)
    init_db(conn)
    return tmp, conn


# ---------------------------------------------------------------------------
# _norm_order_side — new mappings
# ---------------------------------------------------------------------------

class TestNormOrderSide:
    def _norm(self, v):
        from src.iol_shared.portfolio_db import _norm_order_side
        return _norm_order_side(v)

    def test_buy_unchanged(self):
        assert self._norm("compra") == "buy"
        assert self._norm("buy") == "buy"
        assert self._norm("suscripcion fci") == "buy"

    def test_sell_unchanged(self):
        assert self._norm("venta") == "sell"
        assert self._norm("sell") == "sell"
        assert self._norm("rescate fci") == "sell"

    def test_amortizacion_is_bond_amortization(self):
        assert self._norm("pago de amortizacion") == "bond_amortization"

    def test_dividendos_is_dividend(self):
        assert self._norm("pago de dividendos") == "dividend"

    def test_renta_is_coupon(self):
        assert self._norm("pago de renta") == "coupon"

    def test_fees_unchanged(self):
        assert self._norm("comision") == "fee"
        assert self._norm("iva") == "fee"
        assert self._norm("derechos de mercado") == "fee"
        assert self._norm("derecho de mercado") == "fee"

    def test_unknown_returns_none(self):
        assert self._norm("unknown_type") is None
        assert self._norm("") is None


# ---------------------------------------------------------------------------
# orders_flow_summary — new amounts exposed
# ---------------------------------------------------------------------------

class TestOrdersFlowSummary:
    def _make_order(self, conn, order_number, side, amount):
        conn.execute(
            """
            INSERT OR REPLACE INTO orders(
                order_number, status, symbol, market, side, side_norm,
                quantity, price, order_type, created_at, operated_at,
                operated_amount, currency
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (order_number, "terminada", "AL30", "bcba", side, None,
             100, amount / 100, "market",
             "2024-01-10T00:00:00", "2024-01-10T12:00:00",
             amount, "peso_Argentino"),
        )

    def test_bond_amortization_goes_to_bond_amortization_amount(self):
        tmp, conn = _conn()
        try:
            self._make_order(conn, 1, "pago de amortizacion", 50000)
            conn.commit()
            from src.iol_shared.portfolio_db import orders_flow_summary
            amounts, stats = orders_flow_summary(
                conn, "2024-01-09T23:59:59", "2024-01-10T23:59:59"
            )
            assert amounts["bond_amortization_amount"] == pytest.approx(50000)
            assert amounts["sell_amount"] == pytest.approx(0)
            assert stats["amortization_classified"] == 1
        finally:
            conn.close()

    def test_dividend_goes_to_dividend_and_income(self):
        tmp, conn = _conn()
        try:
            self._make_order(conn, 2, "pago de dividendos", 3000)
            conn.commit()
            from src.iol_shared.portfolio_db import orders_flow_summary
            amounts, stats = orders_flow_summary(
                conn, "2024-01-09T23:59:59", "2024-01-10T23:59:59"
            )
            assert amounts["dividend_amount"] == pytest.approx(3000)
            assert amounts["income_amount"] == pytest.approx(3000)
            assert stats["dividend_classified"] == 1
            assert stats["income_classified"] == 1
        finally:
            conn.close()

    def test_coupon_goes_to_coupon_and_income(self):
        tmp, conn = _conn()
        try:
            self._make_order(conn, 3, "pago de renta", 7500)
            conn.commit()
            from src.iol_shared.portfolio_db import orders_flow_summary
            amounts, stats = orders_flow_summary(
                conn, "2024-01-09T23:59:59", "2024-01-10T23:59:59"
            )
            assert amounts["coupon_amount"] == pytest.approx(7500)
            assert amounts["income_amount"] == pytest.approx(7500)
            assert stats["coupon_classified"] == 1
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# _infer_movement_kind — new kinds
# ---------------------------------------------------------------------------

class TestInferMovementKind:
    def _infer(self, kind_raw, desc):
        from src.iol_cli.commands_cashflow_reconcile import _infer_movement_kind
        return _infer_movement_kind(kind_raw, desc)

    def test_dividend_income_from_kind(self):
        assert self._infer("dividend_income", "") == "dividend_income"

    def test_coupon_income_from_kind(self):
        assert self._infer("coupon_income", "") == "coupon_income"

    def test_bond_amortization_from_kind(self):
        assert self._infer("bond_amortization_income", "") == "bond_amortization_income"

    def test_dividend_from_description(self):
        result = self._infer(None, "ACREDITACION DIVIDENDO AAPL.BA")
        assert result == "dividend_income"

    def test_coupon_from_description(self):
        result = self._infer(None, "PAGO DE CUPON AL30")
        assert result == "coupon_income"

    def test_amortization_from_description(self):
        result = self._infer(None, "DEVOLUCION DE CAPITAL AL30")
        assert result == "bond_amortization_income"

    def test_legacy_dividend_or_coupon_passthrough(self):
        assert self._infer("dividend_or_coupon_income", "") == "dividend_or_coupon_income"

    def test_fee_from_description(self):
        assert self._infer(None, "COMISION DE MERCADO") == "operational_fee_or_tax"
        assert self._infer(None, "IVA") == "operational_fee_or_tax"

    def test_external_deposit(self):
        assert self._infer(None, "DEPOSITO BANCARIO") == "external_deposit"


# ---------------------------------------------------------------------------
# aggregate_imported_movements — new kind routing
# ---------------------------------------------------------------------------

class TestAggregateImportedMovements:
    def _agg(self, rows, fx=None):
        from src.iol_shared.reconciliation_utils import aggregate_imported_movements
        return aggregate_imported_movements(rows, fx)

    def test_dividend_income_goes_to_internal_and_dividend(self):
        rows = [{"kind": "dividend_income", "amount": 1000, "currency": "ARS"}]
        result = self._agg(rows)
        assert result["imported_dividend_ars"] == pytest.approx(1000)
        assert result["imported_internal_ars"] == pytest.approx(1000)
        assert result["imported_external_ars"] == pytest.approx(0)

    def test_coupon_income_goes_to_internal_and_coupon(self):
        rows = [{"kind": "coupon_income", "amount": 500, "currency": "ARS"}]
        result = self._agg(rows)
        assert result["imported_coupon_ars"] == pytest.approx(500)
        assert result["imported_internal_ars"] == pytest.approx(500)

    def test_bond_amortization_goes_to_internal_and_amortization(self):
        rows = [{"kind": "bond_amortization_income", "amount": 20000, "currency": "ARS"}]
        result = self._agg(rows)
        assert result["imported_amortization_ars"] == pytest.approx(20000)
        assert result["imported_internal_ars"] == pytest.approx(20000)
        assert result["imported_external_ars"] == pytest.approx(0)

    def test_legacy_dividend_or_coupon_still_works(self):
        rows = [{"kind": "dividend_or_coupon_income", "amount": 300, "currency": "ARS"}]
        result = self._agg(rows)
        assert result["imported_dividend_ars"] == pytest.approx(300)

    def test_external_deposit_goes_to_external(self):
        rows = [{"kind": "external_deposit", "amount": 10000, "currency": "ARS"}]
        result = self._agg(rows)
        assert result["imported_external_ars"] == pytest.approx(10000)
        assert result["imported_internal_ars"] == pytest.approx(0)


# ---------------------------------------------------------------------------
# _order_to_movement_row
# ---------------------------------------------------------------------------

class TestOrderToMovementRow:
    def _convert(self, order):
        from src.iol_cli.commands_movements import _order_to_movement_row
        return _order_to_movement_row(order)

    def test_pago_de_dividendos_converts(self):
        order = {
            "numero": 12345,
            "tipo": "pago de dividendos",
            "simbolo": "AAPL",
            "fechaOperada": "2024-03-01T14:00:00",
            "montoOperado": 2500,
            "moneda": "peso_Argentino",
        }
        row = self._convert(order)
        assert row is not None
        assert row["kind"] == "dividend_income"
        assert row["symbol"] == "AAPL"
        assert row["amount"] == pytest.approx(2500)
        assert row["movement_id"] == "order:12345"

    def test_pago_de_amortizacion_converts(self):
        order = {
            "numero": 99,
            "tipo": "pago de amortizacion",
            "simbolo": "AL30",
            "fechaOperada": "2024-06-15T00:00:00",
            "montoOperado": 50000,
            "moneda": "peso_Argentino",
        }
        row = self._convert(order)
        assert row is not None
        assert row["kind"] == "bond_amortization_income"
        assert row["symbol"] == "AL30"

    def test_pago_de_renta_converts(self):
        order = {
            "numero": 77,
            "tipo": "pago de renta",
            "simbolo": "GD30",
            "fechaOperada": "2024-07-01T10:00:00",
            "montoOperado": 1800,
            "moneda": "dolar_Estadounidense",
        }
        row = self._convert(order)
        assert row is not None
        assert row["kind"] == "coupon_income"
        assert row["currency"] == "USD"

    def test_comision_converts(self):
        order = {
            "numero": 55,
            "tipo": "comision",
            "simbolo": "GGAL",
            "fechaOperada": "2024-01-20T11:00:00",
            "montoOperado": 125,
            "moneda": "peso_Argentino",
        }
        row = self._convert(order)
        assert row is not None
        assert row["kind"] == "operational_fee_or_tax"
        assert row["amount"] == pytest.approx(-125)  # fees are negative

    def test_trade_order_returns_none(self):
        order = {
            "numero": 1,
            "tipo": "compra",
            "simbolo": "GGAL",
            "fechaOperada": "2024-01-20T11:00:00",
            "montoOperado": 10000,
        }
        assert self._convert(order) is None

    def test_missing_date_returns_none(self):
        order = {
            "numero": 2,
            "tipo": "pago de dividendos",
            "simbolo": "AAPL",
            "montoOperado": 100,
        }
        assert self._convert(order) is None


# ---------------------------------------------------------------------------
# order_fees table schema
# ---------------------------------------------------------------------------

class TestOrderFeesSchema:
    def test_order_fees_table_created(self):
        tmp, conn = _conn()
        try:
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            assert "order_fees" in tables
        finally:
            conn.close()

    def test_order_fees_insert(self):
        tmp, conn = _conn()
        try:
            conn.execute(
                """
                INSERT INTO order_fees(
                    trade_order_number, fee_order_number, fee_kind,
                    symbol, amount_ars, occurred_at, linked_at_utc, link_method
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (100, 101, "comision", "GGAL", 250.0,
                 "2024-01-20T11:00:00", "2024-01-20T11:00:01Z", "auto_symbol_timestamp"),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM order_fees WHERE fee_order_number = 101").fetchone()
            assert row is not None
            assert row["fee_kind"] == "comision"
            assert row["amount_ars"] == pytest.approx(250)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# account_cash_movements symbol column
# ---------------------------------------------------------------------------

class TestCashMovementsSymbolColumn:
    def test_symbol_column_exists(self):
        tmp, conn = _conn()
        try:
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(account_cash_movements)"
            ).fetchall()}
            assert "symbol" in cols
        finally:
            conn.close()

    def test_symbol_stored_and_retrieved(self):
        tmp, conn = _conn()
        try:
            conn.execute(
                """
                INSERT INTO account_cash_movements(
                    movement_id, movement_date, currency, amount, kind,
                    symbol, description, source, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                ("mv:1", "2024-03-01", "ARS", 3000.0, "dividend_income",
                 "AAPL", "dividendo AAPL", "test", "2024-03-01T00:00:00Z"),
            )
            conn.commit()
            row = conn.execute(
                "SELECT symbol FROM account_cash_movements WHERE movement_id = 'mv:1'"
            ).fetchone()
            assert row["symbol"] == "AAPL"
        finally:
            conn.close()
