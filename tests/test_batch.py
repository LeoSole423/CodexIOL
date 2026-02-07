import json
import os
import sqlite3
import tempfile
import unittest

from iol_cli.batch import BatchError, _pick_price_from_quote, run_batch
from iol_cli.config import Config
from iol_cli.iol_client import IOLAPIError
from iol_cli.util import normalize_market


class FakeClient:
    def __init__(self, quotes=None, fail_buy=False):
        self.quotes = quotes or {}
        self.calls = []
        self.fail_buy = fail_buy

    def get_quote(self, market: str, symbol: str):
        self.calls.append(("get_quote", market, symbol))
        key = (market, symbol)
        if key not in self.quotes:
            raise IOLAPIError("missing quote")
        return self.quotes[key]

    def buy(self, payload, especie_d=False):
        self.calls.append(("buy", payload, especie_d))
        if self.fail_buy:
            raise IOLAPIError("buy failed")
        return {"numeroOperacion": 123}

    def sell(self, payload, especie_d=False):
        self.calls.append(("sell", payload, especie_d))
        return {"numeroOperacion": 456}

    def fci_subscribe(self, payload):
        self.calls.append(("fci_subscribe", payload))
        return {"numeroOperacion": 777}

    def fci_redeem(self, payload):
        self.calls.append(("fci_redeem", payload))
        return {"numeroOperacion": 888}


def _tmp_config(db_path: str) -> Config:
    return Config(
        username="u",
        password="p",
        base_url="http://example",
        timeout=1,
        commission_rate=0.0,
        commission_min=0.0,
        db_path=db_path,
        market_tz="America/Argentina/Buenos_Aires",
        market_close_time="18:00",
        store_raw=False,
    )


class TestBatch(unittest.TestCase):
    def test_pick_price_fast(self):
        quote = {"puntas": [{"precioCompra": 10, "precioVenta": 12}], "ultimoPrecio": 11}
        price_sell, meta_sell = _pick_price_from_quote(quote, side="sell", price_mode="fast")
        price_buy, meta_buy = _pick_price_from_quote(quote, side="buy", price_mode="fast")
        self.assertEqual(price_sell, 10.0)
        self.assertEqual(price_buy, 12.0)
        self.assertEqual(meta_sell["mode"], "fast")
        self.assertEqual(meta_buy["mode"], "fast")

    def test_validate_plan_buy_requires_exactly_one(self):
        plan = {"version": 1, "defaults": {"market": "bcba"}, "ops": [{"kind": "order", "side": "buy", "symbol": "ALUA"}]}
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json", encoding="utf-8") as f:
            json.dump(plan, f)
            plan_path = f.name
        try:
            client = FakeClient(quotes={(normalize_market("bcba"), "ALUA"): {"puntas": [{"precioCompra": 1, "precioVenta": 2}], "ultimoPrecio": 2}})
            cfg = _tmp_config(db_path=os.path.join(tempfile.gettempdir(), "iol_test_batch.db"))
            with self.assertRaises(BatchError):
                run_batch(
                    client=client,
                    config=cfg,
                    plan_path=plan_path,
                    dry_run=True,
                    price_mode_override=None,
                    default_market="bcba",
                    default_plazo="t1",
                    confirm_enabled=False,
                )
        finally:
            os.unlink(plan_path)

    def test_run_batch_dry_run_logs_and_no_trades(self):
        quote = {"puntas": [{"precioCompra": 100, "precioVenta": 101}], "ultimoPrecio": 100.5}
        plan = {
            "version": 1,
            "defaults": {"market": "bcba", "plazo": "t1", "order_type": "limit", "price_mode": "fast"},
            "ops": [{"kind": "order", "side": "sell", "symbol": "ALUA", "quantity": 1}],
        }
        with tempfile.TemporaryDirectory() as td:
            plan_path = os.path.join(td, "plan.json")
            db_path = os.path.join(td, "test.db")
            with open(plan_path, "w", encoding="utf-8") as f:
                json.dump(plan, f)
            client = FakeClient(quotes={(normalize_market("bcba"), "ALUA"): quote})
            cfg = _tmp_config(db_path=db_path)

            result = run_batch(
                client=client,
                config=cfg,
                plan_path=plan_path,
                dry_run=False,
                price_mode_override=None,
                default_market="bcba",
                default_plazo="t1",
                confirm_enabled=False,
            )
            self.assertTrue(result["dry_run"])
            self.assertEqual(len(result["ops"]), 1)
            self.assertEqual(result["ops"][0]["status"], "prepared")
            self.assertEqual(result["ops"][0]["price"], 100.0)
            self.assertTrue(all(c[0] != "sell" and c[0] != "buy" for c in client.calls))

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                run_row = conn.execute("SELECT status FROM batch_runs WHERE id=?", (result["run_id"],)).fetchone()
                self.assertEqual(run_row["status"], "dry_run")
                ops_rows = conn.execute("SELECT status FROM batch_ops WHERE run_id=? ORDER BY idx", (result["run_id"],)).fetchall()
                self.assertEqual(len(ops_rows), 1)
                self.assertEqual(ops_rows[0]["status"], "prepared")
            finally:
                conn.close()

    def test_run_batch_stop_on_error(self):
        quote = {"puntas": [{"precioCompra": 100, "precioVenta": 101}], "ultimoPrecio": 100.5}
        plan = {
            "version": 1,
            "defaults": {"market": "bcba", "plazo": "t1", "order_type": "limit", "price_mode": "fast"},
            "ops": [
                {"kind": "order", "side": "buy", "symbol": "ALUA", "quantity": 1},
                {"kind": "order", "side": "sell", "symbol": "ALUA", "quantity": 1},
            ],
        }
        with tempfile.TemporaryDirectory() as td:
            plan_path = os.path.join(td, "plan.json")
            db_path = os.path.join(td, "test.db")
            with open(plan_path, "w", encoding="utf-8") as f:
                json.dump(plan, f)
            client = FakeClient(quotes={(normalize_market("bcba"), "ALUA"): quote}, fail_buy=True)
            cfg = _tmp_config(db_path=db_path)

            with self.assertRaises(IOLAPIError):
                run_batch(
                    client=client,
                    config=cfg,
                    plan_path=plan_path,
                    dry_run=False,
                    price_mode_override=None,
                    default_market="bcba",
                    default_plazo="t1",
                    confirm_enabled=True,
                )
            # Only buy should be attempted.
            executed = [c[0] for c in client.calls if c[0] in ("buy", "sell")]
            self.assertEqual(executed, ["buy"])

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                run_row = conn.execute("SELECT status, error_message FROM batch_runs ORDER BY id DESC LIMIT 1").fetchone()
                self.assertEqual(run_row["status"], "error")
                op_rows = conn.execute("SELECT idx, status FROM batch_ops ORDER BY idx").fetchall()
                self.assertEqual(len(op_rows), 2)
                self.assertEqual(op_rows[0]["status"], "error")
                self.assertEqual(op_rows[1]["status"], "prepared")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
