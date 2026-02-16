import unittest

from iol_web.movers import build_union_movers_pnl


class TestUnionMoversPnL(unittest.TestCase):
    def test_sold_position_uses_sell_proceeds(self):
        base = [{"symbol": "AAA", "description": "A", "total_value": 100.0}]
        end = []
        cashflows = {"AAA": {"buy_amount": 0.0, "sell_amount": 120.0}}
        out = build_union_movers_pnl(base, end, cashflows)
        row = next(r for r in out if r["symbol"] == "AAA")
        self.assertEqual(row["total_value"], 0.0)
        self.assertEqual(row["base_total_value"], 100.0)
        self.assertAlmostEqual(row["delta_value"], 20.0)
        self.assertAlmostEqual(row["delta_pct"], 20.0)
        self.assertTrue(row["closed_position"])
        self.assertTrue(row["liquidated_to_cash"])
        self.assertFalse(row["cashflow_missing_for_close"])
        self.assertEqual(row["flow_tag"], "liquidated")

    def test_roundtrip_buy_sell_flat_end_value(self):
        base = []
        end = []
        cashflows = {"BBB": {"buy_amount": 100.0, "sell_amount": 110.0}}
        out = build_union_movers_pnl(base, end, cashflows)
        row = next(r for r in out if r["symbol"] == "BBB")
        self.assertEqual(row["total_value"], 0.0)
        self.assertEqual(row["base_total_value"], 0.0)
        self.assertAlmostEqual(row["delta_value"], 10.0)
        self.assertAlmostEqual(row["delta_pct"], 10.0)

    def test_no_orders_falls_back_to_mark_to_market(self):
        base = [{"symbol": "CCC", "description": "C", "total_value": 100.0}]
        end = [{"symbol": "CCC", "description": "C", "total_value": 80.0}]
        out = build_union_movers_pnl(base, end, {})
        row = next(r for r in out if r["symbol"] == "CCC")
        self.assertAlmostEqual(row["delta_value"], -20.0)
        self.assertAlmostEqual(row["delta_pct"], -20.0)
        self.assertFalse(row["closed_position"])
        self.assertEqual(row["flow_tag"], "none")

    def test_closed_position_without_cashflow_is_flagged(self):
        base = [{"symbol": "DDD", "description": "D", "total_value": 100.0}]
        end = []
        out = build_union_movers_pnl(base, end, {})
        row = next(r for r in out if r["symbol"] == "DDD")
        self.assertAlmostEqual(row["delta_value"], -100.0)
        self.assertAlmostEqual(row["delta_pct"], -100.0)
        self.assertTrue(row["closed_position"])
        self.assertFalse(row["liquidated_to_cash"])
        self.assertTrue(row["cashflow_missing_for_close"])
        self.assertEqual(row["flow_tag"], "missing_cashflow")

    def test_includes_symbols_seen_only_in_orders(self):
        base = []
        end = []
        cashflows = {"ZZZ": {"buy_amount": 50.0, "sell_amount": 60.0}}
        out = build_union_movers_pnl(base, end, cashflows)
        row = next(r for r in out if r["symbol"] == "ZZZ")
        self.assertAlmostEqual(row["delta_value"], 10.0)
        self.assertAlmostEqual(row["delta_pct"], 20.0)


if __name__ == "__main__":
    unittest.main()
