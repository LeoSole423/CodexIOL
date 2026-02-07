import unittest

from iol_web.movers import build_union_movers


class TestUnionMovers(unittest.TestCase):
    def test_union_base_only_symbol(self):
        base = [{"symbol": "AAA", "description": "A", "total_value": 100.0}]
        end = []
        out = build_union_movers(base, end)
        row = next(r for r in out if r["symbol"] == "AAA")
        self.assertEqual(row["total_value"], 0.0)
        self.assertEqual(row["base_total_value"], 100.0)
        self.assertEqual(row["delta_value"], -100.0)
        self.assertAlmostEqual(row["delta_pct"], -100.0)

    def test_union_end_only_symbol(self):
        base = []
        end = [{"symbol": "BBB", "description": "B", "total_value": 50.0}]
        out = build_union_movers(base, end)
        row = next(r for r in out if r["symbol"] == "BBB")
        self.assertEqual(row["total_value"], 50.0)
        self.assertEqual(row["base_total_value"], 0.0)
        self.assertEqual(row["delta_value"], 50.0)
        self.assertIsNone(row["delta_pct"])

    def test_union_both_symbol(self):
        base = [{"symbol": "CCC", "description": "C", "total_value": 80.0}]
        end = [{"symbol": "CCC", "description": "C2", "total_value": 100.0}]
        out = build_union_movers(base, end)
        row = next(r for r in out if r["symbol"] == "CCC")
        self.assertEqual(row["description"], "C2")
        self.assertEqual(row["total_value"], 100.0)
        self.assertEqual(row["base_total_value"], 80.0)
        self.assertEqual(row["delta_value"], 20.0)
        self.assertAlmostEqual(row["delta_pct"], 25.0)


if __name__ == "__main__":
    unittest.main()

