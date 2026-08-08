import unittest

from iol_web.app import app


class TestWebRoutePrefixes(unittest.TestCase):
    def test_api_routes_are_not_double_prefixed(self):
        paths = {getattr(route, "path", "") for route in app.routes}

        expected = {
            "/api/latest",
            "/api/returns",
            "/api/snapshots",
            "/api/quality",
            "/api/advisor/latest",
            "/api/reconciliation/latest",
        }
        for path in expected:
            self.assertIn(path, paths)

        unexpected = {
            "/api/api/latest",
            "/api/api/returns",
            "/api/api/snapshots",
            "/api/api/quality",
            "/api/api/advisor/latest",
            "/api/api/reconciliation/latest",
        }
        for path in unexpected:
            self.assertNotIn(path, paths)


if __name__ == "__main__":
    unittest.main()
