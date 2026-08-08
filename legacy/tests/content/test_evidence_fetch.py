import os
import unittest
from unittest.mock import patch

from iol_cli import evidence_fetch as ef


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = int(status_code)
        self._payload = payload
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        if self._payload is None:
            return {}
        return self._payload


class TestEvidenceFetch(unittest.TestCase):
    def setUp(self):
        ef._SEC_TICKERS_CACHE = None

    def test_fetch_sec_filings_uses_contact_headers(self):
        seen_headers = []

        def fake_get(url, timeout=10, headers=None):
            seen_headers.append(headers or {})
            if url.endswith("/files/company_tickers.json"):
                return _FakeResponse(
                    status_code=200,
                    payload={"0": {"ticker": "AAPL", "cik_str": 320193}},
                )
            if url.endswith("/submissions/CIK0000320193.json"):
                return _FakeResponse(
                    status_code=200,
                    payload={
                        "filings": {
                            "recent": {
                                "form": ["10-Q"],
                                "filingDate": ["2026-02-10"],
                                "accessionNumber": ["0000320193-26-000001"],
                            }
                        }
                    },
                )
            return _FakeResponse(status_code=404)

        with patch.dict(
            os.environ,
            {"IOL_SEC_CONTACT_EMAIL": "bot@example.com", "IOL_SEC_USER_AGENT": ""},
            clear=False,
        ):
            with patch("iol_cli.evidence_fetch.requests.get", side_effect=fake_get):
                rows, err = ef.fetch_sec_filings("AAPL", per_source_limit=1, timeout_sec=5)

        self.assertIsNone(err)
        self.assertEqual(len(rows), 1)
        self.assertIn("bot@example.com", (seen_headers[0] or {}).get("User-Agent", ""))
        self.assertEqual((seen_headers[0] or {}).get("From"), "bot@example.com")

    def test_fetch_sec_filings_returns_friendly_forbidden_error(self):
        def fake_get(url, timeout=10, headers=None):
            if url.endswith("/files/company_tickers.json"):
                return _FakeResponse(status_code=403, text="Forbidden")
            return _FakeResponse(status_code=404)

        with patch("iol_cli.evidence_fetch.requests.get", side_effect=fake_get):
            rows, err = ef.fetch_sec_filings("AAPL", per_source_limit=1, timeout_sec=5)

        self.assertEqual(rows, [])
        self.assertIsNotNone(err)
        self.assertIn("SEC_FORBIDDEN", str(err))

    def test_collect_symbol_evidence_strict_policy_uses_reuters_and_official(self):
        sec_rows = [{"symbol": "AAPL", "query": "q", "source_name": "SEC EDGAR", "source_url": "u1", "published_date": "2026-02-10", "retrieved_at_utc": "2026-02-10T00:00:00Z", "claim": "c1", "confidence": "high", "date_confidence": "high", "notes": "{}", "conflict_key": "k1"}]
        reuters_rows = [{"symbol": "AAPL", "query": "q", "source_name": "Reuters", "source_url": "u2", "published_date": "2026-02-10", "retrieved_at_utc": "2026-02-10T00:00:00Z", "claim": "c2", "confidence": "high", "date_confidence": "high", "notes": "{}", "conflict_key": "k2"}]
        with patch("iol_cli.evidence_fetch.fetch_sec_filings", return_value=(sec_rows, None)) as mock_sec:
            with patch("iol_cli.evidence_fetch.fetch_reuters_rss", return_value=(reuters_rows, None)) as mock_reuters:
                rows, errs = ef.collect_symbol_evidence(
                    "AAPL",
                    source_policy="strict_official_reuters",
                    include_official=True,
                    include_reuters=True,
                    include_sec=True,
                    include_news=True,
                )
        self.assertEqual(errs, [])
        self.assertEqual(len(rows), 2)
        self.assertEqual(mock_sec.call_count, 1)
        self.assertEqual(mock_reuters.call_count, 1)


if __name__ == "__main__":
    unittest.main()
