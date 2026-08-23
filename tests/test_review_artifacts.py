import json
from datetime import date

import pytest

from iol_review.artifacts import create_monthly_skeleton, validate_evidence, validate_proposals


def test_rejects_order_details_and_new_asset_trade():
    with pytest.raises(ValueError, match="quantities"):
        validate_proposals([{"symbol": "GGAL", "market": "argentina", "action": "hold", "quantity": 2}], {("GGAL", "argentina")})
    with pytest.raises(ValueError, match="new assets"):
        validate_proposals([{"symbol": "AAPL", "market": "estados_unidos", "action": "increase", "eligible_for_order": False}], set())


def test_fact_requires_traceable_source():
    with pytest.raises(ValueError, match="source"):
        validate_evidence([{"classification": "fact"}])


def test_simulated_review_generates_three_artifacts(tmp_path):
    context = {"as_of": "2026-08-01", "content_hash": "a" * 64, "portfolio_argentina": {"country": "argentina", "assets": []}, "portfolio_estados_unidos": {"country": "estados_unidos", "assets": []}}
    report, evidence, proposals = create_monthly_skeleton(context, tmp_path / "data", tmp_path / "reports", date(2026, 8, 1))
    assert report.exists() and evidence.exists() and proposals.exists()
    assert json.loads(proposals.read_text())["proposals"] == []
