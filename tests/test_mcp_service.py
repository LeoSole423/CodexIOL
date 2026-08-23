from __future__ import annotations

import json
from datetime import date

import pytest

from iol_mcp.service import MCPReadOnlyService
from iol_review.profile import ProfileError


class Gateway:
    def __init__(self):
        self.portfolio_calls = 0

    def account_status(self):
        return {"totalEnPesos": 100, "dni": "not-forwarded"}

    def portfolio(self, country):
        self.portfolio_calls += 1
        return {"activos": [{"simbolo": "GGAL", "cantidad": 2, "ultimoPrecio": 10, "valorizado": 20, "moneda": "ARS"}]}

    def orders(self):
        return [{"numero": 1, "simbolo": "GGAL", "estado": "pendiente"}]

    def optional_movements(self, as_of):
        return ([{"fecha": "2026-08-01", "simbolo": "GGAL", "importe": 20}], None)

    def movements(self, date_from, date_to, country):
        return [{"fecha": date_from.isoformat(), "simbolo": "GGAL", "importe": 20, "pais": country}]


@pytest.fixture
def profile():
    return {
        "schema_version": 1,
        "objectives": "growth",
        "horizon": "long",
        "liquidity_needs": "none",
        "loss_tolerance": "high",
        "allowed_markets": ["argentina"],
        "allowed_instruments": ["cedears"],
        "watchlist": ["GGAL"],
        "continuity_document": "reports/monthly/2026-08-transition-plan.md",
        "password": "must never leave this test fixture",
        "accessToken": "must never leave this test fixture",
    }


def test_snapshot_is_normalized_sanitized_and_cached(tmp_path, profile):
    gateway = Gateway()
    service = MCPReadOnlyService(lambda: gateway, profile_loader=lambda: profile, review_dir=tmp_path / "reports/monthly", decision_dir=tmp_path / "data/review")

    first = service.portfolio_snapshot()
    second = service.portfolio_snapshot()

    assert first.portfolios["argentina"]["assets"][0]["symbol"] == "GGAL"
    assert first.account == {"balances": [], "reported_totals": {"ARS": 100.0}}
    assert first == second
    assert gateway.portfolio_calls == 2


def test_movements_is_bounded_and_partial_failures_are_safe(tmp_path, profile):
    service = MCPReadOnlyService(lambda: Gateway(), profile_loader=lambda: profile, review_dir=tmp_path / "reports/monthly", decision_dir=tmp_path / "data/review")

    movements = service.movements(date(2026, 8, 1), date(2026, 8, 3))

    assert movements.movements[0]["symbol"] == "GGAL"
    with pytest.raises(ValueError, match="365"):
        service.movements(date(2025, 1, 1), date(2026, 8, 3))


def test_unavailable_movements_and_profile_are_reported_without_sensitive_errors(tmp_path, profile):
    class UnavailableGateway(Gateway):
        def movements(self, date_from, date_to, country):
            raise RuntimeError("IOL body that must not be exposed")

    unavailable_profile = lambda: (_ for _ in ()).throw(ProfileError("private path"))
    service = MCPReadOnlyService(lambda: UnavailableGateway(), profile_loader=unavailable_profile, review_dir=tmp_path / "reports/monthly", decision_dir=tmp_path / "data/review")

    movements = service.movements()
    public_profile = service.investor_profile()

    assert movements.movements is None
    assert movements.warnings == ["IOL movements endpoint unavailable"]
    assert public_profile.profile == {}
    assert public_profile.warnings == ["investor profile unavailable"]


def test_history_returns_review_decisions_and_safe_transition_plan(tmp_path, profile):
    review_dir = tmp_path / "reports/monthly"
    decision_dir = tmp_path / "data/review"
    review_dir.mkdir(parents=True)
    decision_dir.mkdir(parents=True)
    (review_dir / "2026-08-review.md").write_text("# Review\n\nReview ID: `review-1`\n\nEstado: **complete**\n\nThesis remains valid.", encoding="utf-8")
    (review_dir / "2026-08-transition-plan.md").write_text("# Transition\n\nKeep diversification.", encoding="utf-8")
    (decision_dir / "review-1-decision.json").write_text(json.dumps({"review_id": "review-1", "status": "accepted", "notes": "Continue", "recorded_at": "2026-08-02"}), encoding="utf-8")
    profile["continuity_document"] = str(review_dir / "2026-08-transition-plan.md")
    service = MCPReadOnlyService(lambda: Gateway(), profile_loader=lambda: profile, review_dir=review_dir, decision_dir=decision_dir)

    history = service.investment_history()

    assert history.reviews[0]["review_id"] == "review-1"
    assert history.decisions[0]["status"] == "accepted"
    assert history.transition_plan is not None


def test_asset_context_requires_explicit_market_and_never_exposes_profile_secret(tmp_path, profile):
    service = MCPReadOnlyService(lambda: Gateway(), profile_loader=lambda: profile, review_dir=tmp_path / "reports/monthly", decision_dir=tmp_path / "data/review")

    context = service.asset_context("ggal", "argentina")
    public_profile = service.investor_profile()

    assert context.asset["symbol"] == "GGAL"
    assert "password" not in context.profile_context
    assert "password" not in public_profile.profile
    assert "accessToken" not in public_profile.profile
    assert "continuity_document" not in public_profile.profile
    with pytest.raises(ValueError, match="market"):
        service.asset_context("GGAL", "ambiguous")

    unknown = service.asset_context("UNKNOWN", "argentina")
    assert unknown.asset is None
    assert "asset is not a current holding in the requested market" in unknown.warnings
