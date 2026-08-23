from __future__ import annotations

import json
import re
import threading
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable

from iol_review.context import build_context, normalize_movements
from iol_review.gateway import ReadOnlyIOLGateway
from iol_review.profile import ProfileError, load_profile

from . import SCHEMA_VERSION
from .models import AssetContext, Capabilities, InvestmentHistory, InvestorProfileResponse, MovementList, PortfolioSnapshot
from .security import safe_document_path, sanitize, sanitize_text


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class MCPReadOnlyService:
    """The only data facade available to MCP tool functions.

    Its public API contains queries only. It intentionally does not accept or expose
    the generic IOL client used by the operational CLI.
    """

    def __init__(
        self,
        gateway_factory: Callable[[], ReadOnlyIOLGateway],
        *,
        profile_loader: Callable[[], dict[str, Any]] = load_profile,
        review_dir: Path = Path("reports/monthly"),
        decision_dir: Path = Path("data/review"),
        cache_ttl_seconds: int = 45,
    ):
        self._gateway_factory = gateway_factory
        self._profile_loader = profile_loader
        self._review_dir = review_dir
        self._decision_dir = decision_dir
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache: tuple[float, PortfolioSnapshot] | None = None
        self._lock = threading.RLock()

    def portfolio_snapshot(self, refresh: bool = False) -> PortfolioSnapshot:
        with self._lock:
            now = time.monotonic()
            if not refresh and self._cache and now - self._cache[0] < self._cache_ttl_seconds:
                return self._cache[1]
            try:
                context = sanitize(build_context(self._gateway_factory(), date.today()))
            except Exception:
                context = {
                    "as_of": date.today().isoformat(), "generated_at": _now(), "account_status": {},
                    "portfolio_argentina": {"country": "argentina", "assets": [], "cash": []},
                    "portfolio_estados_unidos": {"country": "estados_unidos", "assets": [], "cash": []},
                    "orders": [], "movements": None, "sources": {"IOL": {"status": "unavailable"}},
                    "warnings": ["IOL account context unavailable"],
                }
            snapshot = PortfolioSnapshot(
                schema_version=SCHEMA_VERSION,
                as_of=context["as_of"],
                generated_at=context["generated_at"],
                retrieved_at=_now(),
                account=context.get("account_status", {}),
                portfolios={
                    "argentina": context.get("portfolio_argentina", {"country": "argentina", "assets": [], "cash": []}),
                    "estados_unidos": context.get("portfolio_estados_unidos", {"country": "estados_unidos", "assets": [], "cash": []}),
                },
                orders=context.get("orders", []),
                movements=context.get("movements"),
                sources=context.get("sources", {}),
                warnings=context.get("warnings", []),
            )
            self._cache = (now, snapshot)
            return snapshot

    def investor_profile(self) -> InvestorProfileResponse:
        try:
            profile = sanitize(self._load_profile())
            return InvestorProfileResponse(schema_version=SCHEMA_VERSION, generated_at=_now(), profile=profile)
        except ProfileError:
            return InvestorProfileResponse(
                schema_version=SCHEMA_VERSION, generated_at=_now(), profile={}, warnings=["investor profile unavailable"]
            )

    def movements(self, from_date: date | None = None, to_date: date | None = None, country: str = "argentina") -> MovementList:
        if country not in {"argentina", "estados_unidos"}:
            raise ValueError("country must be argentina or estados_unidos")
        to_date = to_date or date.today()
        from_date = from_date or to_date - timedelta(days=90)
        if from_date > to_date or (to_date - from_date).days > 365:
            raise ValueError("movement range must be between 0 and 365 days")
        try:
            gateway = self._gateway_factory()
            value = gateway.movements(from_date, to_date, country)
            return MovementList(
                schema_version=SCHEMA_VERSION, generated_at=_now(), from_date=from_date.isoformat(), to_date=to_date.isoformat(),
                country=country, movements=sanitize(normalize_movements(value)),
            )
        except Exception:
            return MovementList(
                schema_version=SCHEMA_VERSION, generated_at=_now(), from_date=from_date.isoformat(), to_date=to_date.isoformat(),
                country=country, movements=None, warnings=["IOL movements endpoint unavailable"],
            )

    def investment_history(self, limit: int = 6) -> InvestmentHistory:
        if not 1 <= limit <= 24:
            raise ValueError("limit must be between 1 and 24")
        warnings: list[str] = []
        reviews = self._read_reviews(limit, warnings)
        decisions = self._read_decisions(limit, warnings)
        transition_plan = self._read_transition_plan(warnings)
        return InvestmentHistory(
            schema_version=SCHEMA_VERSION, generated_at=_now(), reviews=reviews, decisions=decisions,
            transition_plan=transition_plan, warnings=warnings,
        )

    def asset_context(self, symbol: str, market: str, refresh: bool = False) -> AssetContext:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol is required")
        if market not in {"argentina", "estados_unidos"}:
            raise ValueError("market must be argentina or estados_unidos")
        snapshot = self.portfolio_snapshot(refresh=refresh)
        assets = snapshot.portfolios[market].get("assets", [])
        asset = next((item for item in assets if item.get("symbol") == normalized_symbol), None)
        related_orders = [item for item in snapshot.orders if str(item.get("symbol") or "").upper() == normalized_symbol]
        related_movements = [item for item in (snapshot.movements or []) if str(item.get("symbol") or "").upper() == normalized_symbol]
        profile = self.investor_profile()
        profile_context = {
            key: profile.profile.get(key)
            for key in ("watchlist", "soft_concentration_limits", "thesis", "preferences", "notes", "allowed_markets", "allowed_instruments")
            if key in profile.profile
        }
        warnings = list(snapshot.warnings) + list(profile.warnings)
        if asset is None:
            warnings.append("asset is not a current holding in the requested market")
        return AssetContext(
            schema_version=SCHEMA_VERSION, generated_at=_now(), symbol=normalized_symbol, market=market, asset=asset,
            related_orders=related_orders, related_movements=related_movements, profile_context=profile_context,
            history=self.investment_history(), warnings=warnings,
        )

    def capabilities(self) -> Capabilities:
        return Capabilities(schema_version=SCHEMA_VERSION, generated_at=_now())

    def _read_reviews(self, limit: int, warnings: list[str]) -> list[dict[str, Any]]:
        if not self._review_dir.exists():
            return []
        entries: list[dict[str, Any]] = []
        for path in sorted(self._review_dir.glob("*-review.md"), reverse=True)[:limit]:
            try:
                text = path.read_text(encoding="utf-8")
                match = re.search(r"Review ID:\s*`?([^`\s]+)", text, re.IGNORECASE)
                status = re.search(r"Estado:\s*\*\*?([^*\n]+)", text, re.IGNORECASE)
                entries.append({
                    "review_id": match.group(1) if match else path.stem.removesuffix("-review"),
                    "review_date": path.name[:7],
                    "status": status.group(1).strip() if status else "recorded",
                    "summary": sanitize_text(text),
                })
            except OSError:
                warnings.append("a monthly review could not be read")
        return entries

    def _read_decisions(self, limit: int, warnings: list[str]) -> list[dict[str, Any]]:
        if not self._decision_dir.exists():
            return []
        entries: list[dict[str, Any]] = []
        for path in sorted(self._decision_dir.glob("*-decision.json"), reverse=True)[:limit]:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(value, dict):
                    entries.append(sanitize({
                        "review_id": value.get("review_id"), "status": value.get("status"),
                        "notes": value.get("notes"), "recorded_at": value.get("recorded_at"),
                    }))
            except (OSError, json.JSONDecodeError):
                warnings.append("a decision record could not be read")
        return entries

    def _read_transition_plan(self, warnings: list[str]) -> dict[str, Any] | None:
        try:
            profile = self._load_profile()
        except ProfileError:
            return None
        document = profile.get("continuity_document")
        if not isinstance(document, str) or not document:
            return None
        path = safe_document_path(self._review_dir, document)
        if path is None:
            warnings.append("continuity document is outside the approved monthly-review directory")
            return None
        try:
            return {"artifact_date": path.name[:7], "summary": sanitize_text(path.read_text(encoding="utf-8"))}
        except OSError:
            warnings.append("continuity document unavailable")
            return None

    def _load_profile(self) -> dict[str, Any]:
        return self._profile_loader()
