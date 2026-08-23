from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any


VALID_ACTIONS = {"hold", "increase", "reduce", "sell", "research"}
VALID_CLAIMS = {"fact", "inference", "forecast", "preference"}


def validate_evidence(evidence: list[dict[str, Any]]) -> None:
    for item in evidence:
        if item.get("classification") not in VALID_CLAIMS:
            raise ValueError("invalid evidence classification")
        if item["classification"] == "fact":
            for field in ("source", "url", "published_at", "accessed_at", "related_asset"):
                if not item.get(field):
                    raise ValueError(f"fact evidence requires {field}")


def validate_proposals(proposals: list[dict[str, Any]], holdings: set[tuple[str, str]]) -> None:
    forbidden = {"quantity", "amount", "ticket_id", "payload", "order"}
    for item in proposals:
        if item.get("action") not in VALID_ACTIONS:
            raise ValueError("invalid proposal action")
        if forbidden & set(item):
            raise ValueError("proposals cannot contain order quantities, tickets, or payloads")
        key = (str(item.get("symbol", "")).upper(), str(item.get("market", "")))
        if key not in holdings and (item.get("action") != "research" or item.get("eligible_for_order") is not False):
            raise ValueError("new assets must be research and not eligible for order")
        if item.get("eligible_for_order") is True:
            raise ValueError("monthly review proposals are never eligible for order")
        for field in ("review_id", "symbol", "market", "thesis", "evidence", "confidence", "horizon", "risks", "invalidation_conditions", "eligible_for_order"):
            if field not in item:
                raise ValueError(f"proposal requires {field}")


def create_monthly_skeleton(context: dict[str, Any], output_dir: Path, report_dir: Path, review_date: date | None = None) -> tuple[Path, Path, Path]:
    review_date = review_date or date.fromisoformat(context["as_of"])
    prefix = review_date.strftime("%Y-%m")
    review_id = f"{prefix}-{context['content_hash'][:12]}"
    output_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)
    evidence: list[dict[str, Any]] = []
    proposals: list[dict[str, Any]] = []
    holdings = {(asset["symbol"], portfolio["country"]) for portfolio in (context.get("portfolio_argentina", {}), context.get("portfolio_estados_unidos", {})) for asset in portfolio.get("assets", [])}
    validate_evidence(evidence)
    validate_proposals(proposals, holdings)
    evidence_path = output_dir / f"{prefix}-evidence.json"
    proposals_path = output_dir / f"{prefix}-proposals.json"
    report_path = report_dir / f"{prefix}-review.md"
    evidence_path.write_text(json.dumps({"schema_version": 1, "review_id": review_id, "status": "incomplete", "claims": evidence}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    proposals_path.write_text(json.dumps({"schema_version": 1, "review_id": review_id, "status": "incomplete", "proposals": proposals}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(f"# Revisión mensual {prefix}\n\nEstado: **incomplete**. Pendiente de investigación de Codex; no hay órdenes ni tickets.\n\nReview ID: `{review_id}`\n", encoding="utf-8")
    return report_path, evidence_path, proposals_path
