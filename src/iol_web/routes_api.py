from __future__ import annotations

from fastapi import APIRouter

from .api_advisor import advisor_history, advisor_latest, advisor_opportunities_latest, router as advisor_router
from .api_engines import router as engines_router
from .api_simulation import router as simulation_router
from .api_cashflows import build_cashflows_router
from .api_inflation import build_inflation_router
from .api_portfolio import allocation, assets_performance, latest, movers, router as portfolio_router
from .api_quality import build_quality_router
from .api_reconciliation import (
    reconciliation_apply,
    reconciliation_dismiss,
    reconciliation_interval,
    reconciliation_latest,
    reconciliation_open,
    router as reconciliation_router,
)
from .api_returns import build_returns_router
from .flow_utils import (
    annotate_flow_rows,
    compute_interval_flow_v2,
    parse_date,
    return_with_flows,
    snapshot_data_freshness,
)
from .inflation_ar import get_inflation_series
from .metrics import compute_return

router = APIRouter(prefix="/api")
router.include_router(advisor_router)
router.include_router(portfolio_router)
router.include_router(reconciliation_router)

cashflows_router, cashflows_auto, cashflows_manual, cashflows_manual_add, cashflows_manual_delete = build_cashflows_router(
    parse_date=parse_date,
    compute_interval_flow=compute_interval_flow_v2,
    annotate_flow_rows=annotate_flow_rows,
)
router.include_router(cashflows_router)

returns_router, health, snapshots, returns = build_returns_router(
    compute_interval_flow=compute_interval_flow_v2,
    annotate_flow_rows=annotate_flow_rows,
    return_with_flows=return_with_flows,
)
router.include_router(returns_router)

inflation_router, inflation, kpi_monthly_vs_inflation, compare_inflation, compare_inflation_series, compare_inflation_annual = build_inflation_router(
    parse_date=parse_date,
    compute_return=compute_return,
    return_with_flows=return_with_flows,
    get_inflation_series=lambda **kwargs: get_inflation_series(**kwargs),
)
router.include_router(inflation_router)

quality_router, quality = build_quality_router(
    returns_fn=lambda: returns(),
    monthly_kpi_fn=lambda: kpi_monthly_vs_inflation(),
    snapshot_data_freshness_fn=snapshot_data_freshness,
)
router.include_router(quality_router)
router.include_router(engines_router)
router.include_router(simulation_router)
