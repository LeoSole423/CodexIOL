# Backward-compat shim — advisor context logic now lives in iol_advisor.advisor_context.
# Internal CLI modules import directly from iol_advisor; this shim preserves
# the old public path for any external code or tests that still reference it.
from iol_advisor.advisor_context import (  # noqa: F401
    ReturnBlock,
    allocation,
    assets_for_snapshot,
    build_advisor_context,
    build_advisor_context_from_db_path,
    build_union_movers,
    compute_return,
    earliest_snapshot,
    first_snapshot_in_range,
    first_snapshot_of_year,
    last_snapshot_in_range,
    latest_snapshot,
    render_advisor_context_md,
    snapshot_before,
    snapshot_on_or_before,
    snapshots_series,
    target_date,
)
