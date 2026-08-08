from __future__ import annotations

from typing import Dict, List, Optional, Tuple


def month_key(date_str: str) -> str:
    return str(date_str)[:7]


def _month_to_int(m: str) -> int:
    y = int(m[:4])
    mm = int(m[5:7])
    return y * 12 + (mm - 1)


def _int_to_month(n: int) -> str:
    y = n // 12
    m = (n % 12) + 1
    return f"{y:04d}-{m:02d}"


def iter_months(from_month: str, to_month: str) -> List[str]:
    a = _month_to_int(from_month)
    b = _month_to_int(to_month)
    if a > b:
        return []
    return [_int_to_month(i) for i in range(a, b + 1)]


def _next_month(m: str) -> str:
    return _int_to_month(_month_to_int(m) + 1)


def compounded_inflation_pct(
    from_date: str,
    to_date: str,
    infl_pct_by_month: Dict[str, float],
    projection_month: Optional[str] = None,
    projection_pct: Optional[float] = None,
) -> Tuple[Optional[float], List[str], List[str]]:
    """
    Compounded inflation percentage aligned to the portfolio return interval.

    months used: (month(from_date) + 1 .. month(to_date)) inclusive.
    If any required month is missing (and not projected), returns None.
    """
    fm = month_key(from_date)
    tm = month_key(to_date)
    start = _next_month(fm)
    months = iter_months(start, tm)

    factor = 1.0
    used: List[str] = []
    projected: List[str] = []
    missing = False
    for m in months:
        pct = infl_pct_by_month.get(m)
        if pct is None and projection_month and projection_pct is not None and m == projection_month:
            pct = float(projection_pct)
            projected.append(m)
        if pct is None:
            missing = True
            continue
        used.append(m)
        factor *= (1.0 + float(pct) / 100.0)

    if missing:
        return None, used, projected
    return (factor - 1.0) * 100.0, used, projected


def inflation_factor_for_date(
    base_date: str,
    target_date: str,
    infl_pct_by_month: Dict[str, float],
    projection_month: Optional[str] = None,
    projection_pct: Optional[float] = None,
) -> Optional[float]:
    """
    Returns compounded inflation factor from base_date to target_date (step-monthly).

    If any required month is missing (and not projected), returns None.
    """
    pct, _, _ = compounded_inflation_pct(
        from_date=base_date,
        to_date=target_date,
        infl_pct_by_month=infl_pct_by_month,
        projection_month=projection_month,
        projection_pct=projection_pct,
    )
    if pct is None:
        return None
    return 1.0 + float(pct) / 100.0

