from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(str(v).strip())
    except Exception:
        return int(default)


def _resolve_path(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return os.path.abspath(os.path.join(os.getcwd(), "data", "cache", "inflation_ipc_ar.json"))
    if os.path.isabs(p):
        return p
    return os.path.abspath(os.path.join(os.getcwd(), p))


DEFAULT_SERIES_ID = "145.3_INGNACUAL_DICI_M_38"
DEFAULT_API_BASE = "https://apis.datos.gob.ar/series/api"
DEFAULT_CACHE_PATH = os.path.join("data", "cache", "inflation_ipc_ar.json")


@dataclass(frozen=True)
class InflationFetchResult:
    series_id: str
    fetched_at: float
    stale: bool
    # Raw API data points: [["YYYY-MM-01", value_decimal], ...]
    data: List[Tuple[str, float]]
    source: str

    def inflation_pct_by_month(self) -> Dict[str, float]:
        """
        Returns month -> inflation percentage.

        This series returns a monthly variation as a decimal (e.g. 0.206 = 20.6%).
        """
        out: Dict[str, float] = {}
        for d, v in self.data or []:
            month = str(d)[:7]
            try:
                out[month] = float(v) * 100.0
            except Exception:
                continue
        return out


def _read_cache(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_cache(path: str, payload: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Use a unique temp file name to avoid cross-request races.
    tmp = str(p) + f".{os.getpid()}.{int(time.time() * 1000)}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, sort_keys=True)
    try:
        os.replace(tmp, str(p))
    except Exception:
        # Best-effort fallback for filesystems where atomic replace may be flaky.
        try:
            with open(tmp, "r", encoding="utf-8") as src, open(str(p), "w", encoding="utf-8") as dst:
                dst.write(src.read())
        finally:
            try:
                os.unlink(tmp)
            except Exception:
                pass


def _parse_data_points(rows: Any) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for r in rows or []:
        if not isinstance(r, (list, tuple)) or len(r) < 2:
            continue
        d = r[0]
        v = r[1]
        if d is None or v is None:
            continue
        try:
            out.append((str(d), float(v)))
        except Exception:
            continue
    return out


def _fetch_from_api(
    series_id: str,
    start_date: Optional[str],
    end_date: Optional[str],
    api_base: str,
    timeout_sec: int,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {"ids": series_id}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    url = api_base.rstrip("/") + "/series"
    r = requests.get(url, params=params, timeout=timeout_sec)
    r.raise_for_status()
    return r.json()


def get_inflation_series(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> InflationFetchResult:
    """
    Fetches Argentina monthly inflation (IPC INDEC) from datos.gob.ar with a local cache.

    Default series is the INDEC monthly variation for IPC Nivel General Nacional:
      145.3_INGNACUAL_DICI_M_38

    The API returns monthly variation as a decimal; we convert to percent in helpers.
    """
    series_id = _env_str("IOL_INFLATION_SERIES_ID", DEFAULT_SERIES_ID)
    api_base = _env_str("IOL_INFLATION_API_BASE", DEFAULT_API_BASE)
    cache_path = _resolve_path(_env_str("IOL_INFLATION_CACHE_PATH", DEFAULT_CACHE_PATH))
    ttl_sec = _env_int("IOL_INFLATION_CACHE_TTL_SEC", 12 * 60 * 60)
    timeout_sec = _env_int("IOL_INFLATION_HTTP_TIMEOUT_SEC", 10)

    now = time.time()
    cached = _read_cache(cache_path)

    def _cache_ok_for_range(c: Dict[str, Any]) -> bool:
        pts = _parse_data_points(c.get("data"))
        if not pts:
            return False
        dates = [d for d, _ in pts]
        lo = min(dates)
        hi = max(dates)
        if start_date and lo > start_date:
            return False
        if end_date and hi < end_date:
            return False
        return True

    if cached and isinstance(cached, dict):
        fetched_at = float(cached.get("fetched_at") or 0.0)
        is_fresh = (now - fetched_at) <= float(ttl_sec)
        if is_fresh and _cache_ok_for_range(cached):
            pts = _parse_data_points(cached.get("data"))
            return InflationFetchResult(
                series_id=str(cached.get("series_id") or series_id),
                fetched_at=fetched_at,
                stale=False,
                data=pts,
                source=str(cached.get("source") or "cache"),
            )

    # Cache missing/expired/insufficient: try fetching.
    try:
        payload = _fetch_from_api(series_id, start_date, end_date, api_base=api_base, timeout_sec=timeout_sec)
        pts = _parse_data_points(payload.get("data"))
        out_payload = {
            "series_id": series_id,
            "fetched_at": now,
            "source": "apis.datos.gob.ar (INDEC)",
            "start_date": start_date,
            "end_date": end_date,
            "data": [[d, v] for d, v in pts],
        }
        _write_cache(cache_path, out_payload)
        return InflationFetchResult(
            series_id=series_id,
            fetched_at=now,
            stale=False,
            data=pts,
            source="apis.datos.gob.ar (INDEC)",
        )
    except Exception:
        # Best-effort fallback to stale cache if available.
        if cached and isinstance(cached, dict) and _cache_ok_for_range(cached):
            fetched_at = float(cached.get("fetched_at") or 0.0)
            pts = _parse_data_points(cached.get("data"))
            return InflationFetchResult(
                series_id=str(cached.get("series_id") or series_id),
                fetched_at=fetched_at,
                stale=True,
                data=pts,
                source=str(cached.get("source") or "cache"),
            )
        raise
