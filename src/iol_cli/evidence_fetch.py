from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import os
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import requests


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _safe_iso_date_from_pub(v: Any) -> Optional[str]:
    s = _safe_str(v)
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        return dt.date().isoformat()
    except Exception:
        return None


def _norm_symbol(v: str) -> str:
    return _safe_str(v).upper()


def _default_http_headers() -> Dict[str, str]:
    return {"User-Agent": "iol-cli-evidence/1.0 (+local)"}


def _sec_http_headers() -> Dict[str, str]:
    # SEC Fair Access asks bots to identify themselves with contact info.
    ua = _safe_str(os.getenv("IOL_SEC_USER_AGENT"))
    contact = _safe_str(os.getenv("IOL_SEC_CONTACT_EMAIL"))
    if not ua:
        if contact:
            ua = f"CodexIOL/1.0 ({contact})"
        else:
            ua = "CodexIOL/1.0 (contact: sec-bot@example.com)"
    headers = {"User-Agent": ua, "Accept-Encoding": "gzip, deflate"}
    if contact:
        headers["From"] = contact
    return headers


def fetch_google_news_rss(symbol: str, per_source_limit: int, timeout_sec: int = 10) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    sym = _norm_symbol(symbol)
    if not sym:
        return [], None
    q = f"{sym} stock OR etf"
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    headers = _default_http_headers()
    try:
        resp = requests.get(url, timeout=timeout_sec, headers=headers)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception as exc:
        return [], f"google_news_error: {exc}"

    out: List[Dict[str, Any]] = []
    now = _now_iso()
    channel = root.find("channel")
    if channel is None:
        return [], None
    for item in channel.findall("item")[: max(0, int(per_source_limit))]:
        title = _safe_str(item.findtext("title"))
        link = _safe_str(item.findtext("link"))
        pub = _safe_iso_date_from_pub(item.findtext("pubDate"))
        if not title or not link:
            continue
        out.append(
            {
                "symbol": sym,
                "query": q,
                "source_name": "Google News RSS",
                "source_url": link,
                "published_date": pub,
                "retrieved_at_utc": now,
                "claim": title,
                "confidence": "medium",
                "date_confidence": "high" if pub else "low",
                "notes": "Auto-ingested news headline",
                "conflict_key": f"{sym}:news",
            }
        )
    return out, None


_SEC_TICKERS_CACHE: Optional[Dict[str, Dict[str, Any]]] = None


def _load_sec_tickers(timeout_sec: int = 10) -> Dict[str, Dict[str, Any]]:
    global _SEC_TICKERS_CACHE
    if _SEC_TICKERS_CACHE is not None:
        return _SEC_TICKERS_CACHE
    headers = _sec_http_headers()
    resp = requests.get("https://www.sec.gov/files/company_tickers.json", timeout=timeout_sec, headers=headers)
    if resp.status_code == 403:
        raise RuntimeError(
            "SEC_FORBIDDEN: configure IOL_SEC_CONTACT_EMAIL or IOL_SEC_USER_AGENT with contact info for SEC access"
        )
    resp.raise_for_status()
    raw = resp.json()
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw, dict):
        for _, v in raw.items():
            if not isinstance(v, dict):
                continue
            t = _safe_str(v.get("ticker")).upper()
            if not t:
                continue
            out[t] = v
    _SEC_TICKERS_CACHE = out
    return out


def fetch_sec_filings(symbol: str, per_source_limit: int, timeout_sec: int = 10) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    sym = _norm_symbol(symbol)
    if not sym:
        return [], None
    headers = _sec_http_headers()
    try:
        tickers = _load_sec_tickers(timeout_sec=timeout_sec)
        info = tickers.get(sym)
        if not info:
            return [], None
        cik = int(info.get("cik_str"))
        cik10 = f"{cik:010d}"
        sub_url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
        resp = requests.get(sub_url, timeout=timeout_sec, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        return [], f"sec_error: {exc}"

    recent = (data.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    filing_dates = recent.get("filingDate") or []
    accession = recent.get("accessionNumber") or []
    n = min(len(forms), len(filing_dates), len(accession), max(0, int(per_source_limit)))
    out: List[Dict[str, Any]] = []
    now = _now_iso()
    browse_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={sym}&owner=exclude&count=40"
    for i in range(n):
        form = _safe_str(forms[i])
        fdate = _safe_str(filing_dates[i]) or None
        acc = _safe_str(accession[i])
        claim = f"SEC filing {form} on {fdate}" if fdate else f"SEC filing {form}"
        out.append(
            {
                "symbol": sym,
                "query": f"{sym} SEC filings",
                "source_name": "SEC EDGAR",
                "source_url": browse_url,
                "published_date": fdate,
                "retrieved_at_utc": now,
                "claim": claim + (f" (accession {acc})" if acc else ""),
                "confidence": "high",
                "date_confidence": "high" if fdate else "low",
                "notes": "Auto-ingested SEC filing metadata",
                "conflict_key": f"{sym}:sec",
            }
        )
    return out, None


def collect_symbol_evidence(
    symbol: str,
    per_source_limit: int = 2,
    include_news: bool = True,
    include_sec: bool = True,
    timeout_sec: int = 10,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    out: List[Dict[str, Any]] = []
    errs: List[str] = []
    if include_sec:
        rows, err = fetch_sec_filings(symbol, per_source_limit=per_source_limit, timeout_sec=timeout_sec)
        out.extend(rows)
        if err:
            errs.append(err)
    if include_news:
        rows, err = fetch_google_news_rss(symbol, per_source_limit=per_source_limit, timeout_sec=timeout_sec)
        out.extend(rows)
        if err:
            errs.append(err)

    dedup = {}
    for r in out:
        k = (
            _safe_str(r.get("symbol")),
            _safe_str(r.get("source_name")),
            _safe_str(r.get("source_url")),
            _safe_str(r.get("claim")),
            _safe_str(r.get("published_date")),
        )
        dedup[k] = r
    return list(dedup.values()), errs
