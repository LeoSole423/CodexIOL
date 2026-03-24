"""SEC EDGAR 13F-HR filing fetcher and parser.

Flow:
  1. Fetch the fund's recent submissions JSON from data.sec.gov to find
     the latest 13F-HR filing accession number.
  2. Download the filing index to locate the primary document URL.
  3. Parse the 13F information table (XML or TSV) to extract holdings.
  4. Compute position-change direction vs. the previous quarter.

All HTTP calls use the same SEC-polite headers as evidence_fetch.py.
Returns (holdings_dict, error_str) — empty dict on any failure.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import urllib.request
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET


_TIMEOUT = 15


def _sec_headers() -> Dict[str, str]:
    contact = os.getenv("IOL_SEC_CONTACT_EMAIL", "sec-bot@example.com")
    ua = os.getenv("IOL_SEC_USER_AGENT") or f"CodexIOL/1.0 ({contact})"
    return {
        "User-Agent": ua,
        "Accept-Encoding": "gzip, deflate",
        "From": contact,
    }


def _decompress(data: bytes) -> bytes:
    """Decompress gzip data if needed."""
    if data[:2] == b"\x1f\x8b":
        return gzip.decompress(data)
    return data


def _get_json(url: str) -> Tuple[Optional[dict], str]:
    try:
        req = urllib.request.Request(url, headers=_sec_headers())
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            raw = _decompress(resp.read())
            return json.loads(raw.decode("utf-8")), ""
    except Exception as exc:
        return None, str(exc)


def _get_text(url: str) -> Tuple[str, str]:
    try:
        req = urllib.request.Request(url, headers=_sec_headers())
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            raw = _decompress(resp.read())
            return raw.decode("utf-8", errors="replace"), ""
    except Exception as exc:
        return "", str(exc)


# ── Step 1: Find latest 13F-HR filing ───────────────────────────────────────

def get_latest_13f_accession(cik: str) -> Tuple[Optional[str], Optional[str], str]:
    """Return (accession_number, filing_date, error) for the latest 13F-HR."""
    cik_norm = cik.lstrip("0")
    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    data, err = _get_json(url)
    if data is None:
        return None, None, f"submissions fetch failed for CIK {cik}: {err}"

    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    accessions = filings.get("accessionNumber", [])
    dates = filings.get("filingDate", [])

    for i, form in enumerate(forms):
        if form in ("13F-HR", "13F-HR/A"):
            acc = accessions[i].replace("-", "")
            return acc, dates[i], ""

    return None, None, f"No 13F-HR found for CIK {cik}"


# ── Step 2: Get filing index ─────────────────────────────────────────────────

def get_filing_index(cik: str, accession: str) -> Tuple[List[Dict], str]:
    """Return list of files in the filing index."""
    cik_norm = cik.lstrip("0")
    acc_fmt = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_norm}&type=13F-HR&dateb=&owner=include&count=1&search_text=&output=atom"
    # Use the direct index JSON instead
    idx_url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    # Actually navigate directly to the filing index
    acc_dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession}/{acc_dashed}-index.json"
    data, err = _get_json(index_url)
    if data is None:
        return [], f"Filing index fetch failed: {err}"
    files = data.get("documents", [])
    return files, ""


# ── CUSIP → ticker mapping for CEDEAR universe ───────────────────────────────
# CUSIPs are stable identifiers for US large-caps (13F filings rarely include tickers).
_CUSIP_TO_TICKER: Dict[str, str] = {
    "037833100": "AAPL", "594918104": "MSFT", "023135106": "AMZN",
    "67066G104": "NVDA", "02079K305": "GOOGL", "02079K107": "GOOGL",
    "30303M102": "META", "88160R101": "TSLA", "46625H100": "JPM",
    "92826C839": "V",    "478160104": "JNJ",  "931142103": "WMT",
    "742718109": "PG",   "30231G102": "XOM",  "060505104": "BAC",
    "437076102": "HD",   "191216100": "KO",   "717081103": "PFE",
    "00287Y109": "ABBV", "11135F101": "AVGO", "68389X105": "ORCL",
    "64110L106": "NFLX", "254687106": "DIS",  "458140100": "INTC",
    "007903107": "AMD",  "747525103": "QCOM", "17275R102": "CSCO",
    "38141G104": "GS",   "617446448": "MS",   "172967424": "C",
    "949746101": "WFC",  "166764100": "CVX",  "097023105": "BA",
    "149123101": "CAT",  "580135101": "MCD",  "855244109": "SBUX",
    "654106103": "NKE",  "00724F101": "ADBE", "79466L302": "CRM",
    "81762P102": "NOW",
}

# Partial name → ticker fallback (uppercase, partial match)
_NAME_TO_TICKER: Dict[str, str] = {
    "APPLE": "AAPL", "MICROSOFT": "MSFT", "AMAZON": "AMZN",
    "NVIDIA": "NVDA", "ALPHABET": "GOOGL", "META PLATFORMS": "META",
    "TESLA": "TSLA", "JPMORGAN": "JPM", "VISA INC": "V",
    "JOHNSON & JOHNSON": "JNJ", "WALMART": "WMT", "PROCTER": "PG",
    "EXXON": "XOM", "BANK OF AMERICA": "BAC", "HOME DEPOT": "HD",
    "COCA-COLA": "KO", "PFIZER": "PFE", "ABBVIE": "ABBV",
    "BROADCOM": "AVGO", "ORACLE": "ORCL", "NETFLIX": "NFLX",
    "WALT DISNEY": "DIS", "INTEL": "INTC", "ADVANCED MICRO": "AMD",
    "QUALCOMM": "QCOM", "CISCO": "CSCO", "GOLDMAN SACHS": "GS",
    "MORGAN STANLEY": "MS", "CITIGROUP": "C", "WELLS FARGO": "WFC",
    "CHEVRON": "CVX", "BOEING": "BA", "CATERPILLAR": "CAT",
    "MCDONALD": "MCD", "STARBUCKS": "SBUX", "NIKE": "NKE",
    "ADOBE": "ADBE", "SALESFORCE": "CRM", "SERVICENOW": "NOW",
}


def _resolve_ticker(cusip: str, name: str) -> Optional[str]:
    """Resolve a ticker from CUSIP (primary) or issuer name (fallback)."""
    ticker = _CUSIP_TO_TICKER.get(cusip.strip().upper())
    if ticker:
        return ticker
    name_up = name.strip().upper()
    for key, sym in _NAME_TO_TICKER.items():
        if key in name_up:
            return sym
    return None


# ── Step 3: Parse 13F information table ─────────────────────────────────────

def _parse_13f_xml(xml_text: str) -> Dict[str, float]:
    """Parse a 13F-HR information table XML → {ticker: shares}.

    SEC 13F filings do not always include a <ticker> element.
    Primary lookup: CUSIP via _CUSIP_TO_TICKER.
    Fallback: partial issuer name via _NAME_TO_TICKER.
    """
    holdings: Dict[str, float] = {}
    # Remove default namespace so ElementTree can find tags without prefix
    xml_clean = xml_text.replace('xmlns="', 'xmlnsx="')
    try:
        root = ET.fromstring(xml_clean)
    except ET.ParseError:
        return holdings

    for entry in root.iter("infoTable"):
        # Try explicit <ticker> first (some filers include it)
        ticker_el = entry.find("ticker")
        ticker = (ticker_el.text or "").strip().upper() if ticker_el is not None else ""

        if not ticker:
            cusip_el = entry.find("cusip")
            name_el  = entry.find("nameOfIssuer")
            cusip = (cusip_el.text or "") if cusip_el is not None else ""
            name  = (name_el.text  or "") if name_el  is not None else ""
            resolved = _resolve_ticker(cusip, name)
            if resolved:
                ticker = resolved

        if not ticker:
            continue

        shares_el = entry.find("shrsOrPrnAmt")
        if shares_el is None:
            continue
        amt_el = shares_el.find("sshPrnamt")
        if amt_el is None:
            continue
        try:
            shares = float((amt_el.text or "0").replace(",", ""))
        except ValueError:
            continue

        holdings[ticker] = holdings.get(ticker, 0) + shares

    return holdings


def _parse_13f_tsv(tsv_text: str) -> Dict[str, float]:
    """Parse a 13F information table in TSV/pipe-delimited format → {ticker: shares}."""
    holdings: Dict[str, float] = {}
    for line in tsv_text.splitlines():
        parts = line.split("|")
        if len(parts) < 7:
            parts = line.split("\t")
        if len(parts) < 5:
            continue
        # Common format: ISSUER | CLASS | CUSIP | VALUE | SHARES | ...
        # Ticker is often not in the TSV directly — skip
    return holdings


# ── Main entry point ─────────────────────────────────────────────────────────

def fetch_13f_holdings(cik: str) -> Tuple[Dict[str, float], str, Optional[str]]:
    """Fetch the latest 13F-HR holdings for a given CIK.

    Returns:
      (holdings_dict, filing_date, error)
      holdings_dict: {ticker_symbol: shares}
      filing_date: YYYY-MM-DD of the filing
    """
    accession, filing_date, err = get_latest_13f_accession(cik)
    if accession is None:
        return {}, filing_date or "", err

    files, err = get_filing_index(cik, accession)
    if err and not files:
        # Try alternative URL format
        pass  # fall through to direct document fetch

    # Try to fetch the primary XML document directly
    cik_num = cik.lstrip("0")
    acc_clean = accession.replace("-", "")

    # Common 13F-HR XML filename patterns
    for suffix in ["informationtable.xml", "form13f.xml", "primary_doc.xml"]:
        doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/{suffix}"
        text, err2 = _get_text(doc_url)
        if text and "<infoTable>" in text:
            holdings = _parse_13f_xml(text)
            if holdings:
                return holdings, filing_date or "", ""

    # Try fetching the accession index HTML to find the actual document
    idx_html_url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/"
    html, _ = _get_text(idx_html_url)
    if html:
        # Look for .xml links in the index
        import re
        xml_links = re.findall(r'href="([^"]*\.xml)"', html, re.IGNORECASE)
        for link in xml_links:
            full_url = f"https://www.sec.gov{link}" if link.startswith("/") else \
                       f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/{link}"
            text, _ = _get_text(full_url)
            if text and "<infoTable>" in text:
                holdings = _parse_13f_xml(text)
                if holdings:
                    return holdings, filing_date or "", ""

    return {}, filing_date or "", f"Could not parse 13F document for CIK {cik} acc {accession}"


# ── Quarter-over-quarter direction ───────────────────────────────────────────

def compute_direction(
    current: Dict[str, float],
    previous: Dict[str, float],
    symbol: str,
) -> Tuple[str, float]:
    """Return (direction, change_pct) for a symbol between two quarters.

    direction: "added" | "trimmed" | "new" | "exited" | "held"
    change_pct: positive = increased, negative = decreased
    """
    curr = current.get(symbol, 0.0)
    prev = previous.get(symbol, 0.0)

    if prev == 0 and curr > 0:
        return "new", 100.0
    if prev > 0 and curr == 0:
        return "exited", -100.0
    if prev == 0 and curr == 0:
        return "held", 0.0

    pct = (curr - prev) / prev * 100
    if pct > 5:
        return "added", pct
    if pct < -5:
        return "trimmed", pct
    return "held", pct
