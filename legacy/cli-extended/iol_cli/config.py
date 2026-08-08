import os
from dataclasses import dataclass, field
from typing import List, Tuple
from dotenv import load_dotenv

class ConfigError(RuntimeError):
    pass

@dataclass
class Config:
    username: str
    password: str
    base_url: str
    timeout: int
    commission_rate: float
    commission_min: float
    db_path: str
    market_tz: str
    market_open_time: str
    market_close_time: str
    store_raw: bool
    ohlcv_watchlist: List[Tuple[str, str]] = field(default_factory=list)
    opp_watchlist: List[Tuple[str, str]] = field(default_factory=list)
    sim_commission_tier: str = "gold"
    sim_include_iva: bool = True
    sim_include_market_fees: bool = True
    sim_default_instrument_type: str = "stock"
    sim_instrument_overrides: str = ""
    sim_max_daily_volume_pct: float = 0.02

    def resolve_base_url(self, base_url_override=None):
        if base_url_override:
            return base_url_override.rstrip("/")
        return self.base_url.rstrip("/")


def _get_float(name, default):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"Invalid float in {name}: {raw}") from exc


def _get_int(name, default):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Invalid int in {name}: {raw}") from exc


def _get_bool(name, default=False):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    key = raw.strip().lower()
    if key in ("1", "true", "yes", "y", "on"):
        return True
    if key in ("0", "false", "no", "n", "off"):
        return False
    raise ConfigError(f"Invalid bool in {name}: {raw}")


def load_config() -> Config:
    load_dotenv()
    username = os.getenv("IOL_USERNAME", "").strip()
    password = os.getenv("IOL_PASSWORD", "").strip()

    if not username or not password:
        raise ConfigError("Missing IOL_USERNAME or IOL_PASSWORD in .env")

    # IOL doesn't expose a stable sandbox environment. Keep a single base URL.
    base_url = (
        os.getenv("IOL_API_URL", "").strip()
        or os.getenv("IOL_API_URL_REAL", "").strip()
        or os.getenv("IOL_API_URL_SANDBOX", "").strip()
        or "https://api.invertironline.com"
    )
    timeout = _get_int("IOL_TIMEOUT", 20)
    commission_rate = _get_float("IOL_COMMISSION_RATE", 0.0)
    commission_min = _get_float("IOL_COMMISSION_MIN", 0.0)
    sim_commission_tier = os.getenv("IOL_SIM_COMMISSION_TIER", "gold").strip().lower() or "gold"
    sim_include_iva = _get_bool("IOL_SIM_INCLUDE_IVA", True)
    sim_include_market_fees = _get_bool("IOL_SIM_INCLUDE_MARKET_FEES", True)
    sim_default_instrument_type = os.getenv("IOL_SIM_DEFAULT_INSTRUMENT_TYPE", "stock").strip() or "stock"
    sim_instrument_overrides = os.getenv("IOL_SIM_INSTRUMENT_OVERRIDES", "").strip()
    sim_max_daily_volume_pct = _get_float("IOL_SIM_MAX_DAILY_VOLUME_PCT", 0.02)
    db_path = os.getenv("IOL_DB_PATH", "data/iol_history.db").strip()
    market_tz = os.getenv("IOL_MARKET_TZ", "America/Argentina/Buenos_Aires").strip()
    market_open_time = os.getenv("IOL_MARKET_OPEN_TIME", "11:00").strip()
    market_close_time = os.getenv("IOL_MARKET_CLOSE_TIME", "18:00").strip()
    store_raw = _get_bool("IOL_STORE_RAW", False)

    # IOL_OHLCV_WATCHLIST: comma-separated "SYMBOL:MARKET" pairs to track
    # even when not in the portfolio. Market defaults to "bCBA" if omitted.
    # Example: IOL_OHLCV_WATCHLIST=SPY:bCBA,GLD:bCBA,GGAL:bCBA,YPF:bCBA
    ohlcv_watchlist: List[Tuple[str, str]] = []
    raw_watchlist = os.getenv("IOL_OHLCV_WATCHLIST", "").strip()
    if raw_watchlist:
        for entry in raw_watchlist.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                sym, mkt = entry.split(":", 1)
            else:
                sym, mkt = entry, "bCBA"
            sym = sym.strip().upper()
            mkt = mkt.strip()
            if sym:
                ohlcv_watchlist.append((sym, mkt))

    # IOL_OPP_WATCHLIST: comma-separated "SYMBOL:MARKET" pairs always considered
    # by the opportunity engine, regardless of whether they appear in the IOL panel.
    # Market defaults to "bcba" if omitted.
    # Example: IOL_OPP_WATCHLIST=ETSY:bcba,COIN:bcba,MELI:bcba
    opp_watchlist: List[Tuple[str, str]] = []
    raw_opp_watchlist = os.getenv("IOL_OPP_WATCHLIST", "").strip()
    if raw_opp_watchlist:
        for entry in raw_opp_watchlist.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if ":" in entry:
                sym, mkt = entry.split(":", 1)
            else:
                sym, mkt = entry, "bcba"
            sym = sym.strip().upper()
            mkt = mkt.strip().lower()
            if sym:
                opp_watchlist.append((sym, mkt))

    return Config(
        username=username,
        password=password,
        base_url=base_url,
        timeout=timeout,
        commission_rate=commission_rate,
        commission_min=commission_min,
        sim_commission_tier=sim_commission_tier,
        sim_include_iva=sim_include_iva,
        sim_include_market_fees=sim_include_market_fees,
        sim_default_instrument_type=sim_default_instrument_type,
        sim_instrument_overrides=sim_instrument_overrides,
        sim_max_daily_volume_pct=sim_max_daily_volume_pct,
        db_path=db_path,
        market_tz=market_tz,
        market_open_time=market_open_time,
        market_close_time=market_close_time,
        store_raw=store_raw,
        ohlcv_watchlist=ohlcv_watchlist,
        opp_watchlist=opp_watchlist,
    )
