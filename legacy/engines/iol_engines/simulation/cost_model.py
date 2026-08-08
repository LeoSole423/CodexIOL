"""Execution-cost model for realistic paper trading."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from math import floor
from typing import Any, Dict, Mapping, Optional


COST_MODEL_VERSION = "iol-costs-2026-05-gold-v1"
IVA_RATE = 0.21

BROKER_COMMISSION_BY_TIER = {
    "gold": 0.005,
    "platinum": 0.003,
    "black": 0.001,
}

MARKET_FEE_BY_INSTRUMENT = {
    "stock": 0.0005,
    "cedear": 0.0005,
    "equity": 0.0005,
    "bond": 0.0001,
    "on": 0.0001,
    "obligation": 0.0001,
    "letra": 0.00001,
    "bill": 0.00001,
}

MARKET_FEE_IVA_INSTRUMENTS = frozenset({"stock", "cedear", "equity", "option"})
BROKER_IVA_EXEMPT_INSTRUMENTS = frozenset({"bond", "on", "obligation", "letra", "bill"})


def parse_instrument_overrides(raw: str | None) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    if not raw:
        return overrides
    for item in raw.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        symbol, kind = item.split(":", 1)
        symbol = symbol.strip().upper()
        kind = normalize_instrument_type(kind)
        if symbol:
            overrides[symbol] = kind
    return overrides


def normalize_instrument_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "stock"
    if "cedear" in text:
        return "cedear"
    if "accion" in text or "acción" in text or "stock" in text or "equity" in text:
        return "stock"
    if "bono" in text or "bond" in text or "titulo publico" in text or "título público" in text:
        return "bond"
    if "obligacion" in text or "obligación" in text or text in {"on", "ons"}:
        return "on"
    if "letra" in text or "bill" in text:
        return "letra"
    return text


@dataclass(frozen=True)
class ExecutionFill:
    symbol: str
    side: str
    quantity: float
    requested_amount_ars: float
    gross_amount_ars: float
    net_amount_ars: float
    commission_ars: float
    market_fee_ars: float
    iva_ars: float
    slippage_ars: float
    total_cost_ars: float
    net_cash_impact_ars: float
    effective_price: float
    reference_price: float
    instrument_type: str
    price_source: str
    cost_model_version: str = COST_MODEL_VERSION
    liquidity_warning: Optional[str] = None
    realized_pnl_ars: Optional[float] = None

    @property
    def total_entry_basis_ars(self) -> float:
        return self.net_cash_impact_ars if self.side == "buy" else 0.0

    def to_json_dict(self) -> Dict[str, Any]:
        return {
            "cost_model_version": self.cost_model_version,
            "side": self.side,
            "requested_amount_ars": round(self.requested_amount_ars, 2),
            "gross_amount_ars": round(self.gross_amount_ars, 2),
            "net_amount_ars": round(self.net_amount_ars, 2),
            "commission_ars": round(self.commission_ars, 2),
            "market_fee_ars": round(self.market_fee_ars, 2),
            "iva_ars": round(self.iva_ars, 2),
            "slippage_ars": round(self.slippage_ars, 2),
            "total_cost_ars": round(self.total_cost_ars, 2),
            "net_cash_impact_ars": round(self.net_cash_impact_ars, 2),
            "effective_price": round(self.effective_price, 6),
            "reference_price": round(self.reference_price, 6),
            "instrument_type": self.instrument_type,
            "price_source": self.price_source,
            "liquidity_warning": self.liquidity_warning,
            "realized_pnl_ars": round(self.realized_pnl_ars, 2) if self.realized_pnl_ars is not None else None,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_json_dict(), sort_keys=True)


@dataclass(frozen=True)
class ExecutionCostModel:
    commission_tier: str = "gold"
    include_iva: bool = True
    include_market_fees: bool = True
    default_instrument_type: str = "stock"
    max_daily_volume_pct: float = 0.02
    instrument_overrides: Mapping[str, str] = field(default_factory=dict)
    commission_min: float = 0.0
    legacy_commission_rate: Optional[float] = None
    version: str = COST_MODEL_VERSION

    @classmethod
    def from_config(
        cls,
        *,
        commission_tier: str = "gold",
        include_iva: bool = True,
        include_market_fees: bool = True,
        default_instrument_type: str = "stock",
        max_daily_volume_pct: float = 0.02,
        instrument_overrides: Mapping[str, str] | None = None,
        commission_min: float = 0.0,
        legacy_commission_rate: Optional[float] = None,
    ) -> "ExecutionCostModel":
        return cls(
            commission_tier=str(commission_tier or "gold").strip().lower(),
            include_iva=bool(include_iva),
            include_market_fees=bool(include_market_fees),
            default_instrument_type=normalize_instrument_type(default_instrument_type),
            max_daily_volume_pct=float(max_daily_volume_pct),
            instrument_overrides={k.upper(): normalize_instrument_type(v) for k, v in (instrument_overrides or {}).items()},
            commission_min=float(commission_min or 0.0),
            legacy_commission_rate=legacy_commission_rate,
        )

    def broker_commission_rate(self) -> float:
        if self.commission_tier in BROKER_COMMISSION_BY_TIER:
            return BROKER_COMMISSION_BY_TIER[self.commission_tier]
        if self.legacy_commission_rate is not None:
            return float(self.legacy_commission_rate)
        return BROKER_COMMISSION_BY_TIER["gold"]

    def instrument_type_for(self, symbol: str, hinted_type: Any = None) -> str:
        override = self.instrument_overrides.get(str(symbol or "").upper())
        if override:
            return normalize_instrument_type(override)
        if hinted_type:
            return normalize_instrument_type(hinted_type)
        return normalize_instrument_type(self.default_instrument_type)

    def max_gross_for_cash(self, cash_available: float, price: float, slippage_pct: float, instrument_type: str) -> float:
        if cash_available <= 0 or price <= 0:
            return 0.0
        unit = price * (1.0 + max(slippage_pct, 0.0))
        if unit <= 0:
            return 0.0
        probe = self.buy_fill(
            symbol="_PROBE",
            amount_ars=max(cash_available, unit),
            price=price,
            slippage_pct=slippage_pct,
            cash_available=cash_available,
            instrument_type=instrument_type,
        )
        return probe.gross_amount_ars

    def buy_fill(
        self,
        *,
        symbol: str,
        amount_ars: float,
        price: float,
        slippage_pct: float,
        cash_available: float,
        instrument_type: str,
        price_source: str = "market_symbol_snapshots",
        volume_amount: Optional[float] = None,
    ) -> ExecutionFill:
        instrument_type = normalize_instrument_type(instrument_type)
        reference_price = float(price)
        effective_price = reference_price * (1.0 + max(float(slippage_pct or 0.0), 0.0))
        capped_amount, liquidity_warning = self._apply_liquidity_cap(amount_ars, volume_amount)
        quantity = floor(max(0.0, capped_amount) / effective_price) if effective_price > 0 else 0
        while quantity > 0:
            gross = quantity * reference_price
            net = quantity * effective_price
            fees = self._fees(net, instrument_type)
            cash_impact = net + fees["cash_fees"]
            if cash_impact <= cash_available + 1e-6:
                break
            quantity -= 1
        return self._fill(
            symbol=symbol,
            side="buy",
            quantity=float(quantity),
            requested_amount_ars=float(amount_ars or 0.0),
            reference_price=reference_price,
            effective_price=effective_price,
            instrument_type=instrument_type,
            price_source=price_source,
            liquidity_warning=liquidity_warning,
        )

    def sell_fill(
        self,
        *,
        symbol: str,
        quantity: float,
        price: float,
        slippage_pct: float,
        instrument_type: str,
        price_source: str = "market_symbol_snapshots",
        volume_amount: Optional[float] = None,
        cost_basis_ars: Optional[float] = None,
    ) -> ExecutionFill:
        instrument_type = normalize_instrument_type(instrument_type)
        reference_price = float(price)
        effective_price = reference_price * (1.0 - max(float(slippage_pct or 0.0), 0.0))
        requested_amount = max(0.0, float(quantity or 0.0)) * reference_price
        capped_amount, liquidity_warning = self._apply_liquidity_cap(requested_amount, volume_amount)
        capped_qty = min(max(0.0, float(quantity or 0.0)), floor(capped_amount / reference_price) if reference_price > 0 else 0)
        fill = self._fill(
            symbol=symbol,
            side="sell",
            quantity=float(capped_qty),
            requested_amount_ars=requested_amount,
            reference_price=reference_price,
            effective_price=effective_price,
            instrument_type=instrument_type,
            price_source=price_source,
            liquidity_warning=liquidity_warning,
        )
        if cost_basis_ars is None:
            return fill
        return ExecutionFill(**{**fill.__dict__, "realized_pnl_ars": fill.net_cash_impact_ars - float(cost_basis_ars)})

    def _fill(
        self,
        *,
        symbol: str,
        side: str,
        quantity: float,
        requested_amount_ars: float,
        reference_price: float,
        effective_price: float,
        instrument_type: str,
        price_source: str,
        liquidity_warning: Optional[str],
    ) -> ExecutionFill:
        gross = quantity * reference_price
        net = quantity * effective_price
        fees = self._fees(net, instrument_type)
        slippage = abs(net - gross)
        cash_fees = fees["cash_fees"]
        net_cash_impact = net + cash_fees if side == "buy" else max(0.0, net - cash_fees)
        return ExecutionFill(
            symbol=symbol,
            side=side,
            quantity=quantity,
            requested_amount_ars=requested_amount_ars,
            gross_amount_ars=gross,
            net_amount_ars=net,
            commission_ars=fees["commission"],
            market_fee_ars=fees["market_fee"],
            iva_ars=fees["iva"],
            slippage_ars=slippage,
            total_cost_ars=slippage + cash_fees,
            net_cash_impact_ars=net_cash_impact,
            effective_price=effective_price,
            reference_price=reference_price,
            instrument_type=instrument_type,
            price_source=price_source,
            cost_model_version=self.version,
            liquidity_warning=liquidity_warning,
        )

    def _fees(self, notional: float, instrument_type: str) -> Dict[str, float]:
        commission = max(float(notional or 0.0) * self.broker_commission_rate(), self.commission_min if notional > 0 else 0.0)
        market_fee = 0.0
        if self.include_market_fees:
            market_fee = float(notional or 0.0) * MARKET_FEE_BY_INSTRUMENT.get(instrument_type, MARKET_FEE_BY_INSTRUMENT["stock"])
        iva = 0.0
        if self.include_iva:
            if instrument_type not in BROKER_IVA_EXEMPT_INSTRUMENTS:
                iva += commission * IVA_RATE
            if instrument_type in MARKET_FEE_IVA_INSTRUMENTS:
                iva += market_fee * IVA_RATE
        return {
            "commission": commission,
            "market_fee": market_fee,
            "iva": iva,
            "cash_fees": commission + market_fee + iva,
        }

    def _apply_liquidity_cap(self, requested_amount: float, volume_amount: Optional[float]) -> tuple[float, Optional[str]]:
        if volume_amount is None or volume_amount <= 0:
            return float(requested_amount or 0.0), "missing_volume_amount"
        cap = float(volume_amount) * max(0.0, self.max_daily_volume_pct)
        if requested_amount > cap:
            return cap, f"capped_by_volume:{self.max_daily_volume_pct:.4f}"
        return float(requested_amount or 0.0), None
