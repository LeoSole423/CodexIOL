from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PortfolioSnapshot(BaseModel):
    schema_version: int
    source: str = "IOL"
    as_of: str
    generated_at: str
    retrieved_at: str
    account: dict[str, Any]
    portfolios: dict[str, dict[str, Any]]
    orders: list[dict[str, Any]]
    movements: list[dict[str, Any]] | None
    sources: dict[str, dict[str, Any]]
    warnings: list[str] = Field(default_factory=list)


class InvestorProfileResponse(BaseModel):
    schema_version: int
    source: str = "investor-profile.toml"
    generated_at: str
    profile: dict[str, Any]
    warnings: list[str] = Field(default_factory=list)


class MovementList(BaseModel):
    schema_version: int
    source: str = "IOL"
    generated_at: str
    from_date: str
    to_date: str
    country: str
    movements: list[dict[str, Any]] | None
    warnings: list[str] = Field(default_factory=list)


class InvestmentHistory(BaseModel):
    schema_version: int
    source: str = "CodexIOL review records"
    generated_at: str
    reviews: list[dict[str, Any]] = Field(default_factory=list)
    decisions: list[dict[str, Any]] = Field(default_factory=list)
    transition_plan: dict[str, Any] | None = None
    warnings: list[str] = Field(default_factory=list)


class AssetContext(BaseModel):
    schema_version: int
    source: str = "CodexIOL and IOL"
    generated_at: str
    symbol: str
    market: str
    asset: dict[str, Any] | None
    related_orders: list[dict[str, Any]]
    related_movements: list[dict[str, Any]]
    profile_context: dict[str, Any]
    history: InvestmentHistory
    warnings: list[str] = Field(default_factory=list)


class Capabilities(BaseModel):
    schema_version: int
    generated_at: str
    read_only: bool = True
    broker: str = "InvertirOnline"
    can_read_portfolio: bool = True
    can_read_movements: bool = True
    can_read_orders: bool = True
    can_read_investor_profile: bool = True
    can_read_history: bool = True
    can_prepare_orders: bool = False
    can_execute_orders: bool = False
    can_cancel_orders: bool = False
    can_transfer_money: bool = False
