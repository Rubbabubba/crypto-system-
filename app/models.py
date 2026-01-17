from __future__ import annotations

from pydantic import BaseModel, Field


class WebhookPayload(BaseModel):
    secret: str = Field(..., description="WEBHOOK_SECRET")
    symbol: str = Field(..., description="UI symbol like BTC/USD")
    side: str = Field(..., description="buy or sell")
    strategy: str = Field(..., description="strategy tag for attribution")
    signal: str | None = Field(None, description="optional signal name")
    notional_usd: float | None = Field(None, description="USD notional to trade")


class WorkerExitPayload(BaseModel):
    worker_secret: str | None = None
