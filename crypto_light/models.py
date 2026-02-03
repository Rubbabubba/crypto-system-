from __future__ import annotations

from pydantic import BaseModel, Field


class WebhookPayload(BaseModel):
    secret: str = Field(..., description="WEBHOOK_SECRET")
    symbol: str = Field(..., description="UI symbol like BTC/USD")
    side: str = Field(..., description="buy or sell (sell == close spot position)")
    strategy: str = Field(..., description="strategy tag for attribution (rb1, tc1, etc.)")
    signal: str | None = Field(None, description="optional signal name")
    signal_id: str | None = Field(None, description="optional idempotency key for this signal")
    notional_usd: float | None = Field(None, description="USD notional to trade")


class WorkerExitPayload(BaseModel):
    worker_secret: str | None = None
