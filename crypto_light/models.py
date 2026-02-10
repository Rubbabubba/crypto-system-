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

class WorkerScanPayload(BaseModel):
    worker_secret: str | None = None
    dry_run: bool | None = False

    # Optional controls used by /worker/scan_entries
    # - symbols: explicit universe override (e.g. [\"BTC/USD\", \"ETH/USD\"])
    # - force_scan: bypass allow-list filtering when SCANNER_SOFT_ALLOW is enabled
    symbols: list[str] | None = None
    force_scan: bool = False
