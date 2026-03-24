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
    price: float | None = Field(None, description="optional entry/reference price")
    entry_price: float | None = Field(None, description="optional entry/reference price")
    stop_price: float | None = Field(None, description="optional stop price")
    take_price: float | None = Field(None, description="optional take price")
    bar_ts: int | None = Field(None, description="optional bar timestamp for signal fingerprinting")
    trigger_price: float | None = Field(None, description="optional trigger/breakout level for signal fingerprinting")
    fingerprint: str | None = Field(None, description="optional explicit fingerprint override")
    ranking_score: float | None = Field(None, description="optional scanner ranking score")


class WorkerExitPayload(BaseModel):
    worker_secret: str | None = None
    heartbeat_kind: str | None = None
    heartbeat_utc: str | None = None
    heartbeat_ts: float | None = None
    heartbeat_seq: int | None = None
    loop_interval_sec: float | None = None
    loop_pid: int | None = None
    heartbeat_source: str | None = None


class WorkerScanPayload(BaseModel):
    worker_secret: str | None = None
    dry_run: bool | None = False
    heartbeat_kind: str | None = None
    heartbeat_utc: str | None = None
    heartbeat_ts: float | None = None
    heartbeat_seq: int | None = None
    loop_interval_sec: float | None = None
    loop_pid: int | None = None
    heartbeat_source: str | None = None

    # Optional controls used by /worker/scan_entries
    # - symbols: explicit universe override (e.g. ["BTC/USD", "ETH/USD"])
    # - force_scan: bypass allow-list filtering when SCANNER_SOFT_ALLOW is enabled
    symbols: list[str] | None = None
    force_scan: bool = False


class WorkerExitDiagnosticsPayload(BaseModel):
    worker_secret: str | None = None
    dry_run: bool | None = True
    symbols: list[str] | None = None


class WorkerAdoptPositionsPayload(BaseModel):
    worker_secret: str | None = None
    include_dust: bool | None = False
    min_notional_usd: float | None = None
    reset_plans: bool | None = False


class WorkerRouteTruthPayload(BaseModel):
    worker_secret: str | None = None
    worker_kind: str | None = None
    heartbeat_kind: str | None = None
    heartbeat_utc: str | None = None
    heartbeat_ts: float | None = None
    heartbeat_seq: int | None = None
    loop_interval_sec: float | None = None
    loop_pid: int | None = None
    heartbeat_source: str | None = None
    phase: str | None = None
    ok: bool | None = None
    target_base_url: str | None = None
    target_path: str | None = None
    target_url: str | None = None
    status_code: int | None = None
    elapsed_ms: float | None = None
    auth_present: bool | None = None
    error: str | None = None
    response_excerpt: str | None = None
    worker_request_id: str | None = None
    route_truth_utc: str | None = None
