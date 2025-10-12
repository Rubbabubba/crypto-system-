#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
book.py — Lightweight order book utilities (broker-agnostic)
Build: v2.0.0 (2025-10-11, America/Chicago)

Purpose
- Provide small helpers to analyze level-1/level-2 snapshots for guards and attribution.
- No network calls. Works with dicts/lists your adapters may return later.

Accepted shapes
- L1 example: {"bid": 64250.10, "ask": 64250.60, "bidsz": 0.12, "asksz": 0.10}
- L2 example: {"bids": [[price, size], ...], "asks": [[price, size], ...]} with best first.

Exports
- normalize(ob) -> dict                    # best-effort L1 from L1/L2 input
- best_bid_ask(ob) -> (bid, ask)           # floats (NaN if unavailable)
- spread(ob) -> float                      # ask - bid (NaN if missing)
- spread_bps(ob, ref=None) -> float        # 10_000 * spread / ref_price
- midprice(ob) -> float                    # (bid + ask)/2
- imbalance(ob) -> float                   # (bid_sz - ask_sz)/(bid_sz + ask_sz)
- microprice(ob) -> float                  # size-weighted mid: (a*bs + b*as) / (bs + as)
- is_marketable(ob, side, price=None) -> bool  # would cross the book?
"""

from __future__ import annotations

__version__ = "2.0.0"

from typing import Any, Dict, List, Tuple
import math

__all__ = [
    "normalize",
    "best_bid_ask",
    "spread",
    "spread_bps",
    "midprice",
    "imbalance",
    "microprice",
    "is_marketable",
]

def _nan() -> float:
    return float("nan")

def _is_num(x: Any) -> bool:
    try:
        float(x)
        return True
    except Exception:
        return False

def normalize(ob: Dict[str, Any]) -> Dict[str, float]:
    """
    Best-effort normalize to L1 dict: {bid, ask, bidsz, asksz}
    Accepts either L1 input or L2 with 'bids'/'asks' arrays.
    """
    if not isinstance(ob, dict):
        return {"bid": _nan(), "ask": _nan(), "bidsz": _nan(), "asksz": _nan()}

    bid = ob.get("bid", None)
    ask = ob.get("ask", None)
    bidsz = ob.get("bidsz", None)
    asksz = ob.get("asksz", None)

    # Try L2 if needed
    if (bid is None or ask is None) and ("bids" in ob or "asks" in ob):
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if bids and isinstance(bids[0], (list, tuple)) and len(bids[0]) >= 1:
            bid = bids[0][0]
            bidsz = bids[0][1] if len(bids[0]) >= 2 else bidsz
        if asks and isinstance(asks[0], (list, tuple)) and len(asks[0]) >= 1:
            ask = asks[0][0]
            asksz = asks[0][1] if len(asks[0]) >= 2 else asksz

    out = {
        "bid": float(bid) if _is_num(bid) else _nan(),
        "ask": float(ask) if _is_num(ask) else _nan(),
        "bidsz": float(bidsz) if _is_num(bidsz) else _nan(),
        "asksz": float(asksz) if _is_num(asksz) else _nan(),
    }
    return out

def best_bid_ask(ob: Dict[str, Any]) -> Tuple[float, float]:
    n = normalize(ob)
    return n["bid"], n["ask"]

def spread(ob: Dict[str, Any]) -> float:
    bid, ask = best_bid_ask(ob)
    if math.isnan(bid) or math.isnan(ask):
        return _nan()
    return float(ask - bid)

def midprice(ob: Dict[str, Any]) -> float:
    bid, ask = best_bid_ask(ob)
    if math.isnan(bid) or math.isnan(ask):
        return _nan()
    return (bid + ask) / 2.0

def spread_bps(ob: Dict[str, Any], ref: float | None = None) -> float:
    spr = spread(ob)
    if math.isnan(spr):
        return _nan()
    ref_price = ref if (ref and ref > 0) else midprice(ob)
    if not ref_price or ref_price <= 0 or math.isnan(ref_price):
        return _nan()
    return (spr / ref_price) * 10_000.0

def imbalance(ob: Dict[str, Any]) -> float:
    n = normalize(ob)
    bs = n["bidsz"]
    asz = n["asksz"]
    if math.isnan(bs) or math.isnan(asz) or (bs + asz) <= 0:
        return _nan()
    return (bs - asz) / (bs + asz)

def microprice(ob: Dict[str, Any]) -> float:
    n = normalize(ob)
    b, a, bs, asz = n["bid"], n["ask"], n["bidsz"], n["asksz"]
    if any(math.isnan(x) for x in (b, a, bs, asz)) or (bs + asz) <= 0:
        # fall back to simple mid
        return midprice(n)
    # size-weighted mid
    return (a * bs + b * asz) / (bs + asz)

def is_marketable(ob: Dict[str, Any], side: str, price: float | None = None) -> bool:
    """
    True if a given order would cross the book:
      - Buy is marketable when price >= ask (or when price is None, we assume MARKET -> True if ask is present)
      - Sell is marketable when price <= bid
    """
    side = (side or "").strip().lower()
    bid, ask = best_bid_ask(ob)
    if side == "buy":
        if price is None:
            return not math.isnan(ask)
        return (not math.isnan(ask)) and price >= ask
    if side == "sell":
        if price is None:
            return not math.isnan(bid)
        return (not math.isnan(bid)) and price <= bid
    return False
