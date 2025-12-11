# advisor_v2.py

import os
import json
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, Optional, Tuple

# IMPORTANT: must match whatever your writer uses (append_journal_v2)
JOURNAL_V2_PATH = os.getenv("JOURNAL_V2_PATH", "journal_v2.jsonl")


@dataclass
class StratStats:
    strategy: str
    count: int = 0
    buy: int = 0
    sell: int = 0
    sent: int = 0
    error: int = 0
    pnl_sum: float = 0.0  # placeholder until we wire real P&L


@dataclass
class PairStats:
    strategy: str
    symbol: str
    count: int = 0
    buy: int = 0
    sell: int = 0
    sent: int = 0
    error: int = 0
    pnl_sum: float = 0.0


def _iter_events(limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    """
    Stream events from JOURNAL_V2_PATH, newest-last.
    limit: if provided, cap the total number of events returned (from the tail).
    """
    path = JOURNAL_V2_PATH
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if limit is not None and limit > 0:
        lines = lines[-limit:]

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            evt = json.loads(line)
        except Exception:
            # skip bad lines
            continue
        if isinstance(evt, dict):
            yield evt


def _accumulate(
    events: Iterable[Dict[str, Any]]
) -> Tuple[Dict[str, StratStats], Dict[Tuple[str, str], PairStats], int]:
    per_strat: Dict[str, StratStats] = {}
    per_pair: Dict[Tuple[str, str], PairStats] = {}
    total = 0

    for evt in events:
        total += 1
        strat = str(evt.get("strategy") or "unlabeled").strip().lower()
        sym = str(evt.get("symbol") or "").strip().upper()
        side = str(evt.get("side") or "").strip().lower()
        status = str(evt.get("status") or "").strip().lower()
        pnl = float(evt.get("pnl", 0.0) or 0.0)  # currently 0, but future-proof

        # --- per-strategy ---
        s = per_strat.get(strat)
        if s is None:
            s = StratStats(strategy=strat)
            per_strat[strat] = s
        s.count += 1
        if side == "buy":
            s.buy += 1
        elif side == "sell":
            s.sell += 1
        if status in ("sent", "filled", "partial"):
            s.sent += 1
        if status == "error":
            s.error += 1
        s.pnl_sum += pnl

        # --- per (strategy, symbol) ---
        if sym:
            key = (strat, sym)
            p = per_pair.get(key)
            if p is None:
                p = PairStats(strategy=strat, symbol=sym)
                per_pair[key] = p
            p.count += 1
            if side == "buy":
                p.buy += 1
            elif side == "sell":
                p.sell += 1
            if status in ("sent", "filled", "partial"):
                p.sent += 1
            if status == "error":
                p.error += 1
            p.pnl_sum += pnl

    return per_strat, per_pair, total


def advisor_summary(limit: Optional[int] = 1000) -> Dict[str, Any]:
    """
    Build a high-level advisor summary on top of journal_v2.
    Returns a JSON-ready dict shaped similarly to /journal/v2/review,
    plus some "top" views you can use in the UI.
    """
    events = list(_iter_events(limit=limit))
    per_strat, per_pair, total = _accumulate(events)

    # Sort by activity
    strat_list = sorted(per_strat.values(), key=lambda s: s.count, reverse=True)
    pair_list = sorted(per_pair.values(), key=lambda p: p.count, reverse=True)

    # Basic "top" views the Advisor can consume
    top_strats = [asdict(s) for s in strat_list[:10]]
    top_pairs = [asdict(p) for p in pair_list[:10]]

    return {
        "ok": True,
        "count": total,
        "limit": limit,
        "per_strategy": [asdict(s) for s in strat_list],
        "per_pair": [asdict(p) for p in pair_list],
        "top_strategies": top_strats,
        "top_pairs": top_pairs,
    }
