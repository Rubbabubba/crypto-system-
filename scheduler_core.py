from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Tuple, Callable

from book import StrategyBook, ScanRequest, ScanResult
from strategy_api import OrderIntent, PositionSnapshot, RiskContext
from position_manager import Position as PMPosition
from risk_engine import RiskEngine


@dataclass
class SchedulerConfig:
    """
    Minimal config for a single scheduler pass.
    """
    now: dt.datetime
    timeframe: str
    limit: int
    symbols: List[str]
    strats: List[str]
    notional: float
    positions: Mapping[Tuple[str, str], PMPosition]  # keyed by (symbol, strategy)
    contexts: Dict[str, Dict[str, Any]]
    risk_cfg: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchedulerResult:
    """
    Result of a single scheduler pass.

    intents:
      - high-level OrderIntent objects (entries / exits / TP / SL)
        AFTER applying per-strategy exits AND global risk exits
        (daily flatten, profit-lock, stop-loss, ATR floor).
    telemetry:
      - human/debug-focused logs about why each intent was produced
    """
    intents: List[OrderIntent] = field(default_factory=list)
    telemetry: List[Dict[str, Any]] = field(default_factory=list)


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------

def _make_position_snapshot_map(
    positions: Mapping[Tuple[str, str], PMPosition],
) -> Dict[Tuple[str, str], PositionSnapshot]:
    out: Dict[Tuple[str, str], PositionSnapshot] = {}
    for (sym, strat), pos in positions.items():
        out[(sym, strat)] = PositionSnapshot(
            symbol=sym,
            strategy=strat,
            qty=float(getattr(pos, "qty", 0.0) or 0.0),
            avg_price=getattr(pos, "avg_price", None),
            unrealized_pct=None,  # filled later
        )
    return out

def _strategy_instance_for(strat_id: str):
    """
    Map strat_id -> strategy instance.

    c1–c6 are implemented as classes with a module-level singleton
    named c1, c2, ..., c6 in their respective modules.
    """
    s = strat_id.strip().lower()

    if s == "c1":
        import c1 as m
        return m.c1
    if s == "c2":
        import c2 as m
        return m.c2
    if s == "c3":
        import c3 as m
        return m.c3
    if s == "c4":
        import c4 as m
        return m.c4
    if s == "c5":
        import c5 as m
        return m.c5
    if s == "c6":
        import c6 as m
        return m.c6

    return None

def _risk_context_from_engine(engine: RiskEngine) -> RiskContext:
    return engine.risk_context()

def debug_scan_for(cfg: SchedulerConfig, strat: str, symbol: str) -> Dict[str, Any]:
    """
    Explain why STRAT did or did not want to trade SYMBOL on this pass.
    """
    book = StrategyBook()
    sreq = ScanRequest(
        strat=strat,
        timeframe=cfg.timeframe,
        limit=cfg.limit,
        topk=book.topk,
        min_score=book.min_score,
        notional=cfg.notional,
    )

    scan_results: List[ScanResult] = book.scan(sreq, cfg.contexts) or []

    # Find the ScanResult for this symbol
    scan = next((r for r in scan_results if isinstance(r, ScanResult) and r.symbol == symbol), None)

    # Current position snapshot (if any)
    pos_map = _make_position_snapshot_map(cfg.positions)
    pos = pos_map.get((symbol, strat))

    out: Dict[str, Any] = {
        "strategy": strat,
        "symbol": symbol,
        "position": {
            "qty": float(getattr(pos, "qty", 0.0) or 0.0),
            "avg_price": float(getattr(pos, "avg_price", 0.0) or 0.0) if pos else None,
        },
    }

    if scan is None:
        out["scan"] = {"reason": "no_scan_result"}
        return out

    # Raw scan fields
    out["scan"] = {
        "action": scan.action,
        "reason": scan.reason,
        "score": float(scan.score),
        "atr_pct": float(scan.atr_pct),
        "notional": float(scan.notional),
        "selected": bool(scan.selected),
    }

    # Entry gating logic (mirror of your entry_signal)
    is_flat = (pos is None) or (abs(getattr(pos, "qty", 0.0) or 0.0) < 1e-10)
    out["entry_gate"] = {
        "is_flat": is_flat,
        "scan_selected": bool(scan.selected),
        "scan_action": scan.action,
        "scan_notional_positive": scan.notional > 0,
        "would_emit_entry": (
            is_flat
            and scan.selected
            and scan.action in ("buy", "sell")
            and scan.notional > 0
        ),
    }

    return out

# ----------------------------------------------------------------------
# Main entry point
# ----------------------------------------------------------------------

def run_scheduler_once(
    cfg: SchedulerConfig,
    last_price_fn: Optional[Callable[[str], float]] = None,
) -> SchedulerResult:
    """
    Pure strategy orchestration + global exit rules.

    This does NOT:
      - talk to the broker
      - enforce per-symbol or portfolio caps
      - send orders

    It ONLY:
      - runs StrategyBook on the given contexts
      - maps ScanResult + PositionSnapshot into OrderIntent
      - applies per-strategy take-profit / stop-loss rules
      - applies global exit rules (daily flatten, global TP/SL, ATR floor)

    Higher-level orchestration (app.py) will:
      - apply policy.guard.guard_allows(...)
      - enforce risk.json caps & loss zones for entries (no-rebuy, max exposure)
      - route to the broker.
    """
    # ---- Setup risk engine & risk context ----------------------------
    risk_engine = RiskEngine(cfg.risk_cfg or {})
    risk_ctx = _risk_context_from_engine(risk_engine)

    # ---- Positions: build snapshots and fill unrealized_pct ----------
    pos_map = _make_position_snapshot_map(cfg.positions)

    if last_price_fn is not None:
        for key, snap in pos_map.items():
            snap.unrealized_pct = risk_engine.compute_unrealized_pct(
                snap,
                last_price_fn=last_price_fn,
            )

    all_intents: List[OrderIntent] = []
    telemetry: List[Dict[str, Any]] = []

    # ---- Strategy loop: entries + per-strategy exits -----------------
    for strat in cfg.strats:
        strat_obj = _strategy_instance_for(strat)
        if strat_obj is None:
            # Strategies not yet upgraded to the new API are skipped here;
            # existing scheduler_run in app.py still handles them via legacy path.
            continue

        # StrategyBook is still the main signal source
        book = StrategyBook()
        sreq = ScanRequest(
            strat=strat,
            timeframe=cfg.timeframe,
            limit=cfg.limit,
            topk=book.topk,
            min_score=book.min_score,
            notional=cfg.notional,
        )

        scan_results: List[ScanResult] = book.scan(sreq, cfg.contexts) or []

        for r in scan_results:
            if not isinstance(r, ScanResult):
                continue

            sym = r.symbol
            key = (sym, strat)
            pos = pos_map.get(key) or PositionSnapshot(symbol=sym, strategy=strat)

            qty = float(getattr(pos, "qty", 0.0) or 0.0)
            is_flat = abs(qty) < 1e-10

            # Precompute some scan fields for reuse
            scan_selected = bool(getattr(r, "selected", False))
            scan_notional = float(getattr(r, "notional", 0.0) or 0.0)
            scan_action = getattr(r, "action", None)
            scan_score = float(getattr(r, "score", 0.0) or 0.0)
            scan_atr_pct = float(getattr(r, "atr_pct", 0.0) or 0.0)

            # ----- ENTRY (flat -> new position) -----------------------
            if is_flat:
                intent = strat_obj.entry_signal(r, pos, risk_ctx)

                if intent:
                    all_intents.append(intent)
                    telemetry.append(
                        {
                            "symbol": sym,
                            "strategy": strat,
                            "kind": intent.kind,
                            "side": intent.side,
                            "reason": intent.reason,
                            "score": scan_score,
                            "atr_pct": scan_atr_pct,
                            "source": "entry_signal",
                        }
                    )
                    # We don't need to consider exit logic when flat.
                    continue

                # No entry intent: if this looked like a valid candidate,
                # record WHY the strategy skipped it.
                if scan_selected and scan_notional > 0.0:
                    reason = "strategy_rejected_entry"
                    if scan_action == "sell":
                        # This is the case we're especially interested in:
                        # long-only strategy ignoring a SELL signal while flat.
                        reason = "long_only_blocked_sell_entry"

                    telemetry.append(
                        {
                            "symbol": sym,
                            "strategy": strat,
                            "kind": "entry_skip",
                            "side": scan_action,
                            "reason": reason,
                            "source": "strategy_layer",
                            "scan_score": scan_score,
                            "scan_atr_pct": scan_atr_pct,
                            "scan_notional": scan_notional,
                        }
                    )

                # Still flat, no entry; nothing more to do for this scan.
                continue

            # ----- EXIT on reverse signal (non-flat position) ---------
            exit_intent = strat_obj.exit_signal(r, pos, risk_ctx)

            if exit_intent:
                all_intents.append(exit_intent)
                telemetry.append(
                    {
                        "symbol": sym,
                        "strategy": strat,
                        "kind": exit_intent.kind,
                        "side": exit_intent.side,
                        "reason": exit_intent.reason,
                        "source": "exit_signal",
                    }
                )
            else:
                # Optional: log when we had a SELL signal while long but
                # the strategy decided not to exit.
                if scan_action == "sell":
                    telemetry.append(
                        {
                            "symbol": sym,
                            "strategy": strat,
                            "kind": "exit_skip",
                            "side": "sell",
                            "reason": "strategy_rejected_exit",
                            "source": "strategy_layer",
                            "scan_score": scan_score,
                            "scan_atr_pct": scan_atr_pct,
                        }
                    )

        # ----- Per-position TP / SL rules (once per position) --------
        for (sym, s_id), snap in pos_map.items():
            if s_id != strat:
                continue

            # Strategy-specific take-profit
            tp_intent = strat_obj.profit_take_rule(snap, risk_ctx)
            if tp_intent:
                all_intents.append(tp_intent)
                telemetry.append(
                    {
                        "symbol": sym,
                        "strategy": strat,
                        "kind": tp_intent.kind,
                        "side": tp_intent.side,
                        "reason": tp_intent.reason,
                        "source": "strategy_take_profit",
                    }
                )

            # Strategy-specific stop-loss
            sl_intent = strat_obj.stop_loss_rule(snap, risk_ctx)
            if sl_intent:
                all_intents.append(sl_intent)
                telemetry.append(
                    {
                        "symbol": sym,
                        "strategy": strat,
                        "kind": sl_intent.kind,
                        "side": sl_intent.side,
                        "reason": sl_intent.reason,
                        "source": "strategy_stop_loss",
                    }
                )

    # ---- Global exits on top of strategy exits -----------------------
    # For each open position, see if RiskEngine wants to flatten
    # (daily flatten, global profit-lock, global stop-loss, ATR floor).
    for (sym, strat), snap in pos_map.items():
        if abs(snap.qty) < 1e-10:
            continue

        # We may not have unrealized_pct (if last_price_fn missing)
        unrealized_pct = snap.unrealized_pct
        # ATR pct isn't known at the position level; leave as None for now.
        atr_pct = None

        reason = risk_engine.apply_global_exit_rules(
            pos=snap,
            unrealized_pct=unrealized_pct,
            atr_pct=atr_pct,
            now=cfg.now,
        )
        if not reason:
            continue

        # Decide close side
        if snap.qty > 0:
            side = "sell"
        else:
            side = "buy"

        # Map reason -> kind
        if reason.startswith("profit_lock:"):
            kind = "take_profit"
        elif reason.startswith("stop_loss:"):
            kind = "stop_loss"
        else:
            # daily_flatten, atr_floor_flat, etc. => use generic EXIT
            kind = "exit"

        intent = OrderIntent(
            strategy=strat,
            symbol=sym,
            side=side,
            kind=kind,
            notional=None,  # "close full position" semantic
            reason=f"global_{reason}",
            meta={
                "unrealized_pct": unrealized_pct,
            },
        )
        all_intents.append(intent)
        telemetry.append(
            {
                "symbol": sym,
                "strategy": strat,
                "kind": kind,
                "side": side,
                "reason": intent.reason,
                "source": "global_risk_engine",
            }
        )

    # NOTE: We deliberately do NOT enforce:
    #   - per-symbol caps
    #   - no-rebuy-below loss-zone rule
    # here. Those will be applied at the higher orchestration layer
    # (e.g. in app.py) right before sending to the broker, using
    # RiskEngine.enforce_symbol_cap() and
    # RiskEngine.is_loss_zone_norebuy_block().

    return SchedulerResult(intents=all_intents, telemetry=telemetry)
