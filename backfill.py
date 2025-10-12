#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backfill.py — Simple OHLC backfiller via active broker (Kraken-first)
Build: v2.0.0 (2025-10-11, America/Chicago)

Usage (examples)
  python backfill.py --symbols BTCUSD,ETHUSD --timeframe 5Min --limit 1000
  python backfill.py --symbols SOLUSD --timeframe 1H --limit 500 --outdir ./data

Notes
- Uses br_router.get_bars(symbol, timeframe, limit) -> [{t,o,h,l,c,v}] newest last.
- Appends/merges by timestamp, de-duplicating rows on 'ts'.
- Timeframe is passed through to the adapter; Kraken adapter maps it to minutes.
"""

from __future__ import annotations

__version__ = "2.0.0"

import os
import csv
import argparse
from typing import List, Dict, Any
from datetime import datetime, timezone
import pandas as pd

# Route via broker router; Kraken preferred
try:
    import br_router as br
except Exception:
    import broker_kraken as br  # type: ignore

UTC = timezone.utc

def _ensure_outdir(path: str) -> None:
    if path and not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def _bars_to_df(bars: List[dict]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
    df = pd.DataFrame(bars).copy()
    # timestamp
    if "t" in df.columns:
        df["ts"] = pd.to_datetime(df["t"], unit="s", utc=True, errors="coerce")
    elif "time" in df.columns:
        df["ts"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    else:
        df["ts"] = pd.to_datetime(df.index, utc=True, errors="coerce")
    # columns
    rename_map = {"o":"open","h":"high","l":"low","c":"close","v":"volume","vol":"volume"}
    for k,v in rename_map.items():
        if k in df and v not in df:
            df[v] = df[k]
    keep = ["ts","open","high","low","close","volume"]
    df = df[keep].dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    return df

def _read_existing_csv(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
    try:
        df = pd.read_csv(path, parse_dates=["ts"])
        if "ts" not in df.columns:
            return pd.DataFrame(columns=["ts","open","high","low","close","volume"])
        return df
    except Exception:
        return pd.DataFrame(columns=["ts","open","high","low","close","volume"])

def backfill_symbol(symbol: str, timeframe: str, limit: int, outdir: str) -> Dict[str, Any]:
    """Fetch latest bars and append/merge to CSV."""
    sym = symbol.upper().replace("/", "")
    filename = f"{sym}_{timeframe}.csv"
    outpath = os.path.join(outdir, filename)

    try:
        bars = br.get_bars(sym, timeframe=timeframe, limit=int(limit))
        new_df = _bars_to_df(bars)
    except Exception as e:
        return {"symbol": sym, "timeframe": timeframe, "ok": False, "error": str(e)}

    old_df = _read_existing_csv(outpath)
    all_df = pd.concat([old_df, new_df], ignore_index=True)

    # De-dup on ts; keep last occurrence (newest values)
    if not all_df.empty:
        all_df = all_df.drop_duplicates(subset=["ts"], keep="last").sort_values("ts")

    _ensure_outdir(outdir)
    all_df.to_csv(outpath, index=False)

    return {
        "symbol": sym,
        "timeframe": timeframe,
        "ok": True,
        "rows_new": len(new_df),
        "rows_total": len(all_df),
        "file": outpath,
    }

def parse_symbols(arg: str) -> List[str]:
    if not arg:
        return []
    parts = []
    for chunk in arg.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.extend(chunk.split())
    return [p.strip().upper().replace("/", "") for p in parts if p.strip()]

def main():
    ap = argparse.ArgumentParser(description="Backfill OHLC bars via active broker (Kraken-first).")
    ap.add_argument("--symbols", required=True, help="Comma/space separated symbols (e.g., BTCUSD,ETHUSD)")
    ap.add_argument("--timeframe", default="5Min", help="Timeframe (e.g., 1m,5Min,1H,4h,1d). Default: 5Min")
    ap.add_argument("--limit", type=int, default=600, help="Number of recent bars to fetch. Default: 600")
    ap.add_argument("--outdir", default="./data", help="Output directory for CSVs. Default: ./data")

    args = ap.parse_args()
    syms = parse_symbols(args.symbols)

    results = []
    for s in syms:
        r = backfill_symbol(s, args.timeframe, args.limit, args.outdir)
        results.append(r)
        status = "OK" if r.get("ok") else "ERR"
        print(f"[{status}] {s}@{args.timeframe} -> {r.get('file','')} (new {r.get('rows_new',0)}, total {r.get('rows_total',0)})"
              + (f" | {r.get('error')}" if r.get("error") else ""))

if __name__ == "__main__":
    main()
