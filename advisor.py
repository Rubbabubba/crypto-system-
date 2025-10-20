# advisor.py
from __future__ import annotations
import os, json, math, datetime as dt
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd

POLICY_DIR = Path(os.getenv("POLICY_CFG_DIR", "policy_config"))
JOURNAL_PATH = Path(os.getenv("JOURNAL_PATH", "./journal_v2.jsonl"))  # same default as app.py
TZ = dt.timezone(dt.timedelta(hours=-5))  # America/Chicago; keep simple for daily reports

def _load_jsonl(path: Path) -> List[dict]:
    rows = []
    if not path.exists(): return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: rows.append(json.loads(line))
            except: pass
    return rows

def _read_policy() -> Dict[str, Any]:
    def read(p): 
        try: return json.loads(Path(POLICY_DIR / p).read_text())
        except: return {}
    return {
        "windows": read("windows.json"),
        "whitelist": read("whitelist.json"),
        "risk": read("risk.json"),
    }

def _kpis_for_day(rows: List[dict], day: dt.date) -> pd.DataFrame:
    # keep only filled trades with price/vol and the given day (America/Chicago-ish)
    recs = []
    for r in rows:
        if not (r.get("price") and r.get("vol")): 
            continue
        ts = r.get("filled_ts") or r.get("ts")
        if not ts: 
            continue
        t = dt.datetime.fromtimestamp(float(ts), tz=dt.timezone.utc).astimezone(TZ)
        if t.date() != day: 
            continue
        recs.append({
            "dt": t, "hour": t.hour,
            "strategy": r.get("strategy", "unknown"),
            "symbol": (r.get("symbol") or "").upper(),
            "side": r.get("side"),
            "price": float(r.get("price") or 0),
            "vol": float(r.get("vol") or 0),
            "fee": float(r.get("fee") or 0),
            "cost": float(r.get("cost") or 0),
        })
    if not recs:
        return pd.DataFrame(columns=["dt","hour","strategy","symbol","side","price","vol","fee","pnl"])
    df = pd.DataFrame(recs)
    # Approx trade P&L contribution: use signed notional deltas against fill pairs (buy then sell).
    # For a daily diagnostic, we can approximate per fill: sell proceeds - buy cost - fee.
    # A better calc uses your FIFO engine, but daily advice is robust to this approximation.
    df["signed_cash"] = df.apply(lambda r: (-r["price"]*r["vol"]) if r["side"]=="buy" else (r["price"]*r["vol"]), axis=1)
    df["pnl"] = df["signed_cash"] - df["fee"]
    return df

def _agg_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {"by_strategy": {}, "by_symbol": {}, "by_hour": {}}
    if df.empty: return out
    g_strat = df.groupby("strategy")
    for s, g in g_strat:
        wins = (g["pnl"] > 0).sum()
        losses = (g["pnl"] < 0).sum()
        wr = float(wins) / max(1, (wins+losses))
        pf = float(g.loc[g["pnl"]>0, "pnl"].sum()) / max(1e-9, -g.loc[g["pnl"]<0,"pnl"].sum())
        exp = float(g["pnl"].sum()) / max(1, len(g))
        out["by_strategy"][s] = {
            "trades": int(len(g)), "win_rate": wr, "profit_factor": pf, "expectancy": exp,
            "net_pnl": float(g["pnl"].sum()), "fees": float(g["fee"].sum())
        }
    g_sym = df.groupby("symbol")
    for y, g in g_sym:
        out["by_symbol"][y] = {
            "trades": int(len(g)), "net_pnl": float(g["pnl"].sum()),
            "fees": float(g["fee"].sum())
        }
    g_hr = df.groupby(["strategy","hour"])
    for (s,h), g in g_hr:
        out["by_hour"].setdefault(s, {})[int(h)] = float(g["pnl"].sum())
    return out

def _recommend(policy: Dict[str,Any], k: Dict[str,Any]) -> Dict[str, Any]:
    rec: Dict[str, Any] = {"windows": {}, "whitelist": {}, "risk": {}, "notes": []}
    # ---- windows: add profitable hours, remove worst hours
    win_cfg = (policy.get("windows") or {}).get("windows", {})
    for strat, hr_map in (k.get("by_hour") or {}).items():
        hours_now = set((win_cfg.get(strat, {}) or {}).get("hours", []))
        if not hr_map: continue
        # Top + bottom hours
        top = sorted(hr_map.items(), key=lambda kv: kv[1], reverse=True)[:2]
        bot = sorted(hr_map.items(), key=lambda kv: kv[1])[:1]
        add = [h for h, v in top if v > 0 and h not in hours_now]
        drop = [h for h, v in bot if v < 0 and h in hours_now]
        if add or drop:
            rec["windows"][strat] = {"add_hours": add, "drop_hours": drop}
    # ---- whitelist: demote consistent losers, promote consistent winners
    wl = {k.upper(): set(map(str.upper, v)) for k, v in (policy.get("whitelist") or {}).items()}
    for y, row in (k.get("by_symbol") or {}).items():
        pnl = row["net_pnl"]
        for strat, syms in wl.items():
            if y in syms and pnl < 0:
                rec["whitelist"].setdefault(strat, {}).setdefault("remove", []).append(y)
    # We could also propose promotions by checking symbols traded outside WL (if any recorded)
    # ---- risk: tweak edge multiple / ATR tiers if fees dominate or too many blocks
    risk = policy.get("risk") or {}
    fee_pct = float(risk.get("fee_rate_pct", 0.26))
    edge_mult = float(risk.get("edge_multiple_vs_fee", 3.0))
    # If daily profit factor < 1 but fee share is high => raise edge_multiple; else if high PF but few trades => lower it slightly
    all_strat = k.get("by_strategy") or {}
    if all_strat:
        pf_avg = sum(s["profit_factor"] for s in all_strat.values())/max(1,len(all_strat))
        fees = sum(s["fees"] for s in all_strat.values())
        pnl = sum(s["net_pnl"] for s in all_strat.values())
        fee_share = fees / max(1e-9, abs(pnl)+fees)
        if pf_avg < 1.0 and fee_share > 0.35 and edge_mult < 4.0:
            rec["risk"]["edge_multiple_vs_fee"] = round(edge_mult + 0.5, 2)
        elif pf_avg > 1.5 and edge_mult > 2.0:
            rec["risk"]["edge_multiple_vs_fee"] = round(edge_mult - 0.25, 2)
    return rec

def analyze_day(date_str: Optional[str]=None) -> Dict[str, Any]:
    day = dt.datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else dt.datetime.now(TZ).date()
    rows = _load_jsonl(JOURNAL_PATH)
    df = _kpis_for_day(rows, day)
    kpis = _agg_kpis(df)
    policy = _read_policy()
    recs = _recommend(policy, kpis)
    return {
        "date": day.isoformat(),
        "kpis": kpis,
        "recommendations": recs,
        "policy_snapshot": policy,
    }

def write_policy_updates(recs: Dict[str, Any], dry: bool=True) -> Dict[str, Any]:
    changes = {}
    policy = _read_policy()
    # windows
    if recs.get("windows"):
        win = policy.get("windows") or {}
        win.setdefault("windows", {})
        for strat, d in recs["windows"].items():
            cur = win["windows"].setdefault(strat, {"hours": [], "dows": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]})
            hours = set(cur.get("hours", []))
            for h in d.get("add_hours", []): hours.add(int(h))
            for h in d.get("drop_hours", []): hours.discard(int(h))
            cur["hours"] = sorted(hours)
        changes["windows.json"] = win
        if not dry:
            Path(POLICY_DIR / "windows.json").write_text(json.dumps(win, indent=2))
    # whitelist
    if recs.get("whitelist"):
        wl = policy.get("whitelist") or {}
        for strat, d in recs["whitelist"].items():
            if "remove" in d:
                cur = set(map(str.upper, wl.get(strat, [])))
                for y in d["remove"]:
                    cur.discard(y.upper())
                wl[strat] = sorted(cur)
        changes["whitelist.json"] = wl
        if not dry:
            Path(POLICY_DIR / "whitelist.json").write_text(json.dumps(wl, indent=2))
    # risk
    if recs.get("risk"):
        rk = policy.get("risk") or {}
        rk.update(recs["risk"])
        changes["risk.json"] = rk
        if not dry:
            Path(POLICY_DIR / "risk.json").write_text(json.dumps(rk, indent=2))
    return {"dry": dry, "changes": changes}