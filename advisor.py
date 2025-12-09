from __future__ import annotations
import os, json, datetime as dt
from pathlib import Path
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque

POLICY_DIR = Path(os.getenv("POLICY_CFG_DIR", "policy_config"))
JOURNAL_PATH = Path(os.getenv("JOURNAL_PATH", "./journal_v2.jsonl"))
TELEMETRY_PATH = Path(os.getenv("TELEMETRY_PATH", "./telemetry_v2.jsonl"))
TZ = dt.timezone(dt.timedelta(hours=-5))
_DOWS = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

def _load_telemetry() -> List[dict]:
    return _load_jsonl(TELEMETRY_PATH)

def _load_jsonl(path: Path) -> List[dict]:
    rows=[]; 
    if not path.exists(): return rows
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: rows.append(json.loads(line))
            except Exception: pass
    return rows

def _to_local(ts: float) -> dt.datetime: return dt.datetime.fromtimestamp(float(ts), tz=dt.timezone.utc).astimezone(TZ)
def _date_only(ts: float) -> dt.date: return _to_local(ts).date()

def _read_policy()->Dict[str,Any]:
    def rd(name):
        p=POLICY_DIR/name
        try: return json.loads(p.read_text())
        except Exception: return {}
    return {"windows":rd("windows.json"),"whitelist":rd("whitelist.json"),"risk":rd("risk.json")}

def _is_in_window(strategy: str, ts: float, policy: dict) -> bool:
    w=(policy.get("windows") or {}).get("windows",{}).get(strategy,{})
    hours=set((w.get("hours") or [])); dows=set((w.get("dows") or _DOWS))
    t=_to_local(ts); return (t.hour in hours if hours else True) and (_DOWS[t.weekday()] in dows if dows else True)

def _norm_pair(s: str) -> str:
    s=(s or "").upper(); return s if "/" in s else (s[:-3]+"/USD" if s.endswith("USD") and len(s)>3 else s)

def _fifo_stats(rows: List[dict]):
    lots=defaultdict(lambda: deque()); stats=defaultdict(lambda: {"realized":0.0,"fees":0.0,"in_pnl":0.0,"out_pnl":0.0,"trades":0,"wins":0,"hours":defaultdict(float)})
    policy=_read_policy()
    for r in sorted(rows,key=lambda x:x.get("filled_ts") or x.get("ts") or 0):
        strat=(r.get("strategy") or "import").lower(); sym=(r.get("symbol") or "").upper(); side=r.get("side")
        px=float(r.get("price") or 0.0); vol=float(r.get("vol") or 0.0); fee=float(r.get("fee") or 0.0); ts=float(r.get("filled_ts") or r.get("ts") or 0.0)
        if not (side in ("buy","sell") and px>0 and vol>0 and ts>0): continue
        key=(strat,sym); stats[key]["trades"]+=1; stats[key]["fees"]+=fee
        if side=="buy": lots[key].append([vol,px]); continue
        rem=vol; realized=0.0
        while rem>1e-12 and lots[key]:
            q,cpx=lots[key][0]; take=min(q,rem); realized+=(px-cpx)*take; q-=take; rem-=take
            if q<=1e-12: lots[key].popleft()
            else: lots[key][0][0]=q
        pnl=realized-fee; 
        if pnl>0: stats[key]["wins"]+=1
        stats[key]["hours"][_to_local(ts).hour]+=pnl
        if _is_in_window(strat,ts,policy): stats[key]["in_pnl"]+=pnl
        else: stats[key]["out_pnl"]+=pnl
    by_strategy,by_symbol=defaultdict(dict),defaultdict(dict)
    for (strat,sym),st in stats.items():
        net=st["in_pnl"]+st["out_pnl"]; wr=st["wins"]/max(1,st["trades"])
        row={"trades":st["trades"],"win_rate":wr,"net_pnl":net,"in_window_pnl":st["in_pnl"],"out_window_pnl":st["out_pnl"],"fees":st["fees"],"hours":dict(st["hours"])}
        by_strategy[strat][sym]=row; by_symbol[sym][strat]=row
    return {"by_strategy":by_strategy,"by_symbol":by_symbol}

def _recommend(policy: Dict[str, Any], kpis: Dict[str, Any]):
    rec={"windows":{}, "whitelist":{}, "reassign":[]}
    win_cfg=(policy.get("windows") or {}).get("windows",{})
    for strat,sym_map in (kpis.get("by_strategy") or {}).items():
        from collections import defaultdict as _dd; hour=_dd(float)
        for row in sym_map.values():
            for h,v in (row.get("hours") or {}).items(): hour[int(h)] += float(v)
        if not hour: continue
        hours_now=set((win_cfg.get(strat,{}) or {}).get("hours",[]))
        top=sorted(hour.items(), key=lambda kv: kv[1], reverse=True)[:3]
        bot=sorted(hour.items(), key=lambda kv: kv[1])[:2]
        add=[h for h,v in top if v>0 and h not in hours_now]; drop=[h for h,v in bot if v<0 and h in hours_now]
        if add or drop: rec["windows"][strat]={"add_hours":add,"drop_hours":drop}
    wl={k:set(map(str.upper,v)) for k,v in (policy.get("whitelist") or {}).items()}
    def norm(s): s=s.upper(); return s if "/" in s else (s[:-3]+"/USD" if s.endswith("USD") and len(s)>3 else s)
    for strat,sym_map in (kpis.get("by_strategy") or {}).items():
        cur={norm(x) for x in (wl.get(strat) or set())}
        losers=[s for s,r in sym_map.items() if (r["net_pnl"]<0 and norm(s) in cur and r["trades"]>=3)]
        adders=[s for s,r in sym_map.items() if (r["net_pnl"]>0 and norm(s) not in cur and r["trades"]>=3)]
        if losers: rec["whitelist"].setdefault(strat,{})["remove"]=sorted(losers)
        if adders: rec["whitelist"].setdefault(strat,{})["add"]=sorted(adders)
    for sym,strat_map in (kpis.get("by_symbol") or {}).items():
        ranked=sorted(((st,r["net_pnl"],r["trades"]) for st,r in strat_map.items()), key=lambda x:x[1], reverse=True)
        if len(ranked)>=2:
            best_st,best_pnl,best_trades=ranked[0]; worst_st,worst_pnl,worst_trades=ranked[-1]
            if best_pnl>0 and best_trades>=3 and worst_pnl<0 and worst_trades>=3 and best_st!=worst_st:
                rec["reassign"].append({"symbol":sym,"from":worst_st,"to":best_st,"delta":round(best_pnl-worst_pnl,2)})
    return rec

def analyze_range(start_date: str, end_date: str)->dict:
    import datetime as _dt
    S=_dt.datetime.strptime(start_date,"%Y-%m-%d").date(); E=_dt.datetime.strptime(end_date,"%Y-%m-%d").date()
    rows=_load_jsonl(JOURNAL_PATH)
    rows=[r for r in rows if (r.get("price") and r.get("vol") and r.get("side") in ("buy","sell") and S <= _date_only(float(r.get("filled_ts") or r.get("ts") or 0)) <= E)]
    k=_fifo_stats(rows); policy=_read_policy(); rec=_recommend(policy,k)
    return {"range":{"start":S.isoformat(),"end":E.isoformat()},"kpis":k,"recommendations":rec,"policy_snapshot":policy}

def analyze_week(end_date: str, days: int=7)->dict:
    import datetime as _dt
    E=_dt.datetime.strptime(end_date,"%Y-%m-%d").date(); S=E-_dt.timedelta(days=days-1)
    return analyze_range(S.isoformat(), E.isoformat())

def analyze_day(date_str: str|None=None)->dict:
    import datetime as _dt
    day=_dt.datetime.strptime(date_str,"%Y-%m-%d").date() if date_str else _dt.datetime.now(TZ).date()
    rows=_load_jsonl(JOURNAL_PATH); rows=[r for r in rows if _date_only(float(r.get("filled_ts") or r.get("ts") or 0))==day]
    k=_fifo_stats(rows); policy=_read_policy(); rec=_recommend(policy,k)
    return {"date":day.isoformat(),"kpis":k,"recommendations":rec,"policy_snapshot":policy}

def write_policy_updates(recs: Dict[str,Any], dry: bool=True)->Dict[str,Any]:
    changes={}; policy=_read_policy()
    if recs.get("windows"):
        win=policy.get("windows") or {}; win.setdefault("windows",{})
        for strat,d in recs["windows"].items():
            cur=win["windows"].setdefault(strat,{"hours":[],"dows":_DOWS}); hours=set(cur.get("hours",[]))
            for h in d.get("add_hours",[]): hours.add(int(h))
            for h in d.get("drop_hours",[]): hours.discard(int(h))
            cur["hours"]=sorted(hours)
        changes["windows.json"]=win
        if not dry: (POLICY_DIR/"windows.json").write_text(json.dumps(win,indent=2))
    if recs.get("whitelist"):
        wl=policy.get("whitelist") or {}
        def norm(s): s=s.upper(); return s if "/" in s else (s[:-3]+"/USD" if s.endswith("USD") and len(s)>3 else s)
        for strat,d in recs["whitelist"].items():
            cur=set(map(norm, wl.get(strat,[])))
            for y in d.get("remove",[]): cur.discard(norm(y))
            for y in d.get("add",[]):    cur.add(norm(y))
            wl[strat]=sorted(cur)
        changes["whitelist.json"]=wl
        if not dry: (POLICY_DIR/"whitelist.json").write_text(json.dumps(wl,indent=2))
    if recs.get("risk"):
        rk=policy.get("risk") or {}; rk.update(recs["risk"]); changes["risk.json"]=rk
        if not dry: (POLICY_DIR/"risk.json").write_text(json.dumps(rk,indent=2))
    return {"dry":dry,"changes":changes}