import os
import time
import json
import socket
import uuid
import requests

BASE_URL = os.getenv("BASE_URL", "http://localhost:10000").rstrip("/")

EXIT_PATH = os.getenv("EXIT_PATH", "/worker/exit")
EXIT_INTERVAL_SEC = int(float(os.getenv("EXIT_INTERVAL_SEC", "30") or 30))

# Entry engine tick
SCAN_PATH = os.getenv("SCAN_PATH", "/worker/scan_entries")
SCAN_INTERVAL_SEC = int(float(os.getenv("SCAN_INTERVAL_SEC", str(EXIT_INTERVAL_SEC)) or EXIT_INTERVAL_SEC))

WORKER_SECRET = os.getenv("WORKER_SECRET", "")
DRY_RUN = os.getenv("SCAN_DRY_RUN", "0").strip().lower() in ("1", "true", "yes", "on")
HOSTNAME = os.getenv("HOSTNAME", socket.gethostname() or "unknown")
PID = os.getpid()
ROUTE_TRUTH_PATH = os.getenv("ROUTE_TRUTH_PATH", "/worker/route_truth")
WORKER_ROUTE_CONNECT_TIMEOUT_SEC = float(os.getenv("WORKER_ROUTE_CONNECT_TIMEOUT_SEC", "10") or 10)
WORKER_ROUTE_TIMEOUT_SEC = float(os.getenv("WORKER_ROUTE_TIMEOUT_SEC", "90") or 90)
ROUTE_TRUTH_CONNECT_TIMEOUT_SEC = float(os.getenv("ROUTE_TRUTH_CONNECT_TIMEOUT_SEC", str(WORKER_ROUTE_CONNECT_TIMEOUT_SEC)) or WORKER_ROUTE_CONNECT_TIMEOUT_SEC)
ROUTE_TRUTH_TIMEOUT_SEC = float(os.getenv("ROUTE_TRUTH_TIMEOUT_SEC", "10") or 10)


def _timeout_tuple(read_timeout: float | None = None) -> tuple[float, float]:
    connect_timeout = max(1.0, float(WORKER_ROUTE_CONNECT_TIMEOUT_SEC or 10))
    final_read_timeout = max(connect_timeout, float(read_timeout if read_timeout is not None else WORKER_ROUTE_TIMEOUT_SEC or 90))
    return (connect_timeout, final_read_timeout)


def _route_truth_timeout_tuple() -> tuple[float, float]:
    connect_timeout = max(1.0, float(ROUTE_TRUTH_CONNECT_TIMEOUT_SEC or WORKER_ROUTE_CONNECT_TIMEOUT_SEC or 10))
    read_timeout = max(connect_timeout, float(ROUTE_TRUTH_TIMEOUT_SEC or 10))
    return (connect_timeout, read_timeout)


def _post(path: str, payload: dict, timeout: float | tuple[float, float] | None = None):
    url = f"{BASE_URL}{path}"
    headers = {"x-request-id": f"worker-{int(time.time()*1000)}"}
    started = time.time()
    req_timeout = timeout if timeout is not None else _timeout_tuple()
    r = requests.post(url, json=payload, timeout=req_timeout, headers=headers)
    elapsed_ms = round((time.time() - started) * 1000.0, 3)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text[:1000]}
    return r.status_code, body, elapsed_ms


def _base_payload(kind: str, seq: int, interval_sec: int) -> dict:
    utc_now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    payload = {
        "heartbeat_kind": kind,
        "heartbeat_utc": utc_now,
        "heartbeat_ts": time.time(),
        "heartbeat_seq": int(seq),
        "loop_interval_sec": int(interval_sec),
        "loop_pid": int(PID),
        "heartbeat_source": f"background-worker@{HOSTNAME}",
        "worker_request_id": f"{kind}-{seq}-{uuid.uuid4().hex[:8]}",
    }
    if WORKER_SECRET:
        payload["worker_secret"] = WORKER_SECRET
    return payload




def _post_route_truth(base_payload: dict, *, phase: str, target_path: str, status_code: int | None = None, elapsed_ms: float | None = None, ok: bool | None = None, error: str | None = None, response_excerpt: str | None = None):
    payload = dict(base_payload or {})
    payload.update({
        "worker_kind": payload.get("heartbeat_kind") or payload.get("worker_kind"),
        "phase": phase,
        "ok": ok,
        "target_base_url": BASE_URL,
        "target_path": target_path,
        "target_url": f"{BASE_URL}{target_path}",
        "status_code": status_code,
        "elapsed_ms": elapsed_ms,
        "auth_present": bool(WORKER_SECRET),
        "error": error,
        "response_excerpt": (response_excerpt or "")[:400],
        "route_truth_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })
    try:
        _post(ROUTE_TRUTH_PATH, payload, timeout=_route_truth_timeout_tuple())
    except Exception as e:
        print(json.dumps({"kind": "route_truth_post", "phase": phase, "target_path": target_path, "error": str(e)}, default=str)[:2000])

def tick_exit(seq: int):
    payload = _base_payload("exit", seq, EXIT_INTERVAL_SEC)
    _post_route_truth(payload, phase="attempt", target_path=EXIT_PATH, ok=True)
    try:
        code, body, elapsed_ms = _post(EXIT_PATH, payload, timeout=_timeout_tuple())
        _post_route_truth(payload, phase="result", target_path=EXIT_PATH, status_code=code, elapsed_ms=elapsed_ms, ok=(200 <= int(code) < 300), response_excerpt=json.dumps(body, default=str)[:400])
        print(json.dumps({"kind": "exit_tick", "seq": seq, "code": code, "elapsed_ms": elapsed_ms, "body": body}, default=str)[:4000])
    except Exception as e:
        _post_route_truth(payload, phase="post_failed", target_path=EXIT_PATH, ok=False, error=str(e))
        print(json.dumps({"kind": "exit_tick", "seq": seq, "error": str(e)}, default=str)[:2000])


def tick_scan(seq: int):
    payload = _base_payload("scan", seq, SCAN_INTERVAL_SEC)
    payload["dry_run"] = DRY_RUN
    _post_route_truth(payload, phase="attempt", target_path=SCAN_PATH, ok=True)
    try:
        code, body, elapsed_ms = _post(SCAN_PATH, payload, timeout=_timeout_tuple())
        _post_route_truth(payload, phase="result", target_path=SCAN_PATH, status_code=code, elapsed_ms=elapsed_ms, ok=(200 <= int(code) < 300), response_excerpt=json.dumps(body, default=str)[:400])
        print(json.dumps({"kind": "scan_tick", "seq": seq, "code": code, "elapsed_ms": elapsed_ms, "body": body}, default=str)[:4000])
    except Exception as e:
        _post_route_truth(payload, phase="post_failed", target_path=SCAN_PATH, ok=False, error=str(e))
        print(json.dumps({"kind": "scan_tick", "seq": seq, "error": str(e)}, default=str)[:2000])


if __name__ == "__main__":
    last_exit = 0.0
    last_scan = 0.0
    exit_seq = 0
    scan_seq = 0

    while True:
        now = time.time()

        if now - last_exit >= EXIT_INTERVAL_SEC:
            exit_seq += 1
            tick_exit(exit_seq)
            last_exit = now

        if now - last_scan >= SCAN_INTERVAL_SEC:
            scan_seq += 1
            tick_scan(scan_seq)
            last_scan = now

        time.sleep(1)
