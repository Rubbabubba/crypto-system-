import os
import time
import json
import socket
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


def _post(path: str, payload: dict, timeout: int = 30):
    url = f"{BASE_URL}{path}"
    headers = {"x-request-id": f"worker-{int(time.time()*1000)}"}
    started = time.time()
    r = requests.post(url, json=payload, timeout=timeout, headers=headers)
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
    }
    if WORKER_SECRET:
        payload["worker_secret"] = WORKER_SECRET
    return payload


def tick_exit(seq: int):
    payload = _base_payload("exit", seq, EXIT_INTERVAL_SEC)
    try:
        code, body, elapsed_ms = _post(EXIT_PATH, payload, timeout=20)
        print(json.dumps({"kind": "exit_tick", "seq": seq, "code": code, "elapsed_ms": elapsed_ms, "body": body}, default=str)[:4000])
    except Exception as e:
        print(json.dumps({"kind": "exit_tick", "seq": seq, "error": str(e)}, default=str)[:2000])


def tick_scan(seq: int):
    payload = _base_payload("scan", seq, SCAN_INTERVAL_SEC)
    payload["dry_run"] = DRY_RUN
    try:
        code, body, elapsed_ms = _post(SCAN_PATH, payload, timeout=60)
        print(json.dumps({"kind": "scan_tick", "seq": seq, "code": code, "elapsed_ms": elapsed_ms, "body": body}, default=str)[:4000])
    except Exception as e:
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
