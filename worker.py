import os
import time
import requests

BASE_URL = os.getenv("BASE_URL", "http://localhost:10000").rstrip("/")

EXIT_PATH = os.getenv("EXIT_PATH", "/worker/exit")
EXIT_INTERVAL_SEC = int(float(os.getenv("EXIT_INTERVAL_SEC", "30") or 30))

# Entry engine tick
SCAN_PATH = os.getenv("SCAN_PATH", "/worker/scan_entries")
SCAN_INTERVAL_SEC = int(float(os.getenv("SCAN_INTERVAL_SEC", str(EXIT_INTERVAL_SEC)) or EXIT_INTERVAL_SEC))

WORKER_SECRET = os.getenv("WORKER_SECRET", "")
DRY_RUN = os.getenv("SCAN_DRY_RUN", "0").strip().lower() in ("1", "true", "yes", "on")


def _post(path: str, payload: dict, timeout: int = 30):
    url = f"{BASE_URL}{path}"
    r = requests.post(url, json=payload, timeout=timeout)
    return r.status_code, r.text


def tick_exit():
    payload = {"worker_secret": WORKER_SECRET} if WORKER_SECRET else {}
    try:
        code, text = _post(EXIT_PATH, payload, timeout=20)
        print(f"exit tick {code}: {text[:500]}")
    except Exception as e:
        print(f"exit tick error: {e}")


def tick_scan():
    payload = {"worker_secret": WORKER_SECRET, "dry_run": DRY_RUN} if WORKER_SECRET else {"dry_run": DRY_RUN}
    try:
        code, text = _post(SCAN_PATH, payload, timeout=60)
        print(f"scan tick {code}: {text[:500]}")
    except Exception as e:
        print(f"scan tick error: {e}")


if __name__ == "__main__":
    last_exit = 0.0
    last_scan = 0.0

    while True:
        now = time.time()

        if now - last_exit >= EXIT_INTERVAL_SEC:
            tick_exit()
            last_exit = now

        if now - last_scan >= SCAN_INTERVAL_SEC:
            tick_scan()
            last_scan = now

        time.sleep(1)
