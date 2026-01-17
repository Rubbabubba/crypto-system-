import os
import time
import requests

BASE_URL = os.getenv("BASE_URL", "http://localhost:10000").rstrip("/")
EXIT_PATH = os.getenv("EXIT_PATH", "/worker/exit")
EXIT_INTERVAL_SEC = int(float(os.getenv("EXIT_INTERVAL_SEC", "30") or 30))
WORKER_SECRET = os.getenv("WORKER_SECRET", "")


def tick():
    url = f"{BASE_URL}{EXIT_PATH}"
    payload = {"worker_secret": WORKER_SECRET} if WORKER_SECRET else {}
    try:
        r = requests.post(url, json=payload, timeout=20)
        print(f"exit tick {r.status_code}: {r.text[:500]}")
    except Exception as e:
        print(f"exit tick error: {e}")


if __name__ == "__main__":
    while True:
        tick()
        time.sleep(EXIT_INTERVAL_SEC)
