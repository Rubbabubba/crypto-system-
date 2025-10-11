# br_router.py — choose the active broker at import time to keep strategies decoupled
import os
if (os.getenv("BROKER","kraken").lower() == "kraken") or os.getenv("KRAKEN_TRADING","0") in ("1","true","True"):
    from broker_kraken import *  # noqa
else:
    from broker import *  # Alpaca fallback
