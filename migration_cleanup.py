#!/usr/bin/env python3
"""
One-time migration script (Option B: Normalize + Reset Positions)

- Normalizes symbol names in trades table (e.g., AVAXUSD -> AVAX/USD, LINKUSD -> LINK/USD)
- Marks all existing trades as 'legacy' strategy so the live system starts fresh.
  (New trades will use strategy from USERREF_MAP or fall back to 'misc'.)
"""

import os
import sqlite3
from pathlib import Path

def _pick_data_dir() -> Path:
    candidate = os.getenv("DATA_DIR", "./data")
    p = Path(candidate)
    try:
        p.mkdir(parents=True, exist_ok=True)
        # probe writeability
        test = p / ".wtest"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
        return p
    except Exception:
        p = Path("./_data_fallback")
        p.mkdir(parents=True, exist_ok=True)
        return p

def main() -> None:
    data_dir = _pick_data_dir()
    db_path = data_dir / "journal.db"
    if not db_path.exists():
        print(f"ERROR: journal DB not found at {db_path}")
        return

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # 1) Normalize symbols that we know are duplicated (extend as needed)
    symbol_fixes = {
        "AVAXUSD": "AVAX/USD",
        "LINKUSD": "LINK/USD",
    }
    total_sym_updates = 0
    for bad, good in symbol_fixes.items():
        print(f"Normalizing symbol {bad} -> {good}")
        cur.execute("UPDATE trades SET symbol=? WHERE symbol=?", (good, bad))
        total_sym_updates += cur.rowcount
    print(f"Symbol normalization complete. Rows updated: {total_sym_updates}")

    # 2) Mark all existing trades as legacy (reset for new system)
    print("Marking existing trades with strategy in (NULL, '', 'misc') as 'legacy'...")
    cur.execute(
        "UPDATE trades SET strategy='legacy' "
        "WHERE strategy IS NULL OR TRIM(strategy)='' OR strategy='misc'"
    )
    legacy_updates = cur.rowcount
    print(f"Legacy strategy updates: {legacy_updates} rows")

    conn.commit()
    conn.close()
    print("Migration complete.")

if __name__ == "__main__":
    main()
