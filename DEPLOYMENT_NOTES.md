# patch-035-contract-safe-7-symbol-lock-rb1-preservation

Changes:
- Lock live universe to the 7 symbols proven resolvable by scanner alignment:
  BTC/USD,ETH/USD,SOL/USD,ADA/USD,LINK/USD,AVAX/USD,DOT/USD
- Preserve tc0 + rb1 strategy defaults and behavior.
- Lower default Path B admitted symbol cap and scanner rank cap from 10 back to 7.
- Preserve lifecycle, exits, journaling, backfill, worker routing, and sizing behavior.
