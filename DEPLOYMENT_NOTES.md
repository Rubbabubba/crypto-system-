# patch-045a-profitability-filter-hotfix

Changes:
- Add profitability enforcement filters to tc0 and rb1.
- Add fee-aware move-to-cost gate using fees, slippage, and optional live spread.
- Tighten rb1 with required upward momentum, ATR floor, and narrower near-breakout distance cap.
- Preserve journal persistence, P&L truth, and prior provenance mapping behavior.
