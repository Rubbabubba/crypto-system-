Crypto System Patch 6 — Startup Self-Check + Lockout

Drop-in patch contents for the main crypto system.

What changed:
- Added startup self-check on app boot.
- Added configurable startup lockout if critical reconcile anomalies are detected.
- Added /diagnostics/startup_self_check.
- Added startup_self_check block to /diagnostics/runtime.

Recommended verification:
- GET /diagnostics/startup_self_check
- GET /diagnostics/startup_self_check?rerun=1
- GET /diagnostics/runtime
