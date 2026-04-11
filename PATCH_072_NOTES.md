# Patch 072

- Unifies worker connect/read timeout handling with env-driven tuples.
- Route truth posting now honors dedicated/env-driven timeout settings instead of fixed values.
- Adds account-truth cache/grace behavior so entry health can continue using last-known-good broker truth within configured grace windows.
- Preserves lifecycle/journal/reconcile paths; no strategy logic changed in this patch.
