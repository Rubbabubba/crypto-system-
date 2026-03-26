# Patch 022 Deployment Notes

- Hotfixes the active `/worker/exit` crash by defining `PENDING_EXIT_TTL_SEC` in the live Patch 021 code path.
- Adds startup dependency guard coverage for required exit lifecycle helpers/constants.
- Preserves Patch 021 exit execution truth instrumentation and Patch 018 balance truth behavior.
