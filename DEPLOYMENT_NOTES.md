Crypto System Patch 001 — Runtime Truth + Release Identity Foundation

Scope
- Adds /build, /runtime, and /ready to the main crypto service.
- Fixes missing PATCH_BUILD runtime metadata reference in /health.
- Adds lightweight artifact integrity metadata for deployed files.
- No strategy or execution logic changes.

Verify after deploy
- GET /build
- GET /health
- GET /runtime
- GET /ready
- GET /diagnostics/runtime
