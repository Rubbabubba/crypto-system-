#!/usr/bin/env bash
set -euo pipefail

# Web process
exec uvicorn app.app:app --host 0.0.0.0 --port "${PORT:-10000}"
