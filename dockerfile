# Dockerfile — Crypto Trading Bots
# Build: v2.0.0 (2025-10-11)

FROM python:3.11-slim

# ---- system setup ----
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=10000 \
    TZ=America/Chicago

# Install minimal OS deps (certs, tzdata, build base for pandas/numpy wheels if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates tzdata curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only requirements first for better layer caching
COPY requirements.txt /app/requirements.txt

# Install python deps
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Security: drop root
RUN useradd -m runner && chown -R runner:runner /app
USER runner

# Expose service port
EXPOSE 10000

# Healthcheck (basic liveness; adjust path if needed)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:${PORT}/health || exit 1

# Default command
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "10000", "--access-log"]
