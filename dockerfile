# Dockerfile — production-ready for Render
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY . .

# Render provides $PORT; default to 10000 locally
ENV PORT=10000
EXPOSE 10000

# Gunicorn serves Flask app
# - 2 workers is fine for this light API; bump if needed
# - gthread worker for simple concurrency
# - bind to 0.0.0.0:$PORT so Render health checks pass
CMD ["gunicorn", "-w", "2", "-k", "gthread", "-t", "120", "-b", "0.0.0.0:$PORT", "app:app"]
