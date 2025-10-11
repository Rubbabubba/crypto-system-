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

# Option A: uvicorn directly (simple & fine for Render)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "%PORT%"]

# Option B: gunicorn with uvicorn worker (comment Option A if you use this)
# CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "-w", "2", "-t", "120", "-b", "0.0.0.0:$PORT", "app:app"]
