# Panoptes live web service — lean image for Railway.
FROM python:3.11-slim

WORKDIR /app

# Install only the web-service deps (fast build; no torch/whisper).
COPY requirements-web.txt .
RUN pip install --no-cache-dir -r requirements-web.txt

# App + discovery catalogs + web assets (data/*.json are loaded at startup).
COPY . .

# Railway injects $PORT; bind to it (fall back to 8000 for local `docker run`).
ENV PORT=8000
EXPOSE 8000
CMD ["sh", "-c", "uvicorn apb.api.main:app --host 0.0.0.0 --port ${PORT}"]
