# Panoptes live web service — lean image for Railway.
FROM python:3.11-slim

WORKDIR /app

# Install only the web-service deps (fast build; no torch/whisper).
COPY requirements-web.txt .
RUN pip install --no-cache-dir -r requirements-web.txt

# App + discovery catalogs + web assets (data/*.json are loaded at startup).
COPY . .

# The service only needs write access to its state directory. Never serve as root.
RUN useradd --create-home --shell /usr/sbin/nologin panoptes \
    && mkdir -p /app/state \
    && chown -R panoptes:panoptes /app
USER panoptes

# Railway injects $PORT; bind to it (fall back to 8000 for local `docker run`).
ENV PORT=8000
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:' + __import__('os').environ.get('PORT', '8000') + '/health/ready', timeout=3)"
CMD ["sh", "-c", "uvicorn apb.api.main:app --host 0.0.0.0 --port ${PORT}"]
