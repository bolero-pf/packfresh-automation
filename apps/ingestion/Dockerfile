FROM python:3.12-slim

WORKDIR /app

# Chromium + chromedriver power the CGC cert-lookup scraper in
# shared/cgc_client.py. CGC has no developer API and its cert page is
# behind a Cloudflare JS challenge, so a real headless browser is the
# only reliable path (same approach as apps/price_updater).
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libsm6 libxext6 libxrender-dev libgomp1 \
    fonts-dejavu-core \
    chromium chromium-driver \
    libnss3 libxss1 libasound2 libx11-xcb1 libxcomposite1 \
    libxcursor1 libxdamage1 libxi6 libxtst6 libxrandr2 \
    libatk-bridge2.0-0 libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver

COPY apps/ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/ingestion/ .
COPY apps/shared/ ./shared/

ENV PYTHONPATH=/app/shared:/app

EXPOSE 8080

# Single worker + threads so the in-process enrichment cache (_enrich_cache /
# _enrich_jobs) is shared across all requests. With 2 workers, the job-status
# poll would land on the wrong worker and never see 'complete', leaving the
# route session's spinner stuck indefinitely.
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "120"]
