FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libsm6 libxext6 libxrender-dev libgomp1 \
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

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
