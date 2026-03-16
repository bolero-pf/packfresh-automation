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

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120"]
