FROM python:3.12-slim
WORKDIR /app
COPY apps/card_browser/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY apps/card_browser/ .
COPY apps/shared/ ./shared/
ENV PYTHONPATH=/app/shared:/app
EXPOSE 5004
CMD ["python", "app.py"]
