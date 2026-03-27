FROM python:3.12-slim
WORKDIR /app
COPY apps/card_manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY apps/card_manager/app.py .
COPY apps/card_manager/templates/ ./templates/
COPY apps/shared/ ./shared/
ENV PYTHONPATH=/app/shared:/app
EXPOSE 8080
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "120"]
