FROM python:3.12-slim
WORKDIR /app
COPY apps/kiosk/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY apps/kiosk/app.py .
COPY apps/kiosk/templates/ ./templates/
COPY apps/shared/ ./shared/
ENV PYTHONPATH=/app/shared:/app
EXPOSE 8080
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "60"]
