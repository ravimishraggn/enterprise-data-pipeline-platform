FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run producer directly (no FastAPI in Phase 1)
CMD ["python", "producer.py"]
