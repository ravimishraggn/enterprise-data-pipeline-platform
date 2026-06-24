FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy both consumer scripts — docker-compose selects which to run via `command:`
COPY consumer_group_a.py .
COPY consumer_group_b.py .

# Default: consumer A (override in docker-compose)
CMD ["python", "consumer_group_a.py"]
