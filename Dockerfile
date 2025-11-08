# Python runtime for MariaDB ETL
FROM python:3.11-slim

USER root

# Install netcat for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
  && rm -rf /var/lib/apt/lists/*

# Workdir
WORKDIR /app

# Python dependencies (pyspark, etc.)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application
COPY etl /app/etl
COPY .env.example /app/.env.example

# Entrypoint
COPY etl/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
