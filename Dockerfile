# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY entrypoint.py .
COPY main.py .
COPY config.py .
COPY storage.py .
COPY stats.py .
COPY colors.py .
COPY process_artists.py .
COPY process_artist_textsearch.py .
COPY process_releasegroups.py .
COPY process_manual_entries.py .

# Optional: allow overriding the config path at runtime
ENV CONFIG_PATH=/app/data/config.ini

# Default entrypoint runs the scheduler for continuous operation
ENTRYPOINT ["python", "/app/entrypoint.py"]
