# Use the official Python runtime as base image
FROM python:3.13-slim

# Set working directory in the container
WORKDIR /app

# Install system dependencies for ClickHouse client
RUN apt-get update && apt-get install -y \
    gcc git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY README.md pyproject.toml ./

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Install Python dependencies
RUN pip install --upgrade pip && pip install uv && python -m uv sync

# Copy the application code
COPY bin/ ./bin/
COPY metranova/ ./metranova/
COPY pipelines/ ./pipelines/

#Create a directory for storing cache files
RUN mkdir caches

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Run the ClickHouse writer
CMD ["python", "-m", "uv", "run", "python", "bin/run.py"]
