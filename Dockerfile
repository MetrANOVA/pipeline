# Use the official Python runtime as base image
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# Install system dependencies for ClickHouse client
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY ch_writer.py .

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Run the ClickHouse writer
CMD ["python", "ch_writer.py"]