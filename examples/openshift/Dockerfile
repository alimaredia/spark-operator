# Dockerfile for Docling + PySpark (PVC-based storage)
# Use Official apache/spark image (already has PySpark + Java)
FROM apache/spark:3.5.7-java17-python3

LABEL maintainer="roburishabh@outlook.com"
LABEL version="2.0"
LABEL description="Docling + PySpark for distributed PDF processing (PVC-based)"

# Set the working directory
WORKDIR /app

# Install System dependencies (Linux)
USER root 
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-eng \
    libtesseract-dev \
    libleptonica-dev \
    poppler-utils \
    libgomp1 \
    libglib2.0-0 \
    libgl1 \
    pkg-config \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# --- OpenShift Arbitrary UID Compatibility ---
# OpenShift assigns arbitrary non-root UID at runtime, but it's always a member of GID 0
# All directories must be owned by group 0 and group-writable (g=u)
USER 0

# Set Spark directories to be owned by group 0 and group-writable
# Required for: reading jars, writing to work-dir/logs
RUN chgrp -R 0 /opt/spark && \
    chmod -R g=u /opt/spark && \
    mkdir -p /opt/spark/work-dir /opt/spark/logs && \
    chgrp -R 0 /opt/spark/work-dir /opt/spark/logs && \
    chmod -R 775 /opt/spark/work-dir /opt/spark/logs

# Ensure /tmp is writable (usually is, but explicit for verification)
RUN chmod 1777 /tmp

# Copy requirements first (for better caching)
COPY requirements-docker.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-docker.txt

# Set environment variables
ENV PYTHONPATH="/app:$PYTHONPATH"
ENV HOME=/home/spark

# Create directories for PVC mounts and HOME
# /app/assets  - input PVC mount (read-only at runtime)
# /app/output  - output PVC mount (read-write at runtime)
# /home/spark  - HOME directory for Spark temp files
RUN mkdir -p /app/assets /app/output /app/scripts /home/spark && \
    chgrp -R 0 /app /home/spark && \
    chmod -R g=u /app /home/spark && \
    chmod -R 775 /app/output /home/spark

# Copy application code ONLY (no data files - they come from PVC)
COPY scripts/ /app/scripts/
RUN chgrp -R 0 /app/scripts && \
    chmod -R g=u /app/scripts
