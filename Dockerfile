# Multi-stage Dockerfile for Kafka Ops Agent
FROM python:3.11-slim as builder

# Set build arguments
ARG BUILD_DATE
ARG VERSION
ARG VCS_REF

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
COPY requirements-dev.txt .

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Install the package
RUN pip install --no-cache-dir -e .

# Production stage
FROM python:3.11-slim as production

# Set labels for metadata
LABEL maintainer="Kafka Ops Team" \
      version="${VERSION}" \
      description="Kafka Operations Agent - Self-Service Kafka Platform" \
      build-date="${BUILD_DATE}" \
      vcs-ref="${VCS_REF}" \
      org.opencontainers.image.title="Kafka Ops Agent" \
      org.opencontainers.image.description="Self-service Kafka cluster management platform" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Kafka Ops Team" \
      org.opencontainers.image.licenses="MIT"

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    netcat-traditional \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r kafka-ops && useradd -r -g kafka-ops kafka-ops

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY --from=builder /app /app
WORKDIR /app

# Create directories and set permissions
RUN mkdir -p /app/logs /app/data /app/config \
    && chown -R kafka-ops:kafka-ops /app \
    && chmod +x /app/scripts/*.py

# Switch to non-root user
USER kafka-ops

# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    API_HOST=0.0.0.0 \
    API_PORT=8000 \
    MONITORING_HOST=0.0.0.0 \
    MONITORING_PORT=8080

# Expose ports
EXPOSE 8000 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command
CMD ["python", "-m", "kafka_ops_agent.api"]