# -----------------------------
# Base Image
# -----------------------------
FROM python:3.11-slim

# -----------------------------
# Environment setup
# -----------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# -----------------------------
# Work directory
# -----------------------------
WORKDIR /app

# -----------------------------
# System dependencies (important for auth + SSL)
# -----------------------------
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Install Python dependencies
# -----------------------------
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# Copy application code
# -----------------------------
COPY . .

# -----------------------------
# Cloud Run port
# -----------------------------
EXPOSE 8080

# -----------------------------
# Start API
# -----------------------------
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]