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
# Install dependencies
# -----------------------------
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# Copy application code
# -----------------------------
COPY . .

# -----------------------------
# Cloud Run uses port 8080
# -----------------------------
EXPOSE 8080

# -----------------------------
# Start application (production)
# -----------------------------
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]