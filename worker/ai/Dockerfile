FROM python:3.10-slim

# -----------------------------
# Work directory
# -----------------------------
WORKDIR /app

# -----------------------------
# System dependencies (ffmpeg + OpenGL + ODBC)
# -----------------------------
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libgl1 \
    libglib2.0-0 \
    unixodbc \
    unixodbc-dev \
    curl \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install ODBC Driver 17 for SQL Server
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Copy the entire render service
# This includes /common, /handlers, and main.py
# -----------------------------
COPY . /app

# -----------------------------
# Install dependencies for render job only
# -----------------------------
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# Python import root
# -----------------------------
ENV PYTHONPATH=/app

# -----------------------------
# Cloud Run Job entrypoint
# -----------------------------
CMD ["python", "main.py"]