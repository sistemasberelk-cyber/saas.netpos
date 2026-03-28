FROM python:3.10-slim

# Install system dependencies (needed for Pillow/Barcodes and Postgres)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Copy Application Code
COPY . .

RUN mkdir -p static/barcodes static/product_images static/images backups \
    && chmod 755 static/barcodes static/product_images backups

# Expose Port (Render uses $PORT env var, but uvicorn needs explicit bind)
EXPOSE 8000

# Start Command
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
