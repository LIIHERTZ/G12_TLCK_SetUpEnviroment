FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY dashboard.py .

# Create templates directory
RUN mkdir -p /app/templates
RUN mkdir -p /app/data

# Expose port for Flask app
EXPOSE 5000

CMD ["python", "dashboard.py"]