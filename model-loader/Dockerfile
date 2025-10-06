FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY model_loader.py .

# Create data directory
RUN mkdir -p /app/data

CMD ["python", "model_loader.py"]