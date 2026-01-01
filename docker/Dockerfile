# ./docker/Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the src folder
COPY src ./src

# Default command
CMD ["python", "src/etl/ingest_weather.py"]
