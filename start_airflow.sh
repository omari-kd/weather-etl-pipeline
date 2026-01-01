#!/bin/bash

# ===============================
# Weather ETL Pipeline Starter
# ===============================

# Step 0: Navigate to project folder (script's location)
cd "$(dirname "$0")"

# Step 1: Check if Docker Desktop is running
echo "Checking Docker..."
docker info > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "⚠️ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Step 2: Build and start the containers
echo "🚀 Starting Airflow + ETL containers..."
docker compose up --build
#!/bin/bash

# ===============================
# Weather ETL Pipeline Starter
# ===============================

# Step 0: Navigate to project folder (script's location)
cd "$(dirname "$0")"

# Step 1: Check if Docker Desktop is running
echo "Checking Docker..."
docker info > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "⚠️ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Step 2: Build and start the containers
echo "🚀 Starting Airflow + ETL containers..."
docker compose up --build
#!/bin/bash

# ===============================
# Weather ETL Pipeline Starter
# ===============================

# Step 0: Navigate to project folder (script's location)
cd "$(dirname "$0")"

# Step 1: Check if Docker Desktop is running
echo "Checking Docker..."
docker info > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "⚠️ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Step 2: Build and start the containers
echo "🚀 Starting Airflow + ETL containers..."
docker compose up --build
