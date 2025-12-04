#!/bin/bash
# Run full covid ETL: Ingest -> Transform

set -euo pipefail # safer script transform

# Absolute project directory
PROJECT_DIR="/home/kojo/Projects/Data-Engineering-Projects/Weather-ETL-Pipeline"

# Activate virtual environment
source "$PROJECT_DIR/.venv/bin/activate"

# Move to project directory
cd "$PROJECT_DIR" || exit

echo "Starting Weather ETL pipeline..."

echo "Step 2: Ingesting raw data from API..."
python3 "$PROJECT_DIR/src/etl/ingest_weather.py"
echo "Ingestion complete"

echo "ETL pipeline finished successfully."

# deactivate venv
deactivate
