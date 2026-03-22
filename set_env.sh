#!/bin/bash

echo "🚀 Starting Environment Setup for Real-Time Stock Analytics..."

# 1. Get the current directory (Absolute Path)
CURRENT_DIR=$(pwd)

# 2. Create the .env file
echo "Creating .env file..."
cat <<EOF > .env
# --- Infrastructure Paths ---
HOST_PROJECT_PATH=$CURRENT_DIR

# --- Airflow Config ---
AIRFLOW_IMAGE_NAME=apache/airflow:2.8.0-python3.11
AIRFLOW_UID=$(id -u)
AIRFLOW_GID=0

# --- Spark Config ---
SPARK_IMAGE_NAME=real-time-stock-analytics-spark-app
EOF

# 3. Create the data directory structure
echo "Creating local data folders..."
mkdir -p data/raw data/processed logs

# 4. Add dummy .gitkeep files
touch data/raw/.gitkeep
touch data/processed/.gitkeep

# 5. Set permissions (Linux/Mac specific)
chmod +x setup_env.sh

echo ""
echo "✅ Setup Complete!"
echo "---------------------------------------------------"
echo "Your .env file has been generated with:"
echo "HOST_PROJECT_PATH=$CURRENT_DIR"
echo "AIRFLOW_UID=$(id -u)"
echo "---------------------------------------------------"
echo "🚦 Next Step: Run 'docker compose up -d --build'"
echo ""