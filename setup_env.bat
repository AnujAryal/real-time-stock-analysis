@echo off
setlocal

echo 🚀 Starting Environment Setup for Real-Time Stock Analytics...

:: 1. Get the current directory (Absolute Path)
set "CURRENT_DIR=%cd%"
:: Convert backslashes to forward slashes for Docker compatibility
set "CURRENT_DIR=%CURRENT_DIR:\=/%"

:: 2. Create the .env file
echo Creating .env file...
(
    echo # --- Infrastructure Paths ---
    echo HOST_PROJECT_PATH=%CURRENT_DIR%
    echo.
    echo # --- Airflow Config ---
    echo AIRFLOW_IMAGE_NAME=apache/airflow:2.8.0-python3.11
    echo AIRFLOW_UID=50000
    echo.
    echo # --- Spark Config ---
    echo SPARK_IMAGE_NAME=real-time-stock-analytics-spark-app
) > .env

:: 3. Create the data directory structure if it doesn't exist
echo Creating local data folders...
if not exist "data\raw" mkdir "data\raw"
if not exist "data\processed" mkdir "data\processed"
if not exist "logs" mkdir "logs"

:: 4. Add a dummy .gitkeep to ensure folders are tracked if empty
echo. > data\raw\.gitkeep
echo. > data\processed\.gitkeep

echo.
echo ✅ Setup Complete! 
echo ---------------------------------------------------
echo Your .env file has been generated with:
echo HOST_PROJECT_PATH=%CURRENT_DIR%
echo ---------------------------------------------------
echo 🚦 Next Step: Run 'docker compose up -d --build'
echo.
pause