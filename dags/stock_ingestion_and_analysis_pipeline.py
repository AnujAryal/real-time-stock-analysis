import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount

# --- THE PORTABLE PATH LOGIC ---
env_path = os.getenv('HOST_PROJECT_PATH')
if env_path and len(env_path) > 1:
    DATA_PATH_HOST = f"{env_path}/data"
else:
    # Hardcoded fallback for your machine
    DATA_PATH_HOST = "C:/Users/Anuj/Desktop/tutorials/real-time-stock-analytics/data"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'stock_explorer_manual',
    default_args=default_args,
    description='Manual Stock Ingestion and Analysis',
    schedule_interval=None,  # <--- Setting this to None stops all auto-triggers
    catchup=False,
    params={"ticker": "AAPL"}, # Default value in the UI
) as dag:

    data_mount = [
        Mount(source=DATA_PATH_HOST, target="/app/data", type="bind")
    ]

    ingest = DockerOperator(
        task_id='ingest_stock',
        image='real-time-stock-analytics-spark-app',
        docker_url="tcp://docker-proxy:2375",
        network_mode="real-time-stock-analytics_default",
        mount_tmp_dir=False,
        auto_remove=True,
        command="python /app/spark_jobs/ingest_stock_data.py {{ params.ticker }}",
        mounts=data_mount,
    )

    analyze = DockerOperator(
        task_id='analyze_stock',
        image='real-time-stock-analytics-spark-app',
        docker_url="tcp://docker-proxy:2375",
        network_mode="real-time-stock-analytics_default",
        mount_tmp_dir=False,
        auto_remove=True,
        command="python /app/spark_jobs/calculate_metrics.py {{ params.ticker }}",
        mounts=data_mount,
    )

    ingest >> analyze