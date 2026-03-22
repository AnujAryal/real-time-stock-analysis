# Disclaimer: This tool is for educational and portfolio purposes only. It does not constitute financial advice. Always perform your own due diligence before trading. (See the "Caffeine Clause" in my LICENSE for details!)

---

# 🚀 End-to-End Stock Analytics Pipeline
[![Apache Spark](https://img.shields.io/badge/Data_Processing-Apache_Spark-orange?style=flat&logo=apachespark)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache_Airflow-017CEE?style=flat&logo=apacheairflow)](https://airflow.apache.org/)
[![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B?style=flat&logo=streamlit)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Infrastructure-Docker-2496ED?style=flat&logo=docker)](https://www.docker.com/)

A production-grade Data Engineering pipeline that automates the ingestion, transformation, and visualization of stock market data. This project demonstrates a **decoupled architecture** using a Medallion (Bronze/Gold) approach.

---

## 🏗️ Architecture
The system is built to handle heavy analytical compute separately from the user interface:

1. **Ingestion Layer (Bronze):** Airflow triggers a Spark job to pull historical data from Yahoo Finance API and stores it as raw Parquet files.
2. **Transformation Layer (Gold):** A secondary Spark job performs distributed Window Functions to calculate Technical Indicators (SMA, RSI, Volatility, Daily Returns).
3. **Serving Layer (Dashboard):** A Streamlit UI reads the optimized "Gold" Parquet files to provide interactive Plotly visualizations.

---

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow (Dockerized)
* **Processing:** PySpark (Spark 3.x)
* **Storage:** Parquet (Columnar Storage)
* **UI:** Streamlit & Plotly
* **Environment:** Docker Compose (Multi-container setup)

---

## ⚡ Key Engineering Challenges Solved
* **Idempotency:** The pipeline can be fully re-run from a "Cold Start." Wiping the data directory does not break the logic; Airflow simply re-ingests and re-builds the state.
* **Schema Evolution:** Implemented defensive header normalization (lowercase enforcement) to ensure the Spark-to-Streamlit handoff never fails due to casing changes.
* **Complex Spark Math:** Handled `DIVIDE_BY_ZERO` edge cases in distributed RSI calculations using `F.when` and `nullif` logic.
* **Performance:** Utilized `coalesce(1)` for the Gold layer to optimize read speeds for the Streamlit frontend.

---

## 🚀 Getting Started

### 1. Build the Environment
```bash
./setup_env.bat
docker compose up -d --build

2. Run the Pipeline
Open Airflow at http://localhost:8080.
Unpause the stock_explorer_manual DAG.
Trigger with a config JSON: {"ticker": "AAPL"}.

3. View Analytics
Open Streamlit at http://localhost:8501.
Select your ticker and hit "Refresh Data".

