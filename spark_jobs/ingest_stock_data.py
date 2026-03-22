import sys
from pyspark.sql import SparkSession
import pandas as pd
import yfinance as yf

def ingest_stock(ticker):
    spark = SparkSession.builder.appName(f"Ingest_{ticker}").getOrCreate()
    
    # Pulling 5 years of data for the MVP
    print(f"📡 Fetching 5 years of data for {ticker}...")
    df_pandas = yf.download(ticker, period="5y", auto_adjust=True, group_by='column')

    
    if df_pandas.empty:
        print("❌ No data found.")
        return

    # Fix for MultiIndex columns (The 'tuple' error fix)
    if isinstance(df_pandas.columns, pd.MultiIndex):
        df_pandas.columns = df_pandas.columns.get_level_values(0)

    # Now it's safe to clean them
    df_pandas.columns = [str(c).lower().replace(" ", "_") for c in df_pandas.columns]
    
    # Ensure 'date' is a column, not just the index
    df_pandas = df_pandas.reset_index()
    df_pandas.columns = [str(c).lower().replace(" ", "_") for c in df_pandas.columns]
    
    # Convert to Spark
    sdf = spark.createDataFrame(df_pandas)
    
    # Save to Raw
    output_path = f"/app/data/raw/{ticker}"
    sdf.write.mode("overwrite").parquet(output_path)
    print(f"✅ Raw data saved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    ingest_stock(sys.argv[1])