import sys
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

def process_gold_layer(ticker):
    spark = SparkSession.builder.appName(f"Analyze_{ticker}").getOrCreate()
    
    # 1. Load Raw Data
    raw_path = f"/app/data/raw/{ticker}"
    df = spark.read.parquet(raw_path)

    # 2. Force Lowercase Headers
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    # 3. Define Windows
    w20 = Window.orderBy("date").rowsBetween(-19, 0)
    w50 = Window.orderBy("date").rowsBetween(-49, 0)
    w200 = Window.orderBy("date").rowsBetween(-199, 0)
    w14 = Window.orderBy("date").rowsBetween(-13, 0)

    # 4. START CALCULATIONS (This defines df_metrics)
    df_metrics = df.withColumn("sma_20", F.avg("close").over(w20)) \
                   .withColumn("sma_50", F.avg("close").over(w50)) \
                   .withColumn("sma_200", F.avg("close").over(w200))

    # 5. RSI Logic with Divide-by-Zero Protection
    df_metrics = df_metrics.withColumn("prev_close", F.lag("close", 1).over(Window.orderBy("date"))) \
        .withColumn("diff", F.col("close") - F.col("prev_close")) \
        .withColumn("gain", F.when(F.col("diff") > 0, F.col("diff")).otherwise(0)) \
        .withColumn("loss", F.when(F.col("diff") < 0, F.abs(F.col("diff"))).otherwise(0)) \
        .withColumn("avg_gain", F.avg("gain").over(w14)) \
        .withColumn("avg_loss", F.avg("loss").over(w14))

    # Protective 'rs' calculation
    df_metrics = df_metrics.withColumn("rs", 
        F.when(F.col("avg_loss") == 0, None)
        .otherwise(F.col("avg_gain") / F.col("avg_loss"))
    )

    # Final RSI column
    df_metrics = df_metrics.withColumn("rsi_14", 
        F.when(F.col("avg_loss") == 0, 100.0)
        .when(F.col("rs").isNull(), None)
        .otherwise(100.0 - (100.0 / (1.0 + F.col("rs"))))
    )

    # 6. Risk Metrics
    df_metrics = df_metrics.withColumn("daily_return", (F.col("close") - F.col("prev_close")) / F.col("prev_close")) \
        .withColumn("volatility_30d", F.stddev("daily_return").over(Window.orderBy("date").rowsBetween(-29, 0)) * F.sqrt(F.lit(252)))

    # 7. Save to Gold
    gold_path = f"/app/data/processed/{ticker}_analytics.parquet"
    
    # Select only the columns needed for the Dashboard
    final_cols = ["date", "open", "high", "low", "close", "volume", 
                  "sma_20", "sma_50", "sma_200", "rsi_14", "daily_return", "volatility_30d"]
    
    df_metrics.select(*final_cols).coalesce(1).write.mode("overwrite").parquet(gold_path)
    
    print(f"✅ Gold metrics successfully saved for {ticker}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python calculate_metrics.py <TICKER>")
    else:
        process_gold_layer(sys.argv[1])