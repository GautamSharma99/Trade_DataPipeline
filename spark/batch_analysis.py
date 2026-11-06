"""
Batch layer: historical analysis over the Parquet data lake written by the stream job.

Run it while the pipeline is up (after it has collected some data):
    docker compose exec spark python batch_analysis.py

It prints and saves (as CSV under data/lake/reports/):
  1. summary per coin        - trades, volume, price range, VWAP
  2. hourly OHLC per coin    - classic batch aggregation
  3. biggest 1-minute moves  - top price swings found in the full history
  4. return correlation      - how closely the coins move together (1-minute log returns)
"""
import os

from pyspark.sql import SparkSession, Window, functions as F

LAKE_PATH = os.getenv("LAKE_PATH", "/data/lake/trades")
OUT = os.getenv("REPORT_PATH", "/data/lake/reports")

spark = (SparkSession.builder.appName("CryptoBatchAnalysis").master("local[*]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.port", "4041")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

trades = spark.read.parquet(LAKE_PATH)
total = trades.count()
print(f"\nLoaded {total:,} trades from {LAKE_PATH}")
if total == 0:
    raise SystemExit("No data yet - let the stream run for a few minutes first.")


def save(df, name):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")


# 1) Summary per coin
summary = (trades.groupBy("symbol")
           .agg(F.count("*").alias("trades"),
                F.round(F.sum("size"), 4).alias("volume"),
                F.min("price").alias("min_price"),
                F.max("price").alias("max_price"),
                F.round(F.sum(F.col("price") * F.col("size")) / F.sum("size"), 4).alias("vwap"),
                F.min("event_time").alias("first_trade"),
                F.max("event_time").alias("last_trade"))
           .orderBy("symbol"))
print("\n== Summary per coin ==")
summary.show(truncate=False)
save(summary, "summary")

# 2) Hourly OHLC
hourly = (trades.groupBy("symbol", F.date_trunc("hour", "event_time").alias("hour"))
          .agg(F.min_by("price", "event_time").alias("open"),
               F.max("price").alias("high"),
               F.min("price").alias("low"),
               F.max_by("price", "event_time").alias("close"),
               F.count("*").alias("trades"))
          .orderBy("symbol", "hour"))
print("== Hourly OHLC ==")
hourly.show(30, truncate=False)
save(hourly, "hourly_ohlc")

# 3) Biggest 1-minute moves
minute = (trades.groupBy("symbol", F.date_trunc("minute", "event_time").alias("minute"))
          .agg(F.min_by("price", "event_time").alias("open"),
               F.max_by("price", "event_time").alias("close"),
               F.count("*").alias("trades"))
          .withColumn("pct_change", F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 3)))
top_moves = minute.orderBy(F.abs("pct_change").desc()).limit(10)
print("== Top 10 one-minute moves ==")
top_moves.show(truncate=False)
save(top_moves, "top_moves")

# 4) Correlation of 1-minute log returns between coins
w = Window.partitionBy("symbol").orderBy("minute")
returns = (minute.withColumn("ret", F.log(F.col("close") / F.lag("close").over(w)))
           .where(F.col("ret").isNotNull()))
wide = returns.groupBy("minute").pivot("symbol").agg(F.first("ret")).na.drop()
symbols = [c for c in wide.columns if c != "minute"]
print(f"== Correlation of 1-minute returns ({wide.count()} aligned minutes) ==")
rows = []
for a in symbols:
    row = {"symbol": a}
    for b in symbols:
        row[b] = round(wide.stat.corr(a, b), 3) if wide.count() > 2 else None
    rows.append(row)
corr = spark.createDataFrame(rows).select("symbol", *symbols)
corr.show(truncate=False)
save(corr, "return_correlation")

print(f"Reports saved to {OUT} (data/lake/reports on your machine)")
spark.stop()
