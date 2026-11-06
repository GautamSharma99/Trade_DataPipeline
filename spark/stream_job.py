"""
Spark Structured Streaming job.

Reads raw trades from Kafka and runs three streaming queries at once:

  1. lake     raw trades -> Parquet data lake, partitioned by symbol/date   (volume / batch layer)
  2. candles  1-minute TUMBLING windows -> OHLC, VWAP, volume, trade count
              + spike alerts when a 1-minute move exceeds ALERT_PCT        (speed layer)
  3. metrics  5-minute SLIDING windows (1-minute slide) -> moving average
              (SMA-5m) and volatility (std-dev as % of mean)

Aggregates are upserted into SQLite (the serving layer the dashboard reads).
Late data is handled with a 30-second event-time watermark.
"""
import os
import sqlite3
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
DB_PATH = os.getenv("DB_PATH", "/data/db/analytics.db")
LAKE_PATH = os.getenv("LAKE_PATH", "/data/lake/trades")
CHECKPOINTS = os.getenv("CHECKPOINTS", "/data/checkpoints")
ALERT_PCT = float(os.getenv("ALERT_PCT", "0.5"))
MIN_TRADES_FOR_ALERT = 5
WATERMARK = "30 seconds"
KAFKA_PKG = os.getenv("KAFKA_PKG", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")

TRADE_SCHEMA = T.StructType([
    T.StructField("symbol", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("size", T.DoubleType()),
    T.StructField("side", T.StringType()),
    T.StructField("trade_id", T.LongType()),
    T.StructField("ts_ms", T.LongType()),
    T.StructField("source", T.StringType()),
])


# --------------------------------------------------------------------------- serving layer (SQLite)
def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")   # dashboard can read while Spark writes
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS candles (
            symbol TEXT, window_start TEXT, window_end TEXT,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, vwap REAL, trades INTEGER, pct_change REAL, updated_at TEXT,
            PRIMARY KEY (symbol, window_start));
        CREATE TABLE IF NOT EXISTS metrics (
            symbol TEXT, window_start TEXT, window_end TEXT,
            sma_5m REAL, volatility_pct REAL, trades_5m INTEGER, volume_5m REAL, updated_at TEXT,
            PRIMARY KEY (symbol, window_start));
        CREATE TABLE IF NOT EXISTS alerts (
            symbol TEXT, window_start TEXT, direction TEXT, pct_change REAL,
            open REAL, close REAL, trades INTEGER, detected_at TEXT,
            PRIMARY KEY (symbol, window_start));
        CREATE TABLE IF NOT EXISTS throughput (
            query TEXT, batch_id INTEGER, ts TEXT, input_rows INTEGER,
            input_rows_per_sec REAL, processed_rows_per_sec REAL, batch_ms REAL,
            PRIMARY KEY (query, batch_id));
        """)
        try:  # databases created by an older version of this job lack batch_ms
            conn.execute("ALTER TABLE throughput ADD COLUMN batch_ms REAL")
        except sqlite3.OperationalError:
            pass


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def fmt(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def upsert_candles(batch_df, batch_id):
    """foreachBatch sink: each micro-batch holds only the windows that changed (update mode)."""
    rows = batch_df.collect()
    if not rows:
        return
    stamp = now_iso()
    candles, alerts = [], []
    for r in rows:
        pct = (r.close - r.open) / r.open * 100 if r.open else 0.0
        candles.append((r.symbol, fmt(r.window_start), fmt(r.window_end), r.open, r.high, r.low,
                        r.close, r.volume, r.vwap, r.trades, pct, stamp))
        if abs(pct) >= ALERT_PCT and r.trades >= MIN_TRADES_FOR_ALERT:
            alerts.append((r.symbol, fmt(r.window_start), "UP" if pct > 0 else "DOWN", pct,
                           r.open, r.close, r.trades, stamp))
    with db() as conn:
        conn.executemany("""
            INSERT INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, window_start) DO UPDATE SET
              window_end=excluded.window_end, open=excluded.open, high=excluded.high,
              low=excluded.low, close=excluded.close, volume=excluded.volume, vwap=excluded.vwap,
              trades=excluded.trades, pct_change=excluded.pct_change, updated_at=excluded.updated_at
        """, candles)
        conn.executemany("""
            INSERT INTO alerts VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, window_start) DO UPDATE SET
              direction=excluded.direction, pct_change=excluded.pct_change,
              close=excluded.close, trades=excluded.trades
        """, alerts)
    for a in alerts:
        print(f"[ALERT] {a[0]} {a[2]} {a[3]:+.2f}% in minute {a[1]}", flush=True)


def upsert_metrics(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    stamp = now_iso()
    data = [(r.symbol, fmt(r.window_start), fmt(r.window_end), r.sma_5m, r.volatility_pct,
             r.trades_5m, r.volume_5m, stamp) for r in rows]
    with db() as conn:
        conn.executemany("""
            INSERT INTO metrics VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, window_start) DO UPDATE SET
              sma_5m=excluded.sma_5m, volatility_pct=excluded.volatility_pct,
              trades_5m=excluded.trades_5m, volume_5m=excluded.volume_5m,
              updated_at=excluded.updated_at
        """, data)


def record_progress(queries):
    """Store each query's latest micro-batch stats so the dashboard can show ingest throughput."""
    rows = []
    for q in queries:
        p = q.lastProgress
        if not p:
            continue
        rows.append((q.name, p["batchId"], p["timestamp"][:19].replace("T", " "), p["numInputRows"],
                     float(p.get("inputRowsPerSecond") or 0.0),
                     float(p.get("processedRowsPerSecond") or 0.0),
                     float(p.get("durationMs", {}).get("triggerExecution") or 0.0)))
    if rows:
        with db() as conn:
            conn.executemany("INSERT OR IGNORE INTO throughput (query, batch_id, ts, input_rows, "
                             "input_rows_per_sec, processed_rows_per_sec, batch_ms) "
                             "VALUES (?,?,?,?,?,?,?)", rows)


# --------------------------------------------------------------------------- streaming queries
def main():
    init_db()
    spark = (SparkSession.builder
             .appName("CryptoStreamAnalytics")
             .master("local[*]")
             .config("spark.jars.packages", KAFKA_PKG)
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.streaming.metricsEnabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    trades = (raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"),
                         F.col("timestamp").alias("kafka_ts"))
              .select("t.*", "kafka_ts")
              .where(F.col("price").isNotNull() & (F.col("price") > 0))
              .withColumn("event_time", F.expr("timestamp_millis(ts_ms)")))

    # 1) Raw trades -> Parquet data lake
    lake_q = (trades.withColumn("date", F.to_date("event_time"))
              .writeStream.queryName("lake")
              .format("parquet")
              .option("path", LAKE_PATH)
              .option("checkpointLocation", f"{CHECKPOINTS}/lake")
              .partitionBy("symbol", "date")
              .trigger(processingTime="30 seconds")
              .start())

    # 2) 1-minute tumbling-window OHLC candles + spike alerts
    candles = (trades.withWatermark("event_time", WATERMARK)
               .groupBy(F.window("event_time", "1 minute"), "symbol")
               .agg(F.min_by("price", "event_time").alias("open"),
                    F.max("price").alias("high"),
                    F.min("price").alias("low"),
                    F.max_by("price", "event_time").alias("close"),
                    F.sum("size").alias("volume"),
                    (F.sum(F.col("price") * F.col("size")) / F.sum("size")).alias("vwap"),
                    F.count("*").alias("trades"))
               .select(F.col("window.start").alias("window_start"),
                       F.col("window.end").alias("window_end"),
                       "symbol", "open", "high", "low", "close", "volume", "vwap", "trades"))
    candles_q = (candles.writeStream.queryName("candles")
                 .outputMode("update")
                 .foreachBatch(upsert_candles)
                 .option("checkpointLocation", f"{CHECKPOINTS}/candles")
                 .trigger(processingTime="5 seconds")
                 .start())

    # 3) 5-minute sliding windows -> moving average + volatility
    metrics = (trades.withWatermark("event_time", WATERMARK)
               .groupBy(F.window("event_time", "5 minutes", "1 minute"), "symbol")
               .agg(F.avg("price").alias("sma_5m"),
                    F.coalesce(F.stddev_samp("price"), F.lit(0.0)).alias("std_5m"),
                    F.count("*").alias("trades_5m"),
                    F.sum("size").alias("volume_5m"))
               .select(F.col("window.start").alias("window_start"),
                       F.col("window.end").alias("window_end"),
                       "symbol", "sma_5m",
                       (F.col("std_5m") / F.col("sma_5m") * 100).alias("volatility_pct"),
                       "trades_5m", "volume_5m"))
    metrics_q = (metrics.writeStream.queryName("metrics")
                 .outputMode("update")
                 .foreachBatch(upsert_metrics)
                 .option("checkpointLocation", f"{CHECKPOINTS}/metrics")
                 .trigger(processingTime="5 seconds")
                 .start())

    print(f"[spark] streaming from kafka://{BOOTSTRAP}/{TOPIC} | alert threshold {ALERT_PCT}% "
          f"| Spark UI http://localhost:4040", flush=True)

    queries = [lake_q, candles_q, metrics_q]
    # awaitAnyTermination re-raises if any query fails; otherwise log progress every 5s
    while not spark.streams.awaitAnyTermination(5):
        record_progress(queries)


if __name__ == "__main__":
    main()
