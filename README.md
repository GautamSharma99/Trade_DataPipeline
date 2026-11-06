# Real-Time Crypto Price Analytics Pipeline

A Big Data Analytics project that ingests **live cryptocurrency trades**, processes them
with **Apache Spark Structured Streaming**, and serves real-time analytics on a dashboard.

```
 Coinbase WebSocket ──► Producer ──► Kafka topic ──┬─► Spark Structured Streaming ──┬─► Parquet data lake (raw trades)
 (live BTC/ETH/SOL)     (Python)     crypto-trades │   3 concurrent queries         └─► SQLite serving DB (candles, SMA, volatility, alerts)
                                     3 partitions  │                                         │
                                                   └─► FastAPI (2nd Kafka consumer) ◄────────┘
                                                            │ WebSocket push
                                                            ▼
                                                   Tickstream web dashboard
 Batch layer: Spark batch job over the Parquet lake (hourly OHLC, top moves, correlation)
```

| Layer | Technology | What it does |
|---|---|---|
| Ingestion | Python producer, `confluent-kafka` | Subscribes to Coinbase's public `matches` feed, publishes each trade as JSON keyed by symbol |
| Messaging | Apache Kafka 3.8 (KRaft, no ZooKeeper) | Durable, partitioned buffer; decouples producer from consumer |
| Stream processing | Spark 3.5 Structured Streaming | Event-time windows with watermarks, upserted via `foreachBatch` |
| Storage | Parquet (partitioned by `symbol/date`) + SQLite | Data lake for history; small serving DB for the dashboard |
| Batch analytics | Spark SQL / DataFrames | Historical aggregation over the lake |
| Serving API | FastAPI + WebSocket | REST for history; pushes raw trades (from Kafka) and Spark aggregates to the browser |
| Frontend | HTML/CSS/JS + TradingView Lightweight Charts | Live pipeline diagram, candlesticks, trade tape, volatility, throughput, alerts |

## Run it

Only **Docker Desktop** is needed. Java, Spark and Kafka all run inside containers.

```bash
docker compose up --build        # first build takes a few minutes
```

| Open | URL |
|---|---|
| Dashboard | http://localhost:8080 |
| Spark UI (see the **Structured Streaming** tab) | http://localhost:4040 |

Data starts showing up about 10 seconds after startup. Candles and moving averages fill in over the first 5 minutes.

```bash
docker compose logs -f producer spark            # watch trades and [ALERT] lines
docker compose exec spark python batch_analysis.py   # batch layer (run after ~10+ min of data)
docker compose down                              # stop (data in ./data is kept)
docker compose down -v && rm -rf data            # full reset
```

### Options

| Variable | Default | Meaning |
|---|---|---|
| `SOURCE` | `coinbase` | `simulate` = synthetic trades with injected spikes (works offline) |
| `ALERT_PCT` | `0.5` | 1-minute price move (%) that raises a spike alert |
| `SYMBOLS` | `BTC-USD,ETH-USD,SOL-USD` | Any Coinbase product IDs |

Example: `SOURCE=simulate ALERT_PCT=0.8 docker compose up --build`

> **Demo tip:** real markets rarely move 0.5% in one minute, so the live feed may produce no alerts
> for hours. For the presentation, either lower the threshold (`ALERT_PCT=0.05`) or run
> `SOURCE=simulate`, which injects a 0.8–2% spike about every 2 minutes. If Coinbase can't be
> reached, the producer switches to the simulator automatically after 5 failed attempts.

## The dashboard

- **The pipeline, right now**: a live diagram of the whole system. Every moving dot is a real event:
  a batch of trades reaching Kafka, a Spark micro-batch finishing, an update reaching the page.
  Each stage shows live numbers: trades/s, messages per Kafka partition (and which coins hash
  to which partition), Spark rows/s and micro-batch duration, lake size, and exchange-to-screen latency.
- **Coin cards**: live price straight from Kafka, 1-minute change and volatility from Spark, plus a sparkline.
- **Price chart**: Spark's 1-minute candles, the 5-minute moving average, volume, spike-alert markers and a
  dashed live-price line. Hover to read open/high/low/close for any minute.
- **Trades as they arrive**: the raw stream, read by the backend as a second Kafka consumer.
- **Volatility, Spark throughput, spike alerts**: the remaining speed-layer outputs.

## What Spark computes

The stream job (`spark/stream_job.py`) runs **three streaming queries on the same Kafka source**:

1. **`lake`**: writes raw trades to Parquet every 30 s, partitioned by `symbol` and `date`.
2. **`candles`**: groups trades into **1-minute tumbling windows** and computes Open/High/Low/Close
   (with `min_by`/`max_by` on event time), volume, VWAP and trade count. Any window where
   |close − open| / open ≥ `ALERT_PCT` becomes a **spike alert**.
3. **`metrics`**: uses **5-minute sliding windows that slide every 1 minute**, which gives a
   **5-minute simple moving average** and **volatility** (standard deviation as a % of the mean price).

All windows use **event time** (the exchange's trade timestamp, not arrival time) and a
**30-second watermark**, so slightly late trades still count while state stays bounded.
Results are written in **update mode** through `foreachBatch` as idempotent upserts. Together
with Kafka offsets in the checkpoint, a restarted job resumes where it stopped.

## Project structure

```
crypto-stream-analytics/
├── docker-compose.yml        # Kafka, topic init, producer, Spark, web
├── producer/producer.py      # Coinbase WebSocket / simulator → Kafka
├── spark/stream_job.py       # Structured Streaming job (speed layer)
├── spark/batch_analysis.py   # batch job over the Parquet lake (batch layer)
├── web/main.py               # FastAPI backend: REST + WebSocket, second Kafka consumer
├── web/static/               # frontend (index.html, styles.css, app.js)
└── data/                     # created at runtime: lake/ (Parquet), checkpoints/
```

## Big Data concepts covered (useful for the report and viva)

- **The 5 Vs**: *Velocity* (continuous stream, events/s shown live), *Volume* (lake grows without limit,
  partitioned Parquet), *Variety* (multiple assets and sources), *Veracity* (schema enforcement and
  null/negative price filtering, late-data handling), *Value* (alerts and indicators).
- **Lambda architecture**: a speed layer (streaming aggregates) plus a batch layer (Spark over the lake),
  both served to the user.
- **Kafka**: topics, partitions, message keys (same key → same partition → per-coin ordering; the dashboard
  shows that two coins can hash to the same partition), consumer groups and fan-out (Spark and the web
  backend read the same topic independently), offsets, decoupling.
- **Spark Structured Streaming**: micro-batches, tumbling vs sliding windows, event time vs processing time,
  watermarks, output modes, checkpointing and fault tolerance, `foreachBatch` sinks.
- **Columnar storage**: Parquet with partition pruning (`WHERE symbol='BTC-USD'` reads one folder).

## Suggested report outline

1. Introduction & problem statement: why real-time market analytics matters
2. Literature review: Lambda vs Kappa architecture, Kafka, Spark Streaming vs Flink
3. System architecture (diagram above) and dataset description (Coinbase trade schema)
4. Implementation: producer, Kafka setup, streaming queries, serving layer, dashboard
5. Results: dashboard screenshots, Spark UI throughput/latency, batch analysis output
6. Performance evaluation: events/s, micro-batch duration, effect of trigger interval & partitions
7. Limitations & future work (below)
8. Conclusion

## Possible extensions

- Scale out: run Spark on a standalone cluster (`spark-master` + workers) and compare throughput
- Add more exchanges (Binance, Kraken) and detect **arbitrage** gaps between them
- Replace the fixed-threshold alert with a **z-score / anomaly-detection** model (Spark MLlib)
- Price forecasting on the lake (ARIMA / LSTM) and compare with live data
- Store aggregates in Cassandra / PostgreSQL / InfluxDB instead of SQLite; add Grafana
