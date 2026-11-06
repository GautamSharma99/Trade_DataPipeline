"""
Tickstream web backend (FastAPI).

  GET  /api/history   1-minute candles, moving average and alerts for one coin (from Spark's serving DB)
  GET  /api/overview  volatility for every coin + Spark throughput history
  WS   /ws            pushes two kinds of message:
                        {"type": "trades", ...}  raw trades, read straight from Kafka (~4x per second)
                        {"type": "stats",  ...}  latest Spark aggregates + pipeline health (every 2s)
  /                   the frontend (static files)

The backend is a second, independent Kafka consumer of the same topic Spark reads:
Kafka fans the stream out to both without either one slowing the other down.
"""
import asyncio
import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone

import sqlite3
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
DB_PATH = os.getenv("DB_PATH", "/data/db/analytics.db")
LAKE_PATH = os.getenv("LAKE_PATH", "/data/lake/trades")
SOURCE = os.getenv("SOURCE", "coinbase")
ALERT_PCT = float(os.getenv("ALERT_PCT", "0.5"))
SYMBOLS = [s for s in os.getenv("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD").split(",") if s]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [web] %(message)s")
log = logging.getLogger()

clients: set[WebSocket] = set()
kafka_state = {"rate": 0.0, "partitions": [], "connected": False}
_lake_cache = {"at": 0.0, "files": 0, "bytes": 0}


# --------------------------------------------------------------------------- helpers
def db_rows(sql, params=()):
    if not os.path.exists(DB_PATH):
        return []
    try:
        with closing(sqlite3.connect(DB_PATH, timeout=5)) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute(sql, params)]
    except sqlite3.Error:
        return []


def epoch(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


def utc_str(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def candle_json(r):
    return {"time": epoch(r["window_start"]), "open": r["open"], "high": r["high"], "low": r["low"],
            "close": r["close"], "volume": r["volume"], "trades": r["trades"], "vwap": r["vwap"],
            "pct": r["pct_change"]}


def alert_json(r):
    return {"symbol": r["symbol"], "time": epoch(r["window_start"]), "direction": r["direction"],
            "pct": r["pct_change"], "open": r["open"], "close": r["close"], "trades": r["trades"]}


def lake_stats():
    now = time.time()
    if now - _lake_cache["at"] > 10:
        files = size = 0
        for root, _, names in os.walk(LAKE_PATH):
            for n in names:
                if n.endswith(".parquet"):
                    files += 1
                    size += os.path.getsize(os.path.join(root, n))
        _lake_cache.update(at=now, files=files, bytes=size)
    return {"files": _lake_cache["files"], "bytes": _lake_cache["bytes"]}


def finished_metrics_clause():
    """Sliding windows that end in the future cover only part of their 5 minutes; skip them."""
    return utc_str(time.time() + 60)


# --------------------------------------------------------------------------- Kafka -> WebSocket
def kafka_worker(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    while True:
        consumer = None
        try:
            consumer = Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": f"tickstream-web-{uuid.uuid4().hex[:8]}",   # own group: always read live
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            })
            consumer.subscribe([TOPIC])
            kafka_state["connected"] = True
            count, window_start, last_meta = 0, time.time(), 0.0
            keys_by_partition: dict[int, set] = {}
            while True:
                batch = []
                for m in consumer.consume(num_messages=500, timeout=0.25):
                    if m.error():
                        continue
                    try:
                        trade = json.loads(m.value())
                    except ValueError:
                        continue
                    trade["partition"], trade["offset"] = m.partition(), m.offset()
                    keys_by_partition.setdefault(m.partition(), set()).add(trade.get("symbol"))
                    batch.append(trade)
                if batch:
                    loop.call_soon_threadsafe(queue.put_nowait, batch)
                count += len(batch)
                now = time.time()
                if now - window_start >= 2:
                    kafka_state["rate"] = count / (now - window_start)
                    count, window_start = 0, now
                if now - last_meta >= 2:
                    last_meta = now
                    meta = consumer.list_topics(TOPIC, timeout=5).topics[TOPIC]
                    parts = []
                    for p in sorted(meta.partitions):
                        _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=5, cached=False)
                        parts.append({"partition": p, "messages": high,
                                      "keys": sorted(k for k in keys_by_partition.get(p, set()) if k)})
                    kafka_state["partitions"] = parts
        except Exception as exc:
            kafka_state["connected"] = False
            log.warning("kafka consumer error: %s (retrying in 3s)", exc)
            time.sleep(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


async def broadcast(message: dict):
    if not clients:
        return
    data = json.dumps(message, separators=(",", ":"))

    async def send(ws):
        try:
            await asyncio.wait_for(ws.send_text(data), timeout=2)
        except Exception:
            clients.discard(ws)

    await asyncio.gather(*(send(ws) for ws in list(clients)))


async def trade_pump(queue: asyncio.Queue):
    while True:
        batch = await queue.get()
        await asyncio.sleep(0.2)                 # coalesce into ~5 pushes per second
        while not queue.empty():
            batch.extend(queue.get_nowait())
        await broadcast({"type": "trades", "data": batch[-300:], "count": len(batch)})


# --------------------------------------------------------------------------- Spark aggregates
def build_stats():
    horizon = finished_metrics_clause()
    coins, tails = {}, {}
    known = [r["symbol"] for r in db_rows("SELECT DISTINCT symbol FROM candles")]
    for s in sorted(known, key=lambda x: (x not in SYMBOLS, SYMBOLS.index(x) if x in SYMBOLS else x)):
        cs = db_rows("SELECT * FROM candles WHERE symbol=? ORDER BY window_start DESC LIMIT 3", (s,))
        ms = db_rows("SELECT * FROM metrics WHERE symbol=? AND window_end<=? ORDER BY window_end DESC LIMIT 3",
                     (s, horizon))
        if not cs:
            continue
        coins[s] = {
            "price": cs[0]["close"], "pct_1m": cs[0]["pct_change"], "trades_1m": cs[0]["trades"],
            "sma_5m": ms[0]["sma_5m"] if ms else None,
            "volatility_5m": ms[0]["volatility_pct"] if ms else None,
        }
        tails[s] = {
            "candles": [candle_json(r) for r in reversed(cs)],
            "sma": [{"time": epoch(r["window_end"]) - 60, "value": r["sma_5m"]} for r in reversed(ms)],
            "volatility": [{"time": epoch(r["window_end"]) - 60, "value": r["volatility_pct"]} for r in reversed(ms)],
        }
    queries = {}
    for name in ("lake", "candles", "metrics"):
        r = db_rows("SELECT * FROM throughput WHERE query=? ORDER BY batch_id DESC LIMIT 1", (name,))
        if r:
            queries[name] = {"batch_id": r[0]["batch_id"], "ts": r[0]["ts"], "input_rows": r[0]["input_rows"],
                             "input_rps": r[0]["input_rows_per_sec"],
                             "processed_rps": r[0]["processed_rows_per_sec"],
                             "batch_ms": r[0].get("batch_ms")}
    totals = db_rows("SELECT COUNT(*) AS candles, COALESCE(SUM(trades),0) AS trades FROM candles")
    alert_count = db_rows("SELECT COUNT(*) AS n FROM alerts")
    alerts = db_rows("SELECT * FROM alerts ORDER BY window_start DESC LIMIT 20")
    return {
        "type": "stats",
        "server_time": time.time(),
        "coins": coins,
        "tails": tails,
        "alerts": [alert_json(a) for a in alerts],
        "pipeline": {
            "source": SOURCE,
            "alert_pct": ALERT_PCT,
            "kafka": {"topic": TOPIC, "connected": kafka_state["connected"],
                      "rate": kafka_state["rate"], "partitions": kafka_state["partitions"]},
            "spark": {"queries": queries, "trades_aggregated": totals[0]["trades"] if totals else 0},
            "db": {"candles": totals[0]["candles"] if totals else 0,
                   "alerts": alert_count[0]["n"] if alert_count else 0},
            "lake": lake_stats(),
        },
    }


async def stats_pump():
    while True:
        try:
            if clients:
                await broadcast(await asyncio.to_thread(build_stats))
        except Exception as exc:
            log.warning("stats error: %s", exc)
        await asyncio.sleep(2)


# --------------------------------------------------------------------------- app
@asynccontextmanager
async def lifespan(_app):
    queue: asyncio.Queue = asyncio.Queue()
    threading.Thread(target=kafka_worker, args=(asyncio.get_running_loop(), queue), daemon=True).start()
    tasks = [asyncio.create_task(trade_pump(queue)), asyncio.create_task(stats_pump())]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="Tickstream", lifespan=lifespan)


@app.get("/api/history")
def history(symbol: str, minutes: int = Query(120, ge=5, le=1440)):
    since = utc_str(time.time() - minutes * 60)
    candles = db_rows("SELECT * FROM candles WHERE symbol=? AND window_start>=? ORDER BY window_start",
                      (symbol, since))
    metrics = db_rows("SELECT window_end, sma_5m FROM metrics WHERE symbol=? AND window_end>=? AND window_end<=? "
                      "ORDER BY window_end", (symbol, since, finished_metrics_clause()))
    alerts = db_rows("SELECT * FROM alerts WHERE symbol=? AND window_start>=? ORDER BY window_start",
                     (symbol, since))
    return {
        "symbol": symbol,
        "candles": [candle_json(r) for r in candles],
        "sma": [{"time": epoch(r["window_end"]) - 60, "value": r["sma_5m"]} for r in metrics],
        "alerts": [alert_json(a) for a in alerts],
    }


@app.get("/api/overview")
def overview(minutes: int = Query(60, ge=5, le=1440)):
    since = utc_str(time.time() - minutes * 60)
    vol: dict[str, list] = {}
    for r in db_rows("SELECT symbol, window_end, volatility_pct FROM metrics WHERE window_end>=? AND window_end<=? "
                     "ORDER BY window_end", (since, finished_metrics_clause())):
        vol.setdefault(r["symbol"], []).append({"time": epoch(r["window_end"]) - 60, "value": r["volatility_pct"]})
    tp = db_rows("SELECT ts, input_rows_per_sec, batch_ms FROM throughput WHERE query='candles' AND ts>=? "
                 "ORDER BY ts", (since,))
    return {
        "symbols": SYMBOLS,
        "volatility": vol,
        "throughput": [{"time": epoch(r["ts"]), "value": r["input_rows_per_sec"]} for r in tp],
        "batch_ms": [{"time": epoch(r["ts"]), "value": r["batch_ms"]} for r in tp if r.get("batch_ms")],
    }


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        await ws.send_text(json.dumps(await asyncio.to_thread(build_stats)))
        while True:
            await ws.receive_text()      # we only push; this just detects disconnects
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)


app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
