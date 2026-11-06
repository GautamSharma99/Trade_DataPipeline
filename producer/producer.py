"""
Kafka producer: streams live crypto trades into the `crypto-trades` topic.

SOURCE=coinbase  -> real trades from Coinbase's public WebSocket feed (no API key needed)
SOURCE=simulate  -> synthetic trades (geometric Brownian motion + injected price spikes),
                    useful offline or to guarantee alerts fire during a demo.

If the Coinbase feed fails repeatedly, the producer falls back to the simulator
so the pipeline never sits idle.

Every message is keyed by symbol, so all trades for one coin land in the same
Kafka partition and keep their order.
"""
import json
import logging
import math
import os
import random
import time
from datetime import datetime

import websocket
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto-trades")
SOURCE = os.getenv("SOURCE", "coinbase").lower()
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD").split(",") if s.strip()]
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
MAX_WS_FAILURES = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger()

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": 50,             # small batching window -> higher throughput
    "compression.type": "lz4",
    "acks": "1",
})

_sent = 0
_last_report = time.time()


def publish(trade: dict) -> None:
    global _sent, _last_report
    producer.produce(TOPIC, key=trade["symbol"], value=json.dumps(trade))
    producer.poll(0)
    _sent += 1
    now = time.time()
    if now - _last_report >= 10:
        log.info("sent %d trades in last %.0fs (%.1f/s) | last %s @ %.2f",
                 _sent, now - _last_report, _sent / (now - _last_report),
                 trade["symbol"], trade["price"])
        _sent, _last_report = 0, now


# --------------------------------------------------------------------------- Coinbase
def coinbase_to_trade(msg: dict) -> dict:
    ts = datetime.fromisoformat(msg["time"].replace("Z", "+00:00"))
    return {
        "symbol": msg["product_id"],
        "price": float(msg["price"]),
        "size": float(msg["size"]),
        "side": msg["side"],
        "trade_id": int(msg["trade_id"]),
        "ts_ms": int(ts.timestamp() * 1000),
        "source": "coinbase",
    }


def run_coinbase() -> None:
    failures = 0
    while True:
        try:
            log.info("connecting to %s for %s", COINBASE_WS, SYMBOLS)
            ws = websocket.create_connection(COINBASE_WS, timeout=30)
            ws.send(json.dumps({"type": "subscribe", "product_ids": SYMBOLS, "channels": ["matches"]}))
            while True:
                raw = ws.recv()
                if not raw:
                    raise ConnectionError("empty frame / socket closed")
                msg = json.loads(raw)
                if msg.get("type") == "error":
                    raise ConnectionError(msg.get("message", "coinbase error"))
                if msg.get("type") in ("match", "last_match"):
                    publish(coinbase_to_trade(msg))
                    failures = 0
        except Exception as exc:  # network errors, timeouts, closed sockets
            failures += 1
            log.warning("coinbase feed error (%d/%d): %s", failures, MAX_WS_FAILURES, exc)
            if failures >= MAX_WS_FAILURES:
                log.warning("giving up on coinbase, switching to simulator")
                run_simulator()
                return
            time.sleep(min(2 ** failures, 30))


# --------------------------------------------------------------------------- Simulator
START_PRICES = {"BTC-USD": 68000.0, "ETH-USD": 3400.0, "SOL-USD": 160.0}


def run_simulator(trades_per_sec: float = 20.0) -> None:
    log.info("running simulator for %s at ~%.0f trades/s", SYMBOLS, trades_per_sec)
    prices = {s: START_PRICES.get(s, 100.0) for s in SYMBOLS}
    trade_id = 0
    sigma_per_trade = 0.0004          # ~0.04% noise per trade
    spike_prob = 1 / 2500             # roughly one spike every ~2 minutes overall
    while True:
        symbol = random.choice(SYMBOLS)
        shock = random.gauss(0, sigma_per_trade)
        if random.random() < spike_prob:
            shock += random.choice([-1, 1]) * random.uniform(0.008, 0.02)  # 0.8%-2% jump
            log.info("injecting spike on %s (%.2f%%)", symbol, shock * 100)
        prices[symbol] *= math.exp(shock)
        trade_id += 1
        publish({
            "symbol": symbol,
            "price": round(prices[symbol], 4),
            "size": round(random.lognormvariate(-3, 1.2), 6),
            "side": random.choice(["buy", "sell"]),
            "trade_id": trade_id,
            "ts_ms": int(time.time() * 1000),
            "source": "simulator",
        })
        time.sleep(random.expovariate(trades_per_sec))


if __name__ == "__main__":
    log.info("source=%s topic=%s bootstrap=%s", SOURCE, TOPIC, BOOTSTRAP)
    try:
        run_simulator() if SOURCE == "simulate" else run_coinbase()
    finally:
        producer.flush(10)
