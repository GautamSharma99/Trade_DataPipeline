/* Tickstream frontend: live trades + Spark aggregates over one WebSocket, history over REST. */
(() => {
  "use strict";
  const LWC = window.LightweightCharts;
  const css = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
  const C = {
    bg: css("--bg"), panel: css("--panel"), panel2: css("--panel-2"), line: css("--line"), grid: css("--grid"),
    ink: css("--ink"), ink2: css("--ink-2"), muted: css("--muted"), signal: css("--signal"),
    up: css("--up"), down: css("--down"),
  };
  const COIN_COLORS = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"];
  const NAMES = { "BTC-USD": "Bitcoin", "ETH-USD": "Ether", "SOL-USD": "Solana", "DOGE-USD": "Dogecoin", "XRP-USD": "XRP" };
  const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const $ = (id) => document.getElementById(id);

  const state = {
    symbols: [], symbol: null, minutes: 60,
    last: {}, spark: {}, coins: {},
    candles: new Map(), sma: new Map(), alerts: new Map(),
    lastBatch: {}, seen: 0, alertPct: 0.5,
  };

  // ------------------------------------------------------------------ formatting
  const nf = (d) => new Intl.NumberFormat("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  const fmtPrice = (p) => p == null ? "–" : (p >= 1 ? nf(2) : nf(4)).format(p);
  const fmtInt = (n) => n == null ? "–" : nf(0).format(n);
  const fmtPct = (p, d = 2) => p == null ? "–" : `${p > 0 ? "+" : ""}${p.toFixed(d)}%`;
  const fmtBytes = (b) => b >= 1e9 ? `${(b / 1e9).toFixed(2)} GB` : b >= 1e6 ? `${(b / 1e6).toFixed(2)} MB` : `${(b / 1e3).toFixed(0)} KB`;
  const hhmm = (sec) => new Date(sec * 1000).toISOString().slice(11, 16);
  const hhmmssms = (ms) => new Date(ms).toISOString().slice(11, 23);
  const coinColor = (s) => COIN_COLORS[Math.max(0, state.symbols.indexOf(s)) % COIN_COLORS.length];
  const ticker = (s) => s.split("-")[0];

  // ------------------------------------------------------------------ clock
  setInterval(() => { $("clock").textContent = `${new Date().toISOString().slice(11, 19)} UTC`; }, 1000);

  // ------------------------------------------------------------------ charts
  const baseChart = (el, extra = {}) => LWC.createChart(el, {
    autoSize: true,
    layout: {
      background: { type: LWC.ColorType.Solid, color: C.panel }, textColor: C.muted,
      fontFamily: "IBM Plex Sans, system-ui, sans-serif", fontSize: 11, attributionLogo: true,
      panes: { separatorColor: C.line, separatorHoverColor: C.line, enableResize: false },
    },
    grid: { vertLines: { color: C.grid }, horzLines: { color: C.grid } },
    rightPriceScale: { borderColor: C.line },
    timeScale: { borderColor: C.line, timeVisible: true, secondsVisible: false, rightOffset: 3 },
    crosshair: {
      mode: LWC.CrosshairMode.Normal,
      vertLine: { color: C.muted, labelBackgroundColor: C.panel2, style: LWC.LineStyle.Dotted },
      horzLine: { color: C.muted, labelBackgroundColor: C.panel2, style: LWC.LineStyle.Dotted },
    },
    ...extra,
  });

  const price = baseChart($("price-chart"));
  const candleSeries = price.addSeries(LWC.CandlestickSeries, {
    upColor: C.up, downColor: C.down, wickUpColor: C.up, wickDownColor: C.down,
    borderVisible: false, priceLineVisible: false,
  }, 0);
  const smaSeries = price.addSeries(LWC.LineSeries, {
    color: C.ink2, lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerRadius: 4,
  }, 0);
  const volSeries = price.addSeries(LWC.HistogramSeries, {
    color: "#3a4462", priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false,
  }, 1);
  price.panes()[0].setStretchFactor(0.78);
  price.panes()[1].setStretchFactor(0.22);
  const markers = LWC.createSeriesMarkers(candleSeries, []);
  let livePriceLine = null;

  const volChart = baseChart($("vol-chart"));
  const volLines = {};
  const tpChart = baseChart($("tp-chart"));
  const tpSeries = tpChart.addSeries(LWC.AreaSeries, {
    lineColor: C.signal, lineWidth: 2, topColor: "rgba(242,181,68,0.22)", bottomColor: "rgba(242,181,68,0)",
    priceLineVisible: false, priceFormat: { type: "price", precision: 1, minMove: 0.1 },
  });

  function precisionFor(p) {
    return p >= 1 ? { type: "price", precision: 2, minMove: 0.01 } : { type: "price", precision: 4, minMove: 0.0001 };
  }

  // Readout above the price chart follows the crosshair, or shows the latest candle.
  function renderReadout(c) {
    if (!c) { $("readout").textContent = "Waiting for the first candle"; return; }
    const dir = c.close >= c.open ? "up" : "down";
    $("readout").innerHTML =
      `${hhmm(c.time)} UTC &nbsp; O <b>${fmtPrice(c.open)}</b> H <b>${fmtPrice(c.high)}</b> ` +
      `L <b>${fmtPrice(c.low)}</b> C <b>${fmtPrice(c.close)}</b> &nbsp; ` +
      `<span style="color:var(--${dir})">${fmtPct(c.pct)}</span> &nbsp; ${fmtInt(c.trades)} trades`;
  }
  price.subscribeCrosshairMove((param) => {
    const c = param.time != null ? state.candles.get(param.time) : null;
    renderReadout(c || latestCandle());
  });
  const latestCandle = () => { let m = null; for (const c of state.candles.values()) if (!m || c.time > m.time) m = c; return m; };

  const toBar = (c) => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close });
  const toVol = (c) => ({ time: c.time, value: c.volume, color: c.close >= c.open ? "#34507a" : "#6b3a45" });

  function setAllCandles() {
    const list = [...state.candles.values()].sort((a, b) => a.time - b.time);
    candleSeries.setData(list.map(toBar));
    volSeries.setData(list.map(toVol));
    $("chart-empty").hidden = list.length > 0;
    renderReadout(list[list.length - 1]);
  }
  function setAllSma() {
    smaSeries.setData([...state.sma.entries()].sort((a, b) => a[0] - b[0]).map(([time, value]) => ({ time, value })));
  }

  // Merge the newest candles from a stats push. update() only accepts the last bar or a new one,
  // so if Spark revised an older bar (late trades inside the watermark) we redraw everything.
  function applyTail(tail) {
    if (!tail) return;
    let lastT = -Infinity; for (const t of state.candles.keys()) lastT = Math.max(lastT, t);
    let full = false; const incremental = [];
    for (const c of tail.candles) {
      const prev = state.candles.get(c.time);
      state.candles.set(c.time, c);
      if (c.time >= lastT) incremental.push(c);
      else if (!prev || prev.close !== c.close || prev.high !== c.high || prev.low !== c.low) full = true;
    }
    if (full || state.candles.size === tail.candles.length) setAllCandles();
    else {
      for (const c of incremental) { candleSeries.update(toBar(c)); volSeries.update(toVol(c)); }
      $("chart-empty").hidden = state.candles.size > 0;
      renderReadout(latestCandle());
    }
    let lastS = -Infinity; for (const t of state.sma.keys()) lastS = Math.max(lastS, t);
    let fullS = false;
    for (const p of tail.sma) {
      state.sma.set(p.time, p.value);
      if (p.time >= lastS) smaSeries.update({ time: p.time, value: p.value }); else fullS = true;
    }
    if (fullS) setAllSma();
  }

  function renderMarkers() {
    const list = [...state.alerts.values()]
      .filter((a) => a.symbol === state.symbol && state.candles.has(a.time))
      .sort((a, b) => a.time - b.time)
      .map((a) => {
        const up = a.direction === "UP";
        return { time: a.time, position: up ? "aboveBar" : "belowBar", color: up ? C.up : C.down,
                 shape: up ? "arrowUp" : "arrowDown", text: fmtPct(a.pct) };
      });
    markers.setMarkers(list);
  }

  async function loadHistory() {
    const sym = state.symbol;
    const res = await fetch(`/api/history?symbol=${encodeURIComponent(sym)}&minutes=${state.minutes}`);
    if (!res.ok || sym !== state.symbol) return;
    const h = await res.json();
    state.candles = new Map(h.candles.map((c) => [c.time, c]));
    state.sma = new Map(h.sma.map((p) => [p.time, p.value]));
    for (const a of h.alerts) state.alerts.set(`${a.symbol}|${a.time}`, a);
    const ref = h.candles.length ? h.candles[h.candles.length - 1].close : state.last[sym];
    if (ref) { candleSeries.applyOptions({ priceFormat: precisionFor(ref) }); smaSeries.applyOptions({ priceFormat: precisionFor(ref) }); }
    setAllCandles(); setAllSma(); renderMarkers();
    price.timeScale().fitContent();
    if (livePriceLine) { candleSeries.removePriceLine(livePriceLine); livePriceLine = null; }
    $("chart-title").textContent = `${NAMES[sym] || ticker(sym)} price, ${sym}`;
  }

  async function loadOverview() {
    const res = await fetch(`/api/overview?minutes=${Math.max(state.minutes, 30)}`);
    if (!res.ok) return null;
    const o = await res.json();
    if (!state.symbols.length) state.symbols = o.symbols;   // colours follow the configured coin order
    for (const [sym, pts] of Object.entries(o.volatility)) {
      if (!volLines[sym]) {
        volLines[sym] = volChart.addSeries(LWC.LineSeries, {
          color: coinColor(sym), lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
          priceFormat: { type: "custom", formatter: (v) => `${Math.abs(v) < 0.0005 ? "0.000" : v.toFixed(3)}%`, minMove: 0.001 },
        });
      }
      volLines[sym].setData(pts);
    }
    renderVolLegend();
    tpSeries.setData(o.throughput);
    if (o.batch_ms.length) {
      const last = o.batch_ms[o.batch_ms.length - 1].value;
      const tp = o.throughput.length ? o.throughput[o.throughput.length - 1].value : 0;
      $("tp-readout").innerHTML = `Now <b>${tp.toFixed(1)}</b> events/s, last micro-batch took <b>${fmtInt(last)} ms</b>`;
    }
    return o;
  }

  function renderVolLegend(time) {
    const parts = [];
    for (const sym of state.symbols) {
      const s = volLines[sym]; if (!s) continue;
      const data = s.data(); if (!data.length) continue;
      let v = data[data.length - 1].value;
      if (time != null) { const p = data.find((d) => d.time === time); if (p) v = p.value; }
      parts.push(`<span><i class="sw dot" style="background:${coinColor(sym)}"></i>${ticker(sym)} <b>${v.toFixed(3)}%</b></span>`);
    }
    $("vol-legend").innerHTML = parts.join("");
  }
  volChart.subscribeCrosshairMove((p) => renderVolLegend(p.time));

  // ------------------------------------------------------------------ coin cards
  function buildCoins() {
    const wrap = $("coins");
    wrap.innerHTML = "";
    state.symbols.forEach((sym) => {
      const b = document.createElement("button");
      b.type = "button"; b.className = "coin"; b.setAttribute("role", "tab");
      b.dataset.symbol = sym; b.style.setProperty("--c", coinColor(sym));
      b.setAttribute("aria-selected", String(sym === state.symbol));
      b.innerHTML = `
        <span class="name"><b>${NAMES[sym] || ticker(sym)}</b><span>${sym}</span></span>
        <span class="price" data-k="price">–</span>
        <canvas width="240" height="88" aria-hidden="true"></canvas>
        <span class="chg" data-k="chg">waiting for trades</span>`;
      b.addEventListener("click", () => selectSymbol(sym));
      wrap.appendChild(b);
      state.spark[sym] = state.spark[sym] || [];
    });
  }
  const card = (sym) => document.querySelector(`.coin[data-symbol="${sym}"]`);

  function selectSymbol(sym) {
    if (sym === state.symbol) return;
    state.symbol = sym;
    document.querySelectorAll(".coin").forEach((el) => el.setAttribute("aria-selected", String(el.dataset.symbol === sym)));
    loadHistory();
  }

  function drawSpark(sym) {
    const el = card(sym); if (!el) return;
    const cv = el.querySelector("canvas"); const ctx = cv.getContext("2d");
    const pts = state.spark[sym]; ctx.clearRect(0, 0, cv.width, cv.height);
    if (pts.length < 2) return;
    let lo = Infinity, hi = -Infinity; for (const p of pts) { lo = Math.min(lo, p); hi = Math.max(hi, p); }
    const span = hi - lo || hi * 1e-6 || 1, pad = 6;
    ctx.beginPath();
    pts.forEach((p, i) => {
      const x = pad + (i / (pts.length - 1)) * (cv.width - pad * 2);
      const y = cv.height - pad - ((p - lo) / span) * (cv.height - pad * 2);
      i ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
    });
    ctx.strokeStyle = coinColor(sym); ctx.lineWidth = 3; ctx.lineJoin = "round"; ctx.stroke();
  }

  function renderCoinStats() {
    for (const [sym, s] of Object.entries(state.coins)) {
      const el = card(sym); if (!el) continue;
      const chg = el.querySelector('[data-k="chg"]');
      const dir = s.pct_1m > 0 ? "up" : s.pct_1m < 0 ? "down" : "";
      const arrow = dir === "up" ? "▲" : dir === "down" ? "▼" : "";
      chg.innerHTML = `<span class="${dir}">${arrow} ${fmtPct(s.pct_1m, 3)}</span> this minute, ` +
        `vol ${s.volatility_5m != null ? s.volatility_5m.toFixed(3) + "%" : "–"}`;
      if (state.last[sym] == null) el.querySelector('[data-k="price"]').textContent = fmtPrice(s.price);
    }
  }

  // ------------------------------------------------------------------ trade tape
  const tape = $("tape");
  function addToTape(trades) {
    const frag = document.createDocumentFragment();
    for (const t of trades.slice(-18).reverse()) {
      const li = document.createElement("li");
      if (!reducedMotion) li.className = "new";
      li.innerHTML = `<span class="t">${hhmmssms(t.ts_ms)}</span>` +
        `<span class="s"><i class="sw dot" style="background:${coinColor(t.symbol)}"></i>${ticker(t.symbol)}</span>` +
        `<span class="p ${t.dir > 0 ? "up" : t.dir < 0 ? "down" : ""}">${fmtPrice(t.price)}</span>` +
        `<span class="q">${t.size < 1 ? t.size.toFixed(4) : t.size.toFixed(2)}</span>`;
      frag.appendChild(li);
    }
    tape.prepend(frag);
    while (tape.children.length > 60) tape.lastChild.remove();
  }

  // ------------------------------------------------------------------ alerts
  function renderAlerts() {
    const list = [...state.alerts.values()].sort((a, b) => b.time - a.time).slice(0, 20);
    const el = $("alerts");
    if (!list.length) {
      el.innerHTML = `<li class="none">No spikes yet. An alert appears when a coin moves ${state.alertPct}% or more
        inside one minute. Real markets can stay calm for hours; to see one sooner, restart with
        <code>ALERT_PCT=0.05</code> or <code>SOURCE=simulate</code>.</li>`;
      return;
    }
    el.innerHTML = list.map((a) => {
      const up = a.direction === "UP";
      return `<li><span class="ic ${up ? "up" : "down"}" aria-hidden="true">${up ? "▲" : "▼"}</span>
        <span class="m"><b>${a.symbol}</b> ${up ? "rose" : "fell"} ${Math.abs(a.pct).toFixed(2)}% in one minute</span>
        <span class="d">${hhmm(a.time)} UTC, ${fmtPrice(a.open)} to ${fmtPrice(a.close)}, ${fmtInt(a.trades)} trades</span></li>`;
    }).join("");
  }

  // ------------------------------------------------------------------ pipeline wires + packets
  const pipe = $("pipe"), wires = $("wires"), SVGNS = "http://www.w3.org/2000/svg";
  const EDGES = [["feed", "kafka"], ["kafka", "spark"], ["spark", "lake"], ["spark", "db"], ["db", "web"], ["kafka", "web", "lane"]];
  let paths = {}, packets = [], raf = null;
  const nodeEl = (n) => pipe.querySelector(`[data-node="${n}"]`);

  function layoutWires() {
    packets.forEach((p) => p.el.remove()); packets = [];
    paths = {};
    if (getComputedStyle(wires).display === "none") return;
    const box = pipe.getBoundingClientRect();
    wires.setAttribute("viewBox", `0 0 ${box.width} ${box.height}`);
    let html = "";
    for (const [a, b, kind] of EDGES) {
      const ra = nodeEl(a).getBoundingClientRect(), rb = nodeEl(b).getBoundingClientRect();
      let d;
      if (kind === "lane") {
        const x1 = ra.left + ra.width / 2 - box.left, y1 = ra.bottom - box.top;
        const x2 = rb.left + rb.width / 2 - box.left, y2 = rb.bottom - box.top;
        const y = box.height - 14, r = 12;
        d = `M${x1},${y1} L${x1},${y - r} Q${x1},${y} ${x1 + r},${y} L${x2 - r},${y} Q${x2},${y} ${x2},${y - r} L${x2},${y2}`;
        html += `<text class="wire-label" x="${x1 + 22}" y="${y - 7}">Second Kafka consumer: raw trades go straight to this page</text>`;
      } else {
        const x1 = ra.right - box.left, y1 = ra.top + ra.height / 2 - box.top;
        const x2 = rb.left - box.left, y2 = rb.top + rb.height / 2 - box.top, mx = (x1 + x2) / 2;
        d = `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
      }
      html += `<path class="wire" id="w-${a}-${b}" d="${d}"/>`;
    }
    wires.innerHTML = html;
    for (const [a, b] of EDGES) paths[`${a}-${b}`] = wires.querySelector(`#w-${a}-${b}`);
  }

  function hit(n) {
    const el = nodeEl(n); el.classList.add("hit");
    clearTimeout(el._t); el._t = setTimeout(() => el.classList.remove("hit"), 140);
  }

  function spawn(edge, onDone) {
    const path = paths[edge];
    if (reducedMotion || !path || packets.length > 40 || document.hidden) { onDone && onDone(); return; }
    // Drop packets stuck because the tab was throttled (no animation frames while in background)
    const now = performance.now();
    packets = packets.filter((p) => (now - p.t0 < p.dur + 1500) || (p.el.remove(), false));
    const len = path.getTotalLength();
    const start = path.getPointAtLength(0);
    const el = document.createElementNS(SVGNS, "circle");
    el.setAttribute("r", "4.5"); el.setAttribute("class", "packet");
    el.setAttribute("cx", start.x); el.setAttribute("cy", start.y);
    wires.appendChild(el);
    packets.push({ el, path, len, t0: performance.now(), dur: Math.min(1500, 380 + len * 1.4), to: edge.split("-")[1], onDone });
    if (!raf) raf = requestAnimationFrame(step);
  }

  function step(now) {
    packets = packets.filter((p) => {
      const k = (now - p.t0) / p.dur;
      if (k >= 1) { p.el.remove(); hit(p.to); p.onDone && p.onDone(); return false; }
      const e = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
      const pt = p.path.getPointAtLength(e * p.len);
      p.el.setAttribute("cx", pt.x); p.el.setAttribute("cy", pt.y);
      return true;
    });
    raf = packets.length ? requestAnimationFrame(step) : null;
  }
  let resizeT; window.addEventListener("resize", () => { clearTimeout(resizeT); resizeT = setTimeout(layoutWires, 150); });

  // ------------------------------------------------------------------ pipeline numbers
  function renderPipeline(p) {
    state.alertPct = p.alert_pct;
    $("alert-rule").textContent = `Any 1-minute move of ${p.alert_pct}% or more`;
    const sim = p.source === "simulate";
    $("source").textContent = sim ? "Simulated feed" : "Coinbase live feed";
    $("feed-what").textContent = sim ? "Simulator, with injected spikes" : "Coinbase WebSocket";
    $("feed-rate").textContent = p.kafka.rate.toFixed(1);

    const parts = p.kafka.partitions || [];
    const total = parts.reduce((s, x) => s + x.messages, 0);
    const max = Math.max(1, ...parts.map((x) => x.messages));
    $("kafka-total").textContent = fmtInt(total);
    $("partitions").innerHTML = parts.map((x) => `
      <li><span>P${x.partition}</span><span class="bar"><span style="width:${(x.messages / max) * 100}%"></span></span>
      <span class="n">${fmtInt(x.messages)}</span>
      <span class="keys">${x.keys.length ? x.keys.join(", ") : x.messages ? "older messages only" : "empty, no coin hashes here"}</span></li>`).join("");

    const q = p.spark.queries || {};
    $("spark-rate").textContent = q.candles ? q.candles.processed_rps.toFixed(0) : "–";
    const label = { candles: "1-min candles", metrics: "5-min sliding", lake: "lake writer" };
    $("queries").innerHTML = ["candles", "metrics", "lake"].filter((k) => q[k]).map((k) =>
      `<dt>${label[k]}</dt><dd>batch ${fmtInt(q[k].batch_id)}</dd><dd class="ms">${q[k].batch_ms ? fmtInt(q[k].batch_ms) + " ms" : ""}</dd>`).join("");

    $("lake-size").textContent = fmtBytes(p.lake.bytes);
    $("lake-files").textContent = `${fmtInt(p.lake.files)} Parquet files`;
    $("db-candles").textContent = fmtInt(p.db.candles);
    $("db-alerts").textContent = `candles computed, ${fmtInt(p.db.alerts)} alerts`;

    // Packets for Spark micro-batches that finished since the last push
    for (const [k, v] of Object.entries(q)) {
      if (state.lastBatch[k] != null && v.batch_id !== state.lastBatch[k]) spawn(k === "lake" ? "spark-lake" : "spark-db");
      state.lastBatch[k] = v.batch_id;
    }
    spawn("db-web");
  }

  // ------------------------------------------------------------------ socket handlers
  function onTrades(msg) {
    const trades = msg.data; if (!trades.length) return;
    const touched = new Set();
    for (const t of trades) {
      const prev = state.last[t.symbol];
      t.dir = prev == null ? 0 : Math.sign(t.price - prev);
      state.last[t.symbol] = t.price;
      const sp = (state.spark[t.symbol] = state.spark[t.symbol] || []);
      sp.push(t.price); if (sp.length > 150) sp.shift();
      touched.add(t.symbol);
    }
    for (const sym of touched) {
      const el = card(sym); if (!el) continue;
      const pe = el.querySelector('[data-k="price"]');
      const lastT = [...trades].reverse().find((t) => t.symbol === sym);
      pe.textContent = fmtPrice(lastT.price);
      if (!reducedMotion && lastT.dir) {
        pe.classList.remove("tick-up", "tick-down");
        void pe.offsetWidth;
        pe.classList.add(lastT.dir > 0 ? "tick-up" : "tick-down");
        clearTimeout(pe._t); pe._t = setTimeout(() => pe.classList.remove("tick-up", "tick-down"), 60);
      }
      drawSpark(sym);
    }
    const lp = state.last[state.symbol];
    if (lp != null && state.candles.size) {
      if (!livePriceLine) livePriceLine = candleSeries.createPriceLine({
        price: lp, color: C.signal, lineWidth: 1, lineStyle: LWC.LineStyle.Dashed, axisLabelVisible: true, title: "live",
      });
      else livePriceLine.applyOptions({ price: lp });
    }
    addToTape(trades);
    state.seen += trades.length;
    const newest = trades.reduce((m, t) => Math.max(m, t.ts_ms), 0);
    $("web-latency").textContent = `${fmtInt(Math.max(0, Date.now() - newest))} ms`;
    $("web-seen").textContent = `${fmtInt(state.seen)} trades received`;
    spawn("feed-kafka", () => { spawn("kafka-spark"); spawn("kafka-web"); });
  }

  function onStats(msg) {
    state.coins = msg.coins;
    const syms = Object.keys(msg.coins);
    if (syms.some((s) => !state.symbols.includes(s))) {
      state.symbols = [...state.symbols, ...syms.filter((s) => !state.symbols.includes(s))];
      if (!state.symbol) state.symbol = state.symbols[0];
      buildCoins(); loadHistory();
    }
    renderCoinStats();
    applyTail(msg.tails[state.symbol]);
    for (const a of msg.alerts) state.alerts.set(`${a.symbol}|${a.time}`, a);
    renderAlerts(); renderMarkers();
    renderPipeline(msg.pipeline);
  }

  function setConn(s, text) {
    const el = $("conn"); el.dataset.state = s; el.querySelector("span").textContent = text;
  }

  let backoff = 1000;
  function connect() {
    const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`);
    ws.onopen = () => { backoff = 1000; setConn("live", "Live"); };
    ws.onmessage = (e) => {
      const m = JSON.parse(e.data);
      if (m.type === "trades") onTrades(m); else if (m.type === "stats") onStats(m);
    };
    ws.onclose = () => {
      setConn("down", "Reconnecting");
      setTimeout(connect, backoff); backoff = Math.min(backoff * 2, 15000);
    };
  }

  // ------------------------------------------------------------------ boot
  document.querySelectorAll(".ranges button").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll(".ranges button").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
    state.minutes = Number(b.dataset.min);
    loadHistory(); loadOverview();
  }));

  (async () => {
    const o = await loadOverview().catch(() => null);
    state.symbols = (o && o.symbols) || ["BTC-USD", "ETH-USD", "SOL-USD"];
    state.symbol = state.symbols[0];
    buildCoins();
    renderAlerts();
    await loadHistory().catch(() => {});
    if (document.fonts) await document.fonts.ready.catch(() => {});
    layoutWires();
    connect();
    setInterval(() => loadOverview().catch(() => {}), 10000);
  })();
})();
