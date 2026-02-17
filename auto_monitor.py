#!/usr/bin/env python3
"""
BTC 5-Minute Auto-Discovery High-Throughput Monitor

Automatically discovers the current/next BTC 5-minute market on Polymarket,
then monitors it at up to 150 req/s using concurrent connections.

Usage:
  python3 auto_monitor.py              # auto-find, 50 conns, 5 min
  python3 auto_monitor.py -d 300       # explicit duration
  python3 auto_monitor.py -c 10        # fewer connections
"""

import asyncio
import json
import math
import os
import sqlite3
import sys
import threading
import time
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import requests
from websocket import WebSocketApp

import config

# ── Unbuffered print ─────────────────────────────────────────────────────────
_print = print
def print(*a, **kw):
    kw.setdefault("flush", True)
    _print(*a, **kw)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
# Override log handlers to flush
for h in logging.root.handlers:
    h.flush = lambda: None  # already line-buffered on terminals

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Auto-Discovery
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def discover_btc_5min_market():
    """
    Discover the current or next BTC 5-minute market.
    Returns dict with market_id, token_id_yes, token_id_no, question, slug, meta
    or None if nothing found.
    """
    now = int(time.time())
    base = now - (now % 300)

    # Try current window, next, and the one after
    candidates = [base, base + 300, base + 600]

    for ts in candidates:
        slug = f"btc-updown-5m-{ts}"
        log.info("Trying slug: %s", slug)
        try:
            resp = requests.get(
                f"{config.GAMMA_API_URL}/events",
                params={"slug": slug},
                timeout=10,
            )
            resp.raise_for_status()
            events = resp.json()
            if not events:
                continue

            markets = events[0].get("markets", [])
            for m in markets:
                resolved = m.get("resolved")
                closed = m.get("closed", False)
                if resolved:
                    continue  # skip already resolved

                token_ids = m.get("clobTokenIds", "[]")
                if isinstance(token_ids, str):
                    token_ids = json.loads(token_ids)
                if len(token_ids) < 2:
                    continue

                question = m.get("question", slug)
                condition_id = m.get("conditionId", "")

                log.info("Found: %s", question)
                log.info("  closed=%s  resolved=%s", closed, resolved)
                log.info("  condition_id: %s", condition_id[:40])

                return {
                    "slug": slug,
                    "market_id": condition_id,
                    "condition_id": condition_id,
                    "token_id_yes": token_ids[0],
                    "token_id_no": token_ids[1],
                    "question": question,
                    "meta": m,
                    "window_start_ts": ts,
                    "window_end_ts": ts + 300,
                }

        except Exception as e:
            log.debug("Slug %s failed: %s", slug, e)

    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Database
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def init_db(db_path):
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS pm_snapshot (
        snap_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms       INTEGER NOT NULL,
        market_id   TEXT NOT NULL,
        token_side  TEXT NOT NULL DEFAULT 'yes',  -- 'yes' or 'no'
        best_bid_p  REAL,
        best_bid_sz REAL,
        best_ask_p  REAL,
        best_ask_sz REAL,
        spread      REAL,
        bid_levels  INTEGER,
        ask_levels  INTEGER,
        latency_ms  REAL
    );
    CREATE INDEX IF NOT EXISTS idx_snap_ts ON pm_snapshot(ts_ms);
    CREATE INDEX IF NOT EXISTS idx_snap_ts_side ON pm_snapshot(ts_ms, token_side);

    CREATE TABLE IF NOT EXISTS pm_ob_level (
        snap_id     INTEGER NOT NULL,
        side        TEXT NOT NULL,
        level       INTEGER NOT NULL,
        price       REAL,
        size        REAL,
        PRIMARY KEY (snap_id, side, level)
    ) WITHOUT ROWID;

    CREATE TABLE IF NOT EXISTS pm_trades_raw (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms           INTEGER NOT NULL,
        market_id       TEXT NOT NULL,
        asset_id        TEXT,
        price           REAL,
        size            REAL,
        raw_json        TEXT,
        ingest_ts_ms    INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_trades_ts ON pm_trades_raw(ts_ms);

    CREATE TABLE IF NOT EXISTS pm_market_meta (
        market_id   TEXT PRIMARY KEY,
        slug        TEXT,
        question    TEXT,
        token_yes   TEXT,
        token_no    TEXT,
        meta_json   TEXT,
        created_at  TEXT
    );
    """)
    conn.commit()
    return conn


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Trade Tracker (WebSocket)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TradeTracker:
    def __init__(self, market_id, token_yes, token_no):
        self.market_id = market_id
        self.asset_map = {token_yes: market_id, token_no: market_id}
        self.lock = threading.Lock()
        self.buffer = []

    def on_trade(self, evt):
        aid = evt.get("asset_id", "")
        if aid in self.asset_map:
            ts_ms = int(time.time() * 1000)
            with self.lock:
                self.buffer.append((ts_ms, evt))

    def flush(self):
        with self.lock:
            buf = self.buffer
            self.buffer = []
        return buf


def start_websocket(token_yes, token_no, tracker):
    stop_event = threading.Event()

    def on_open(ws):
        ws.send(json.dumps({
            "assets_ids": [token_yes, token_no],
            "type": "market",
        }))
        log.info("WSS subscribed to 2 tokens")

    def on_message(ws, message):
        if message == "PONG":
            return
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        events = data if isinstance(data, list) else [data]
        for evt in events:
            if evt.get("event_type") == "last_trade_price":
                tracker.on_trade(evt)

    def on_error(ws, error):
        log.warning("WSS error: %s", error)

    def on_close(ws, code, msg):
        log.info("WSS closed (code=%s)", code)

    def ping_loop(ws):
        while not stop_event.is_set():
            try:
                ws.send("PING")
            except Exception:
                break
            time.sleep(10)

    def run():
        while not stop_event.is_set():
            try:
                url = config.CLOB_WSS_URL + "/ws/market"
                ws = WebSocketApp(url, on_message=on_message,
                                  on_error=on_error, on_close=on_close,
                                  on_open=on_open)
                threading.Thread(target=ping_loop, args=(ws,), daemon=True).start()
                ws.run_forever()
            except Exception as e:
                log.warning("WSS reconnecting: %s", e)
            if not stop_event.is_set():
                time.sleep(2)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return stop_event


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Async Book Fetcher Workers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def book_worker(session, token_id, market_id, queue, stop, token_side="yes"):
    """
    Fire /book requests back-to-back.
    Push (ts_ms, latency_ms, book_json, token_side) into queue.
    """
    url = f"{config.CLOB_REST_URL}/book"
    params = {"token_id": token_id}
    timeout = aiohttp.ClientTimeout(total=10)

    while not stop.is_set():
        t0 = time.perf_counter()
        ts_ms = int(time.time() * 1000)
        try:
            async with session.get(url, params=params, timeout=timeout) as resp:
                if resp.status == 200:
                    body = await resp.json()
                    latency = (time.perf_counter() - t0) * 1000
                    await queue.put((ts_ms, latency, body, token_side))
                elif resp.status == 404:
                    # Market book removed (settled)
                    await asyncio.sleep(1)
                elif resp.status == 429:
                    # Throttled — tiny backoff
                    await asyncio.sleep(0.05)
                else:
                    await resp.read()
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.1)


def parse_book(book):
    """Parse a /book response into sorted bid/ask lists and L1 stats."""
    bids_raw = book.get("bids") or []
    asks_raw = book.get("asks") or []

    bids = sorted(
        [(float(x["price"]), float(x["size"])) for x in bids_raw],
        key=lambda x: x[0], reverse=True
    )
    asks = sorted(
        [(float(x["price"]), float(x["size"])) for x in asks_raw],
        key=lambda x: x[0]
    )

    best_bid_p = bids[0][0] if bids else None
    best_bid_sz = bids[0][1] if bids else None
    best_ask_p = asks[0][0] if asks else None
    best_ask_sz = asks[0][1] if asks else None
    spread = round(best_ask_p - best_bid_p, 4) if (best_bid_p and best_ask_p) else None

    return bids, asks, best_bid_p, best_bid_sz, best_ask_p, best_ask_sz, spread


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DB Writer — batch flush
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def flush_to_db(conn, snap_batch, trade_batch, market_id):
    """
    Batch insert snapshots + OB levels + trades.
    snap_batch: list of (ts_ms, latency_ms, bids, asks, L1 fields, token_side)
    trade_batch: list of (ts_ms, evt_dict)
    """
    if not snap_batch and not trade_batch:
        return 0, 0

    cursor = conn.cursor()
    ob_count = 0

    for (ts_ms, latency_ms, bids, asks,
         best_bid_p, best_bid_sz, best_ask_p, best_ask_sz, spread,
         token_side) in snap_batch:

        cursor.execute("""
            INSERT INTO pm_snapshot
            (ts_ms, market_id, token_side, best_bid_p, best_bid_sz, best_ask_p,
             best_ask_sz, spread, bid_levels, ask_levels, latency_ms)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (ts_ms, market_id, token_side, best_bid_p, best_bid_sz,
              best_ask_p, best_ask_sz,
              spread, len(bids), len(asks), round(latency_ms, 2)))

        snap_id = cursor.lastrowid

        ob_rows = []
        for i, (p, s) in enumerate(bids, 1):
            ob_rows.append((snap_id, "bid", i, p, s))
        for i, (p, s) in enumerate(asks, 1):
            ob_rows.append((snap_id, "ask", i, p, s))

        if ob_rows:
            cursor.executemany("""
                INSERT INTO pm_ob_level (snap_id, side, level, price, size)
                VALUES (?,?,?,?,?)
            """, ob_rows)
            ob_count += len(ob_rows)

    # Trades
    ingest_ts = int(time.time() * 1000)
    for ts_ms, evt in trade_batch:
        cursor.execute("""
            INSERT INTO pm_trades_raw
            (ts_ms, market_id, asset_id, price, size, raw_json, ingest_ts_ms)
            VALUES (?,?,?,?,?,?,?)
        """, (
            ts_ms, market_id,
            evt.get("asset_id"),
            float(evt["price"]) if evt.get("price") else None,
            float(evt["size"]) if evt.get("size") else None,
            json.dumps(evt),
            ingest_ts,
        ))

    conn.commit()
    return len(snap_batch), ob_count


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main async monitor
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def run_monitor(market, concurrency, duration, output_dir):
    market_id = market["market_id"]
    token_yes = market["token_id_yes"]
    token_no = market["token_id_no"]
    question = market["question"]

    # Session dir
    start_utc = datetime.now(timezone.utc)
    ts_tag = start_utc.strftime("%Y%m%dT%H%M%S")
    slug = market["slug"]
    session_dir = os.path.join(output_dir, f"{slug}_{ts_tag}")
    os.makedirs(session_dir, exist_ok=True)
    db_path = os.path.join(session_dir, "ob_data.db")

    log.info("Session: %s", session_dir)
    log.info("Market:  %s", question)
    log.info("Concurrency: %d  Duration: %ds", concurrency, duration)

    # DB
    conn = init_db(db_path)
    conn.execute("""
        INSERT OR REPLACE INTO pm_market_meta
        (market_id, slug, question, token_yes, token_no, meta_json, created_at)
        VALUES (?,?,?,?,?,?,?)
    """, (market_id, market["slug"], question,
          token_yes, token_no, json.dumps(market["meta"]),
          start_utc.isoformat()))
    conn.commit()

    # WebSocket trades
    tracker = TradeTracker(market_id, token_yes, token_no)
    ws_stop = start_websocket(token_yes, token_no, tracker)
    await asyncio.sleep(2)  # let WSS connect

    # Async book fetchers
    queue = asyncio.Queue(maxsize=5000)
    stop = asyncio.Event()

    connector = aiohttp.TCPConnector(limit=concurrency + 10, force_close=False)
    session = aiohttp.ClientSession(connector=connector)

    workers = []
    half = concurrency // 2
    for _ in range(half):
        t = asyncio.create_task(
            book_worker(session, token_yes, market_id, queue, stop, "yes"))
        workers.append(t)
    for _ in range(concurrency - half):
        t = asyncio.create_task(
            book_worker(session, token_no, market_id, queue, stop, "no"))
        workers.append(t)

    log.info("Started %d book fetcher workers (%d YES + %d NO)",
             concurrency, half, concurrency - half)

    # Main drain + flush loop
    total_snaps = 0
    total_ob = 0
    total_trades = 0
    t_start = time.time()
    last_log = t_start

    try:
        while True:
            elapsed = time.time() - t_start
            if elapsed >= duration:
                log.info("Duration limit reached (%ds). Stopping.", duration)
                break

            # Drain queue
            snap_batch = []
            deadline = time.time() + 1.0  # flush every 1s
            while time.time() < deadline:
                try:
                    ts_ms, latency_ms, book, token_side = queue.get_nowait()
                    bids, asks, bb_p, bb_sz, ba_p, ba_sz, spread = parse_book(book)
                    snap_batch.append((ts_ms, latency_ms, bids, asks,
                                      bb_p, bb_sz, ba_p, ba_sz, spread,
                                      token_side))
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.05)

            # Drain trades
            trade_batch = tracker.flush()

            # Flush to DB
            n_snap, n_ob = flush_to_db(conn, snap_batch, trade_batch, market_id)
            total_snaps += n_snap
            total_ob += n_ob
            total_trades += len(trade_batch)

            # Periodic log (every 5s)
            now = time.time()
            if now - last_log >= 5:
                elapsed_s = now - t_start
                rps = total_snaps / elapsed_s if elapsed_s > 0 else 0
                remaining = duration - elapsed_s
                avg_lat = 0
                if snap_batch:
                    avg_lat = sum(s[1] for s in snap_batch) / len(snap_batch)
                log.info(
                    "t=%3.0fs | snaps=%d (%.1f/s) | OB=%d | trades=%d | "
                    "lat=%.0fms | q=%d | remaining=%.0fs",
                    elapsed_s, total_snaps, rps, total_ob, total_trades,
                    avg_lat, queue.qsize(), remaining,
                )
                last_log = now

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        # Shutdown
        stop.set()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await session.close()
        ws_stop.set()

        end_utc = datetime.now(timezone.utc)
        elapsed = (end_utc - start_utc).total_seconds()

        # Final stats
        log.info("=" * 60)
        log.info("FINAL STATS")
        log.info("  Duration:   %.1fs", elapsed)
        log.info("  Snapshots:  %d (%.1f/s)", total_snaps,
                 total_snaps / elapsed if elapsed else 0)
        log.info("  OB levels:  %d", total_ob)
        log.info("  Trades:     %d", total_trades)

        # DB file size
        db_size = os.path.getsize(db_path)
        wal_path = db_path + "-wal"
        if os.path.exists(wal_path):
            db_size += os.path.getsize(wal_path)
        log.info("  DB size:    %.1f MB", db_size / 1024 / 1024)

        # Verify
        snap_count = conn.execute("SELECT COUNT(*) FROM pm_snapshot").fetchone()[0]
        ob_count = conn.execute("SELECT COUNT(*) FROM pm_ob_level").fetchone()[0]
        trade_count = conn.execute("SELECT COUNT(*) FROM pm_trades_raw").fetchone()[0]
        ts_range = conn.execute(
            "SELECT MIN(ts_ms), MAX(ts_ms) FROM pm_snapshot").fetchone()

        log.info("  DB verify:  snaps=%d  ob=%d  trades=%d",
                 snap_count, ob_count, trade_count)
        if ts_range[0] and ts_range[1]:
            span_s = (ts_range[1] - ts_range[0]) / 1000
            log.info("  Time span:  %.1fs", span_s)

        # Notes
        description = market.get("meta", {}).get("description", "")
        desc_section = ""
        if description:
            desc_section = f"\n## Settlement Rules\n{description}\n"

        notes = f"""# BTC Auto Monitor Session

## Market
- **Question**: {question}
- **Slug**: {market['slug']}
- **Condition ID**: {market_id}
- **Token YES**: {token_yes}
- **Token NO**: {token_no}
{desc_section}
## Recording
- **Start**: {start_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}
- **End**: {end_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}
- **Duration**: {elapsed:.0f}s
- **Concurrency**: {concurrency} ({half} YES + {concurrency - half} NO)
- **Snapshots**: {snap_count} ({snap_count/elapsed:.1f}/s)
- **OB levels**: {ob_count}
- **Trades**: {trade_count}
- **DB size**: {db_size/1024/1024:.1f} MB

## Tables
- `pm_snapshot` — per-request L1 + latency (ms timestamps), token_side=yes/no
- `pm_ob_level` — full orderbook keyed by snap_id
- `pm_trades_raw` — WebSocket trade events
- `pm_market_meta` — market metadata
"""
        with open(os.path.join(session_dir, "notes.md"), "w") as f:
            f.write(notes)

        conn.close()
        log.info("=" * 60)
        log.info("Done. Data at %s", session_dir)

    return session_dir


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    import argparse
    parser = argparse.ArgumentParser(description="BTC 5-min Auto Monitor")
    parser.add_argument("-d", "--duration", type=int, default=300,
                        help="Recording duration in seconds (default: 300)")
    parser.add_argument("-c", "--concurrency", type=int, default=50,
                        help="Number of concurrent /book fetchers (default: 50)")
    parser.add_argument("-o", "--output", default=config.DEFAULT_OUTPUT,
                        help="Output directory")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("BTC 5-Minute Auto-Discovery Monitor")
    log.info("=" * 60)

    # Discover market
    market = discover_btc_5min_market()
    if not market:
        log.error("No active BTC 5-min market found!")
        sys.exit(1)

    log.info("Discovered: %s", market["question"])

    # Run monitor
    asyncio.run(run_monitor(market, args.concurrency, args.duration, args.output))


if __name__ == "__main__":
    main()
