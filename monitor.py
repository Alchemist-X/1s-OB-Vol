#!/usr/bin/env python3
"""
Multi-market Polymarket Order Book Monitor.

Monitors multiple markets simultaneously at 1-second intervals,
collecting FULL order book data (all price levels) and raw trade events
until all markets settle.

Usage:
  python3 monitor.py <url1> <url2> [url3] ...
"""

import json
import os
import re
import sqlite3
import sys
import threading
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs

import requests
from websocket import WebSocketApp

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Database
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_monitor_tables(conn: sqlite3.Connection):
    conn.executescript("""
    -- Per-second summary: L1, spread, depth, volume (YES + NO tokens)
    CREATE TABLE IF NOT EXISTS pm_sports_market_1s (
        ts_utc          TEXT    NOT NULL,
        market_id       TEXT    NOT NULL,
        volume_1s       REAL    DEFAULT 0,
        trades_1s       INTEGER DEFAULT 0,
        best_bid_p      REAL,
        best_bid_sz     REAL,
        best_ask_p      REAL,
        best_ask_sz     REAL,
        depth_bid_1c    REAL,
        depth_ask_1c    REAL,
        depth_bid_5c    REAL,
        depth_ask_5c    REAL,
        depth_bid_10c   REAL,
        depth_ask_10c   REAL,
        depth_bid_15c   REAL,
        depth_ask_15c   REAL,
        spread          REAL,
        -- NO token fields
        best_bid_p_no      REAL,
        best_bid_sz_no     REAL,
        best_ask_p_no      REAL,
        best_ask_sz_no     REAL,
        depth_bid_1c_no    REAL,
        depth_ask_1c_no    REAL,
        depth_bid_5c_no    REAL,
        depth_ask_5c_no    REAL,
        depth_bid_10c_no   REAL,
        depth_ask_10c_no   REAL,
        depth_bid_15c_no   REAL,
        depth_ask_15c_no   REAL,
        spread_no          REAL,
        ingest_ts_utc   TEXT    NOT NULL,
        PRIMARY KEY (market_id, ts_utc)
    );

    -- Full order book: ALL price levels, every second, YES + NO
    CREATE TABLE IF NOT EXISTS pm_orderbook_full_1s (
        ts_utc          TEXT    NOT NULL,
        market_id       TEXT    NOT NULL,
        token_side      TEXT    NOT NULL DEFAULT 'yes',  -- 'yes' or 'no'
        side            TEXT    NOT NULL,   -- 'bid' or 'ask'
        level           INTEGER NOT NULL,   -- 1 = best
        price           REAL,
        size            REAL,
        ingest_ts_utc   TEXT    NOT NULL,
        PRIMARY KEY (market_id, ts_utc, token_side, side, level)
    );

    -- Raw trade events from WebSocket
    CREATE TABLE IF NOT EXISTS pm_trades_raw (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_utc          TEXT    NOT NULL,
        market_id       TEXT    NOT NULL,
        asset_id        TEXT,
        price           REAL,
        size            REAL,
        raw_json        TEXT,
        ingest_ts_utc   TEXT    NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_trades_market_ts
        ON pm_trades_raw(market_id, ts_utc);

    -- Market metadata
    CREATE TABLE IF NOT EXISTS pm_market_meta (
        market_id       TEXT    PRIMARY KEY,
        slug            TEXT,
        question        TEXT,
        condition_id    TEXT,
        token_id_yes    TEXT,
        token_id_no     TEXT,
        meta_json       TEXT,
        created_at      TEXT
    );
    """)
    conn.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Trade Tracker (per-market, thread-safe)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TradeTracker:
    """Thread-safe per-market trade buffer."""

    def __init__(self):
        self.lock = threading.Lock()
        self.buffers = {}          # market_id -> [events]
        self.asset_to_market = {}   # asset_id -> market_id

    def register_market(self, market_id: str, token_yes: str, token_no: str):
        with self.lock:
            self.buffers[market_id] = []
            self.asset_to_market[token_yes] = market_id
            self.asset_to_market[token_no] = market_id

    def on_trade(self, evt: dict):
        asset_id = evt.get("asset_id", "")
        with self.lock:
            market_id = self.asset_to_market.get(asset_id)
            if market_id:
                self.buffers[market_id].append(evt)

    def flush(self, market_id: str):
        """Return (raw_events, total_volume, trade_count) and clear buffer."""
        with self.lock:
            buf = self.buffers.get(market_id, [])
            self.buffers[market_id] = []
        volume = sum(float(t.get("size", 0)) for t in buf)
        return buf, volume, len(buf)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  WebSocket
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class WsManager:
    """Manages a WebSocket connection with automatic reconnect."""

    def __init__(self, all_token_ids, trade_tracker):
        self.token_ids = all_token_ids
        self.tracker = trade_tracker
        self.ws = None
        self._stop = threading.Event()

    def start(self):
        threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self):
        while not self._stop.is_set():
            try:
                self._connect()
            except Exception as e:
                log.warning("WSS connection failed: %s — reconnecting in 3s", e)
            if not self._stop.is_set():
                time.sleep(3)

    def _connect(self):
        url = config.CLOB_WSS_URL + "/ws/market"

        def on_open(ws):
            ws.send(json.dumps({
                "assets_ids": self.token_ids,
                "type": "market",
            }))
            log.info("WSS subscribed to %d asset_ids", len(self.token_ids))
            # Start ping thread
            threading.Thread(target=self._ping, args=(ws,), daemon=True).start()

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
                    self.tracker.on_trade(evt)

        def on_error(ws, error):
            log.warning("WSS error: %s", error)

        def on_close(ws, status_code, msg):
            log.info("WSS closed (code=%s msg=%s)", status_code, msg)

        self.ws = WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        self.ws.run_forever()

    def _ping(self, ws):
        while not self._stop.is_set():
            try:
                ws.send("PING")
            except Exception:
                break
            time.sleep(10)

    def close(self):
        self._stop.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REST API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_http = requests.Session()
_http.headers.update({"Accept": "application/json"})


def fetch_book(token_id: str):
    """Fetch full order book for a token."""
    for attempt in range(3):
        try:
            resp = _http.get(
                f"{config.CLOB_REST_URL}/book",
                params={"token_id": token_id},
                timeout=4,
            )
            # 404 = book doesn't exist (market settled), don't retry
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                wait = 0.3 * (2 ** attempt)
                log.warning("Rate limited, retrying in %.1fs", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == 2:
                log.warning("REST /book failed: %s", e)
    return None


def check_market_resolved(condition_id: str):
    """
    Check if a market has settled via Gamma API.
    Returns (is_resolved, is_closed) tuple.
    - resolved: market has a final outcome
    - closed: market is closed for new orders (may not be resolved yet)
    """
    try:
        resp = _http.get(
            f"{config.GAMMA_API_URL}/markets",
            params={"condition_id": condition_id},
            timeout=10,
        )
        resp.raise_for_status()
        markets = resp.json()
        if markets:
            m = markets[0]
            resolved = bool(m.get("resolved"))
            closed = bool(m.get("closed"))
            return resolved, closed
    except Exception as e:
        log.debug("Settlement check failed: %s", e)
    return False, False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Order Book Parsing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def parse_all_levels(raw, side):
    """Parse ALL levels from the order book — no truncation."""
    parsed = [(float(x["price"]), float(x["size"])) for x in raw]
    if side == "bid":
        parsed.sort(key=lambda x: x[0], reverse=True)
    else:
        parsed.sort(key=lambda x: x[0])
    return [{"price": p, "size": s} for p, s in parsed]


def compute_depth(levels_raw, best, side, cents):
    delta = cents / 100.0
    total = 0.0
    for lv in levels_raw:
        p, s = float(lv["price"]), float(lv["size"])
        if side == "bid" and p >= best - delta:
            total += s
        elif side == "ask" and p <= best + delta:
            total += s
    return total


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  URL Parsing & Market Resolution
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _parse_json_field(raw):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
    return []


def resolve_market_from_url(url: str):
    """
    Resolve a Polymarket URL into market identifiers.
    Handles both /event/<slug> and ?slug=<market_slug> patterns.
    """
    url = url.strip().rstrip("/")

    # Extract event slug from path
    m = re.search(r"polymarket\.com/event/([^/?#]+)", url)
    event_slug = m.group(1) if m else url

    # Extract optional market slug from query param
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    market_slug = qs.get("slug", [None])[0]

    log.info("Resolving: event_slug=%s, market_slug=%s", event_slug, market_slug)

    market = None

    # 1) Try events API with event slug
    try:
        resp = _http.get(
            f"{config.GAMMA_API_URL}/events",
            params={"slug": event_slug},
            timeout=10,
        )
        resp.raise_for_status()
        events = resp.json()
        if events:
            all_markets = events[0].get("markets", [])
            if all_markets:
                # If market_slug provided, find matching market
                if market_slug:
                    for mk in all_markets:
                        if mk.get("slug") == market_slug:
                            market = mk
                            break
                if not market:
                    market = all_markets[0]
    except Exception as e:
        log.debug("Events API failed for %s: %s", event_slug, e)

    # 2) Fallback: try markets API with market_slug
    if not market and market_slug:
        try:
            resp = _http.get(
                f"{config.GAMMA_API_URL}/markets",
                params={"slug": market_slug},
                timeout=10,
            )
            resp.raise_for_status()
            markets_list = resp.json()
            if markets_list:
                market = markets_list[0]
        except Exception as e:
            log.debug("Markets API failed for %s: %s", market_slug, e)

    # 3) Fallback: try markets API with event_slug
    if not market:
        try:
            resp = _http.get(
                f"{config.GAMMA_API_URL}/markets",
                params={"slug": event_slug},
                timeout=10,
            )
            resp.raise_for_status()
            markets_list = resp.json()
            if markets_list:
                market = markets_list[0]
        except Exception as e:
            log.debug("Markets API failed for %s: %s", event_slug, e)

    if not market:
        log.error("Could not resolve market for URL: %s", url)
        return None

    condition_id = market.get("conditionId", "")
    token_ids = _parse_json_field(market.get("clobTokenIds", "[]"))

    if len(token_ids) < 2:
        log.error("Market has fewer than 2 token IDs: %s", token_ids)
        return None

    question = market.get("question", event_slug)
    log.info("  → %s", question)
    log.info("    condition_id: %s", condition_id[:40])
    log.info("    token_yes:    %s", token_ids[0][:40])
    log.info("    token_no:     %s", token_ids[1][:40])

    return {
        "slug": event_slug,
        "condition_id": condition_id,
        "market_id": condition_id,
        "token_id_yes": token_ids[0],
        "token_id_no": token_ids[1],
        "meta": market,
        "question": question,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Notes
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def write_notes(session_dir, markets_info, start_utc, end_utc):
    duration = int((end_utc - start_utc).total_seconds())
    lines = [
        "# Multi-Market Monitor Session\n",
        "## Recording Window",
        f"- **Start (UTC)**: {start_utc.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- **End (UTC)**: {end_utc.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- **Duration**: {duration}s ({duration // 60}m {duration % 60}s)",
        f"- **Sampling**: 1s interval",
        f"- **Markets**: {len(markets_info)}",
        "",
    ]
    for i, m in enumerate(markets_info, 1):
        lines += [
            f"## Market {i}: {m['question']}",
            f"- **Slug**: {m['slug']}",
            f"- **Condition ID**: {m['condition_id']}",
            f"- **Token Yes**: {m['token_id_yes']}",
            f"- **Token No**: {m['token_id_no']}",
            "",
        ]
        # Include settlement rules (description) from market metadata
        description = m.get("meta", {}).get("description", "")
        if description:
            lines += [
                f"### Settlement Rules",
                description,
                "",
            ]
    lines += [
        "## Database Tables",
        "- `pm_sports_market_1s` — per-second L1 + volume + spread + depth (YES + NO tokens)",
        "- `pm_orderbook_full_1s` — per-second full order book, ALL price levels (YES + NO tokens)",
        "- `pm_trades_raw` — raw trade events from WebSocket",
        "- `pm_market_meta` — market metadata",
    ]
    with open(os.path.join(session_dir, "notes.md"), "w") as f:
        f.write("\n".join(lines))
    log.info("Notes written.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Main Monitoring Loop
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _parse_one_book(book, depth_cents):
    """Parse a single book into L1 stats, depths, and sorted levels."""
    best_bid_p = best_bid_sz = best_ask_p = best_ask_sz = None
    spread = None
    depths = {f"depth_{s}_{c}c": 0.0 for s in ("bid", "ask") for c in depth_cents}
    all_bids = []
    all_asks = []

    if book:
        bids_raw = book.get("bids") or []
        asks_raw = book.get("asks") or []
        all_bids = parse_all_levels(bids_raw, "bid")
        all_asks = parse_all_levels(asks_raw, "ask")

        if all_bids:
            best_bid_p, best_bid_sz = all_bids[0]["price"], all_bids[0]["size"]
        if all_asks:
            best_ask_p, best_ask_sz = all_asks[0]["price"], all_asks[0]["size"]
        if best_bid_p is not None and best_ask_p is not None:
            spread = round(best_ask_p - best_bid_p, 4)

        for c in depth_cents:
            if best_bid_p is not None:
                depths[f"depth_bid_{c}c"] = round(
                    compute_depth(bids_raw, best_bid_p, "bid", c), 6)
            if best_ask_p is not None:
                depths[f"depth_ask_{c}c"] = round(
                    compute_depth(asks_raw, best_ask_p, "ask", c), 6)

    return best_bid_p, best_bid_sz, best_ask_p, best_ask_sz, spread, depths, all_bids, all_asks


def process_one_market(m, book_yes, book_no, raw_trades, volume_1s, trades_1s,
                       ts_utc, ingest_ts, depth_cents):
    """
    Process a single market tick with both YES and NO order books.
    Returns (market_row, ob_rows, trade_db_rows).
    """
    market_id = m["market_id"]

    # Prepare trade DB rows
    trade_db_rows = []
    for t in raw_trades:
        trade_db_rows.append((
            ts_utc, market_id,
            t.get("asset_id"),
            float(t["price"]) if t.get("price") else None,
            float(t["size"]) if t.get("size") else None,
            json.dumps(t),
            ingest_ts,
        ))

    # Parse YES book
    bb_p, bb_sz, ba_p, ba_sz, spread, depths, bids_yes, asks_yes = \
        _parse_one_book(book_yes, depth_cents)

    # Parse NO book
    bb_p_no, bb_sz_no, ba_p_no, ba_sz_no, spread_no, depths_no, bids_no, asks_no = \
        _parse_one_book(book_no, depth_cents)

    # Build OB rows for both YES and NO
    ob_rows = []
    for token_side, all_bids, all_asks in [("yes", bids_yes, asks_yes),
                                            ("no", bids_no, asks_no)]:
        for side_name, levels in [("bid", all_bids), ("ask", all_asks)]:
            for i, lv in enumerate(levels, 1):
                ob_rows.append({
                    "ts_utc": ts_utc,
                    "market_id": market_id,
                    "token_side": token_side,
                    "side": side_name,
                    "level": i,
                    "price": lv["price"],
                    "size": lv["size"],
                    "ingest_ts_utc": ingest_ts,
                })

    # NO depths with _no suffix
    no_depths = {k + "_no": v for k, v in depths_no.items()}

    market_row = {
        "ts_utc": ts_utc,
        "market_id": market_id,
        "volume_1s": round(volume_1s, 6),
        "trades_1s": trades_1s,
        "best_bid_p": bb_p,
        "best_bid_sz": bb_sz,
        "best_ask_p": ba_p,
        "best_ask_sz": ba_sz,
        "spread": spread,
        "best_bid_p_no": bb_p_no,
        "best_bid_sz_no": bb_sz_no,
        "best_ask_p_no": ba_p_no,
        "best_ask_sz_no": ba_sz_no,
        "spread_no": spread_no,
        "ingest_ts_utc": ingest_ts,
        **depths,
        **no_depths,
    }

    return market_row, ob_rows, trade_db_rows


def monitor(urls, output_dir=None, max_duration=None):
    """
    Monitor markets. If max_duration is set (seconds), stop after that time
    regardless of settlement. Otherwise run until all markets settle.
    """
    if output_dir is None:
        output_dir = config.DEFAULT_OUTPUT

    depth_cents = config.DEPTH_CENTS

    # ── 1. Resolve all markets ────────────────────────────────────────────
    log.info("=" * 60)
    log.info("Resolving %d markets...", len(urls))
    markets_info = []
    for url in urls:
        m = resolve_market_from_url(url)
        if m:
            markets_info.append(m)
        else:
            log.error("FATAL: Cannot resolve %s", url)
            sys.exit(1)

    log.info("Successfully resolved %d markets:", len(markets_info))
    for i, m in enumerate(markets_info, 1):
        log.info("  [%d] %s", i, m["question"])
    log.info("=" * 60)

    # ── 2. Session directory & DB ─────────────────────────────────────────
    start_utc = datetime.now(timezone.utc)
    ts_tag = start_utc.strftime("%Y%m%dT%H%M%S")
    slug = markets_info[0]["slug"]
    session_dir = os.path.join(output_dir, f"{slug}_{ts_tag}")
    os.makedirs(session_dir, exist_ok=True)
    db_path = os.path.join(session_dir, "ob_data.db")
    log.info("Session dir: %s", session_dir)

    conn = get_conn(db_path)
    init_monitor_tables(conn)

    # Store market metadata
    for m in markets_info:
        conn.execute("""
            INSERT OR REPLACE INTO pm_market_meta
            (market_id, slug, question, condition_id,
             token_id_yes, token_id_no, meta_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            m["market_id"], m["slug"], m["question"], m["condition_id"],
            m["token_id_yes"], m["token_id_no"],
            json.dumps(m["meta"]), start_utc.isoformat(),
        ))
    conn.commit()

    # ── 3. Trade tracker & WebSocket ──────────────────────────────────────
    trade_tracker = TradeTracker()
    all_token_ids = []
    for m in markets_info:
        trade_tracker.register_market(m["market_id"], m["token_id_yes"], m["token_id_no"])
        all_token_ids.extend([m["token_id_yes"], m["token_id_no"]])

    ws_mgr = WsManager(all_token_ids, trade_tracker)
    ws_mgr.start()
    time.sleep(2)  # Let WebSocket connect

    # ── 4. Main loop ─────────────────────────────────────────────────────
    if max_duration:
        log.info("▶ Starting 1-second monitoring for %ds (%dm). Ctrl+C to stop.",
                 max_duration, max_duration // 60)
    else:
        log.info("▶ Starting 1-second monitoring (until settlement). Ctrl+C to stop.")
    settled = {m["market_id"]: False for m in markets_info}
    book_404_count = {m["market_id"]: 0 for m in markets_info}  # Track consecutive 404s
    tick = 0
    last_settle_check = time.time()
    post_settle_countdown = -1  # -1 = not in countdown
    SETTLE_CHECK_INTERVAL = 30

    pool = ThreadPoolExecutor(max_workers=max(len(markets_info) * 2, 4))
    start_t = time.time()

    try:
        while True:
            # Sleep until next tick
            target = start_t + (tick + 1) * 1.0
            sleep_for = target - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

            now_utc = datetime.now(timezone.utc)
            ts_utc = now_utc.strftime("%Y-%m-%d %H:%M:%SZ")
            ingest_ts = now_utc.strftime("%Y-%m-%d %H:%M:%S.%fZ")

            # Fetch YES + NO order books in parallel (skip markets with persistent 404)
            futures = {}
            for m in markets_info:
                mid = m["market_id"]
                if not settled[mid] and book_404_count[mid] < 10:
                    f_yes = pool.submit(fetch_book, m["token_id_yes"])
                    futures[f_yes] = (m, "yes")
                    f_no = pool.submit(fetch_book, m["token_id_no"])
                    futures[f_no] = (m, "no")

            books_yes = {}
            books_no = {}
            for f in as_completed(futures):
                m, token_side = futures[f]
                result = f.result()
                mid = m["market_id"]
                if token_side == "yes":
                    books_yes[mid] = result
                else:
                    books_no[mid] = result
                if result is None:
                    book_404_count[mid] += 1
                    if book_404_count[mid] == 20:  # 10 per side x 2
                        log.warning("Market %s: book unavailable 10x, pausing REST polls",
                                    m["question"][:40])
                else:
                    book_404_count[mid] = 0  # Reset on success

            # Process each market
            all_market_rows = []
            all_ob_rows = []
            all_trade_rows = []

            for m in markets_info:
                mid = m["market_id"]
                if settled[mid]:
                    continue

                book_yes = books_yes.get(mid)
                book_no = books_no.get(mid)
                raw_trades, volume_1s, trades_1s = trade_tracker.flush(mid)

                market_row, ob_rows, trade_rows = process_one_market(
                    m, book_yes, book_no, raw_trades, volume_1s, trades_1s,
                    ts_utc, ingest_ts, depth_cents,
                )
                all_market_rows.append(market_row)
                all_ob_rows.extend(ob_rows)
                all_trade_rows.extend(trade_rows)

            # Batch insert
            for row in all_market_rows:
                conn.execute("""
                    INSERT OR REPLACE INTO pm_sports_market_1s
                    (ts_utc, market_id, volume_1s, trades_1s,
                     best_bid_p, best_bid_sz, best_ask_p, best_ask_sz,
                     depth_bid_1c, depth_ask_1c, depth_bid_5c, depth_ask_5c,
                     depth_bid_10c, depth_ask_10c, depth_bid_15c, depth_ask_15c,
                     spread,
                     best_bid_p_no, best_bid_sz_no, best_ask_p_no, best_ask_sz_no,
                     depth_bid_1c_no, depth_ask_1c_no, depth_bid_5c_no, depth_ask_5c_no,
                     depth_bid_10c_no, depth_ask_10c_no, depth_bid_15c_no, depth_ask_15c_no,
                     spread_no,
                     ingest_ts_utc)
                    VALUES
                    (:ts_utc, :market_id, :volume_1s, :trades_1s,
                     :best_bid_p, :best_bid_sz, :best_ask_p, :best_ask_sz,
                     :depth_bid_1c, :depth_ask_1c, :depth_bid_5c, :depth_ask_5c,
                     :depth_bid_10c, :depth_ask_10c, :depth_bid_15c, :depth_ask_15c,
                     :spread,
                     :best_bid_p_no, :best_bid_sz_no, :best_ask_p_no, :best_ask_sz_no,
                     :depth_bid_1c_no, :depth_ask_1c_no, :depth_bid_5c_no, :depth_ask_5c_no,
                     :depth_bid_10c_no, :depth_ask_10c_no, :depth_bid_15c_no, :depth_ask_15c_no,
                     :spread_no,
                     :ingest_ts_utc)
                """, row)

            if all_ob_rows:
                conn.executemany("""
                    INSERT OR REPLACE INTO pm_orderbook_full_1s
                    (ts_utc, market_id, token_side, side, level, price, size, ingest_ts_utc)
                    VALUES
                    (:ts_utc, :market_id, :token_side, :side, :level, :price, :size,
                     :ingest_ts_utc)
                """, all_ob_rows)

            if all_trade_rows:
                conn.executemany("""
                    INSERT INTO pm_trades_raw
                    (ts_utc, market_id, asset_id, price, size, raw_json, ingest_ts_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, all_trade_rows)

            conn.commit()
            tick += 1

            # Periodic logging (every 10s)
            if tick % 10 == 0:
                active = sum(1 for v in settled.values() if not v)
                ob_count = len(all_ob_rows)
                trade_count = len(all_trade_rows)
                parts = []
                for m in markets_info:
                    mid = m["market_id"]
                    book = books.get(mid)
                    bid_n = ask_n = 0
                    if book:
                        bid_n = len(book.get("bids") or [])
                        ask_n = len(book.get("asks") or [])
                    status = "✓" if settled[mid] else "●"
                    parts.append(f"{status}{bid_n}b/{ask_n}a")
                log.info(
                    "tick=%d (%dm%02ds) | %d/%d active | OB=%d | trades=%d | [%s]",
                    tick, tick // 60, tick % 60,
                    active, len(markets_info),
                    ob_count, trade_count,
                    " | ".join(parts),
                )

            # Settlement check
            if time.time() - last_settle_check > SETTLE_CHECK_INTERVAL:
                last_settle_check = time.time()
                for m in markets_info:
                    mid = m["market_id"]
                    if not settled[mid]:
                        resolved, closed = check_market_resolved(m["condition_id"])
                        if resolved:
                            settled[mid] = True
                            log.info("✓ RESOLVED: %s", m["question"])
                        elif closed:
                            log.info("⏳ CLOSED (not yet resolved): %s", m["question"])

                if all(settled.values()) and post_settle_countdown < 0:
                    log.info("All markets resolved! Recording 60 more seconds...")
                    post_settle_countdown = 60

            # Duration limit check
            if max_duration and tick >= max_duration:
                log.info("Duration limit reached (%ds). Stopping.", max_duration)
                break

            # Post-settlement countdown
            if post_settle_countdown >= 0:
                post_settle_countdown -= 1
                if post_settle_countdown <= 0:
                    log.info("Post-settlement buffer complete. Stopping.")
                    break

    except KeyboardInterrupt:
        log.info("Interrupted by user after %d ticks (%dm%02ds)",
                 tick, tick // 60, tick % 60)
    finally:
        pool.shutdown(wait=False)
        ws_mgr.close()
        end_utc = datetime.now(timezone.utc)
        write_notes(session_dir, markets_info, start_utc, end_utc)

        # Validation
        log.info("─" * 50)
        log.info("VALIDATION")
        for m in markets_info:
            mid = m["market_id"]
            snap_count = conn.execute(
                "SELECT COUNT(*) FROM pm_sports_market_1s WHERE market_id=?",
                (mid,)).fetchone()[0]
            ob_count = conn.execute(
                "SELECT COUNT(*) FROM pm_orderbook_full_1s WHERE market_id=?",
                (mid,)).fetchone()[0]
            trade_count = conn.execute(
                "SELECT COUNT(*) FROM pm_trades_raw WHERE market_id=?",
                (mid,)).fetchone()[0]

            ts_range = conn.execute(
                "SELECT MIN(ts_utc), MAX(ts_utc) FROM pm_sports_market_1s WHERE market_id=?",
                (mid,)).fetchone()

            log.info("  [%s]", m["question"][:50])
            log.info("    snapshots: %d  |  OB rows: %d  |  trades: %d",
                     snap_count, ob_count, trade_count)
            log.info("    range: %s → %s", ts_range[0], ts_range[1])

        conn.close()
        log.info("─" * 50)
        log.info("Done. %d ticks recorded to %s", tick, session_dir)

    return session_dir


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  Entry Point
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Multi-market Polymarket Order Book Monitor")
    parser.add_argument("urls", nargs="+", help="Polymarket event URLs")
    parser.add_argument("-d", "--duration", type=int, default=None,
                        help="Max duration in seconds (default: run until settlement)")
    parser.add_argument("-o", "--output", default=None,
                        help="Output directory (default: data/)")
    args = parser.parse_args()

    monitor(args.urls, output_dir=args.output, max_duration=args.duration)
