#!/usr/bin/env python3
"""
Polymarket 1-second Order Book & Volume Recorder.

Hybrid approach:
  - WebSocket (market channel) captures every last_trade_price event for volume tracking.
  - REST GET /book polls the full order book once per second.
  - Data is aggregated per second and written to SQLite.

Output structure:
  data/
    {market_slug}_{YYYYMMDDTHHmmss}/
      notes.md          <- market metadata, outcomes, recording window
      ob_data.db        <- SQLite with two tables
"""

import json
import os
import re
import threading
import time
import logging
from datetime import datetime, timezone

import requests
from websocket import WebSocketApp

import config
from db import get_conn, init_tables, insert_market_1s, insert_orderbook_levels

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Market metadata ──────────────────────────────────────────────────────────

def fetch_market_meta() -> dict:
    url = f"{config.GAMMA_API_URL}/markets"
    try:
        resp = requests.get(url, params={"clob_token_ids": config.TOKEN_ID_YES}, timeout=10)
        resp.raise_for_status()
        markets = resp.json()
        if markets and len(markets) > 0:
            return markets[0]
    except Exception as e:
        log.warning("Failed to fetch market metadata: %s", e)
    return {}


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def build_session_dir(start_utc: datetime) -> str:
    ts_tag = start_utc.strftime("%Y%m%dT%H%M%S")
    folder_name = f"{config.EVENT_SLUG}_{ts_tag}"
    session_dir = os.path.join(config.DATA_ROOT, folder_name)
    os.makedirs(session_dir, exist_ok=True)
    return session_dir


def write_notes(session_dir: str, meta: dict, start_utc: datetime, end_utc: datetime, duration: int):
    question = meta.get("question", "N/A")
    slug = meta.get("slug", "N/A")
    condition_id = meta.get("conditionId", config.CONDITION_ID)
    outcomes_raw = meta.get("outcomes", "[]")
    if isinstance(outcomes_raw, str):
        try:
            outcomes = json.loads(outcomes_raw)
        except json.JSONDecodeError:
            outcomes = [outcomes_raw]
    else:
        outcomes = outcomes_raw
    outcome_prices_raw = meta.get("outcomePrices", "[]")
    if isinstance(outcome_prices_raw, str):
        try:
            outcome_prices = json.loads(outcome_prices_raw)
        except json.JSONDecodeError:
            outcome_prices = []
    else:
        outcome_prices = outcome_prices_raw

    token_ids_raw = meta.get("clobTokenIds", "[]")
    if isinstance(token_ids_raw, str):
        try:
            token_ids = json.loads(token_ids_raw)
        except json.JSONDecodeError:
            token_ids = []
    else:
        token_ids = token_ids_raw

    description = meta.get("description", "")
    game_start = meta.get("gameStartTime", meta.get("endDate", "N/A"))
    volume_total = meta.get("volumeNum", meta.get("volume", "N/A"))
    liquidity = meta.get("liquidityNum", meta.get("liquidity", "N/A"))
    sport_type = meta.get("sportsMarketType", "N/A")

    outcome_lines = []
    for i, oc in enumerate(outcomes):
        price = outcome_prices[i] if i < len(outcome_prices) else "?"
        tid = token_ids[i] if i < len(token_ids) else "?"
        outcome_lines.append(f"  - {oc}: price={price}, token_id={tid}")

    notes = f"""# Recording Notes

## Market
- **Question**: {question}
- **Slug**: {slug}
- **Condition ID**: {condition_id}
- **Sport Type**: {sport_type}
- **Game Start**: {game_start}

## Outcomes
{chr(10).join(outcome_lines)}

## Market Stats (at recording start)
- **Total Volume**: {volume_total}
- **Liquidity**: {liquidity}

## Recording Window
- **Start (UTC)**: {start_utc.strftime("%Y-%m-%d %H:%M:%S")}
- **End (UTC)**: {end_utc.strftime("%Y-%m-%d %H:%M:%S")}
- **Duration**: {duration} seconds ({duration // 60}m {duration % 60}s)
- **Sampling**: {config.POLL_INTERVAL}s interval

## Description
{description}

## Files
- `ob_data.db` — SQLite database
  - Table `pm_sports_market_1s`: per-second L1 + volume + spread + depth
  - Table `pm_sports_orderbook_top5_1s`: per-second Top5 bid/ask levels
"""
    path = os.path.join(session_dir, "notes.md")
    with open(path, "w") as f:
        f.write(notes)
    log.info("Notes written to %s", path)


# ── Thread-safe trade buffer ─────────────────────────────────────────────────

trade_lock = threading.Lock()
trade_buffer: list[dict] = []


def flush_trades():
    global trade_buffer
    with trade_lock:
        buf = trade_buffer
        trade_buffer = []
    volume = sum(float(t.get("size", 0)) for t in buf)
    return volume, len(buf)


# ── WebSocket: listen for last_trade_price events ────────────────────────────

def _on_message(ws, message):
    if message == "PONG":
        return
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return
    events = data if isinstance(data, list) else [data]
    for evt in events:
        if evt.get("event_type") == "last_trade_price":
            with trade_lock:
                trade_buffer.append(evt)


def _on_error(ws, error):
    log.warning("WSS error: %s", error)


def _on_close(ws, status_code, msg):
    log.info("WSS closed (code=%s msg=%s)", status_code, msg)


def _on_open(ws):
    asset_ids = [config.TOKEN_ID_YES, config.TOKEN_ID_NO]
    ws.send(json.dumps({"assets_ids": asset_ids, "type": "market"}))
    log.info("WSS subscribed to %d asset_ids", len(asset_ids))


def _ping_loop(ws):
    while True:
        try:
            ws.send("PING")
        except Exception:
            break
        time.sleep(10)


def start_websocket():
    url = config.CLOB_WSS_URL + "/ws/market"
    ws = WebSocketApp(
        url,
        on_message=_on_message,
        on_error=_on_error,
        on_close=_on_close,
        on_open=_on_open,
    )
    t = threading.Thread(target=ws.run_forever, daemon=True)
    t.start()
    ping_t = threading.Thread(target=_ping_loop, args=(ws,), daemon=True)
    ping_t.start()
    return ws


# ── REST: fetch order book snapshot ──────────────────────────────────────────

def fetch_book(token_id: str):
    url = f"{config.CLOB_REST_URL}/book"
    try:
        resp = requests.get(url, params={"token_id": token_id}, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning("REST /book failed: %s", e)
        return None


# ── Order book parsing helpers ───────────────────────────────────────────────

def parse_levels(raw, side, top_n=5):
    parsed = [(float(x["price"]), float(x["size"])) for x in raw]
    if side == "bid":
        parsed.sort(key=lambda x: x[0], reverse=True)
    else:
        parsed.sort(key=lambda x: x[0])
    return [{"price": p, "size": s} for p, s in parsed[:top_n]]


def compute_depth(levels_raw, best, side, cents):
    delta = cents / 100.0
    total = 0.0
    for lv in levels_raw:
        p = float(lv["price"])
        s = float(lv["size"])
        if side == "bid" and p >= best - delta:
            total += s
        elif side == "ask" and p <= best + delta:
            total += s
    return total


# ── Main recording loop ─────────────────────────────────────────────────────

def record():
    start_utc = datetime.now(timezone.utc)
    duration = config.RECORD_DURATION_SECONDS

    log.info("Fetching market metadata...")
    meta = fetch_market_meta()
    market_question = meta.get("question", config.CONDITION_ID)
    log.info("Market: %s", market_question)

    session_dir = build_session_dir(start_utc)
    db_path = os.path.join(session_dir, "ob_data.db")
    log.info("Session dir: %s", session_dir)

    conn = get_conn(db_path)
    init_tables(conn)

    ws = start_websocket()
    time.sleep(2)

    interval = config.POLL_INTERVAL
    total_ticks = int(duration / interval)
    log.info("Recording %d seconds (%d snapshots at %.0fs interval)...",
             duration, total_ticks, interval)

    start = time.time()
    tick = 0

    try:
        while tick < total_ticks:
            target = start + (tick + 1) * interval
            sleep_for = target - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

            now_utc = datetime.now(timezone.utc)
            ts_utc = now_utc.strftime("%Y-%m-%d %H:%M:%SZ")
            ingest_ts = now_utc.strftime("%Y-%m-%d %H:%M:%S.%fZ")

            book = fetch_book(config.TOKEN_ID_YES)
            volume_1s, trades_1s = flush_trades()

            best_bid_p = best_bid_sz = best_ask_p = best_ask_sz = None
            spread = None
            depths = {f"depth_{side}_{c}c": 0.0 for side in ("bid", "ask") for c in config.DEPTH_CENTS}
            ob_rows = []

            if book:
                bids_raw = book.get("bids") or []
                asks_raw = book.get("asks") or []

                top_bids = parse_levels(bids_raw, "bid", config.TOP_LEVELS)
                top_asks = parse_levels(asks_raw, "ask", config.TOP_LEVELS)

                if top_bids:
                    best_bid_p = top_bids[0]["price"]
                    best_bid_sz = top_bids[0]["size"]
                if top_asks:
                    best_ask_p = top_asks[0]["price"]
                    best_ask_sz = top_asks[0]["size"]

                if best_bid_p is not None and best_ask_p is not None:
                    spread = round(best_ask_p - best_bid_p, 4)

                for c in config.DEPTH_CENTS:
                    if best_bid_p is not None:
                        depths[f"depth_bid_{c}c"] = round(compute_depth(bids_raw, best_bid_p, "bid", c), 6)
                    if best_ask_p is not None:
                        depths[f"depth_ask_{c}c"] = round(compute_depth(asks_raw, best_ask_p, "ask", c), 6)

                for i, lv in enumerate(top_bids, start=1):
                    ob_rows.append(dict(ts_utc=ts_utc, market_id=config.MARKET_ID,
                                        side="bid", level=i,
                                        price=lv["price"], size=lv["size"],
                                        ingest_ts_utc=ingest_ts))
                for i, lv in enumerate(top_asks, start=1):
                    ob_rows.append(dict(ts_utc=ts_utc, market_id=config.MARKET_ID,
                                        side="ask", level=i,
                                        price=lv["price"], size=lv["size"],
                                        ingest_ts_utc=ingest_ts))

                for side, top, total_levels in [("bid", top_bids, config.TOP_LEVELS),
                                                 ("ask", top_asks, config.TOP_LEVELS)]:
                    for i in range(len(top) + 1, total_levels + 1):
                        ob_rows.append(dict(ts_utc=ts_utc, market_id=config.MARKET_ID,
                                            side=side, level=i,
                                            price=None, size=None,
                                            ingest_ts_utc=ingest_ts))

            market_row = dict(
                ts_utc=ts_utc,
                market_id=config.MARKET_ID,
                volume_1s=round(volume_1s, 6),
                trades_1s=trades_1s,
                best_bid_p=best_bid_p,
                best_bid_sz=best_bid_sz,
                best_ask_p=best_ask_p,
                best_ask_sz=best_ask_sz,
                depth_bid_1c=depths["depth_bid_1c"],
                depth_ask_1c=depths["depth_ask_1c"],
                depth_bid_5c=depths["depth_bid_5c"],
                depth_ask_5c=depths["depth_ask_5c"],
                depth_bid_10c=depths["depth_bid_10c"],
                depth_ask_10c=depths["depth_ask_10c"],
                depth_bid_15c=depths["depth_bid_15c"],
                depth_ask_15c=depths["depth_ask_15c"],
                spread=spread,
                ingest_ts_utc=ingest_ts,
            )

            insert_market_1s(conn, market_row)
            if ob_rows:
                insert_orderbook_levels(conn, ob_rows)
            conn.commit()

            tick += 1
            if tick % 12 == 0:
                elapsed = tick * interval
                log.info("Progress: %d/%d snapshots (%.0fs / %.0fs, %.0f%%)",
                         tick, total_ticks, elapsed, duration,
                         tick / total_ticks * 100)

    except KeyboardInterrupt:
        log.info("Interrupted by user after %d snapshots", tick)
    finally:
        ws.close()
        end_utc = datetime.now(timezone.utc)
        actual_seconds = int((end_utc - start_utc).total_seconds())
        write_notes(session_dir, meta, start_utc, end_utc, actual_seconds)
        validate(conn)
        conn.close()
        log.info("Done. %d snapshots recorded to %s", tick, session_dir)


# ── Post-recording validation ────────────────────────────────────────────────

def validate(conn):
    log.info("── Validation ──")

    cur = conn.execute("SELECT COUNT(*) FROM pm_sports_market_1s")
    count_a = cur.fetchone()[0]
    cur = conn.execute("SELECT MIN(ts_utc), MAX(ts_utc) FROM pm_sports_market_1s")
    ts_min, ts_max = cur.fetchone()
    log.info("Table A: %d rows, range [%s .. %s]", count_a, ts_min, ts_max)

    cur = conn.execute("SELECT COUNT(*) FROM pm_sports_orderbook_top5_1s")
    count_b = cur.fetchone()[0]
    log.info("Table B: %d rows", count_b)

    cur = conn.execute("SELECT COUNT(*) FROM pm_sports_market_1s WHERE volume_1s < 0")
    neg = cur.fetchone()[0]
    if neg > 0:
        log.warning("Found %d rows with negative volume_1s!", neg)
    else:
        log.info("No negative volume_1s values.")

    cur = conn.execute("""
        SELECT COUNT(*) FROM pm_sports_market_1s a
        JOIN pm_sports_orderbook_top5_1s b
          ON a.market_id = b.market_id AND a.ts_utc = b.ts_utc
        WHERE b.side = 'bid' AND b.level = 1
          AND (a.best_bid_p IS NOT NULL AND b.price IS NOT NULL)
          AND ABS(a.best_bid_p - b.price) > 0.0001
    """)
    mismatch = cur.fetchone()[0]
    if mismatch > 0:
        log.warning("L1 bid mismatch between Table A and B: %d rows", mismatch)
    else:
        log.info("L1 bid consistency check passed.")

    cur = conn.execute("""
        SELECT COUNT(*) FROM pm_sports_market_1s a
        JOIN pm_sports_orderbook_top5_1s b
          ON a.market_id = b.market_id AND a.ts_utc = b.ts_utc
        WHERE b.side = 'ask' AND b.level = 1
          AND (a.best_ask_p IS NOT NULL AND b.price IS NOT NULL)
          AND ABS(a.best_ask_p - b.price) > 0.0001
    """)
    mismatch = cur.fetchone()[0]
    if mismatch > 0:
        log.warning("L1 ask mismatch between Table A and B: %d rows", mismatch)
    else:
        log.info("L1 ask consistency check passed.")


if __name__ == "__main__":
    record()
