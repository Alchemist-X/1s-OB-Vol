"""
Polymarket Order Book & Volume Recorder.

Hybrid approach:
  - WebSocket (market channel) captures last_trade_price events for volume tracking.
  - REST GET /book polls the full order book at a configurable interval.
  - Data is aggregated per interval and written to SQLite.
"""

import json
import os
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


# ── Thread-safe trade buffer ─────────────────────────────────────────────────

trade_lock = threading.Lock()
trade_buffer: list = []


def flush_trades():
    global trade_buffer
    with trade_lock:
        buf = trade_buffer
        trade_buffer = []
    volume = sum(float(t.get("size", 0)) for t in buf)
    return volume, len(buf)


# ── WebSocket ────────────────────────────────────────────────────────────────

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


def _ping_loop(ws):
    while True:
        try:
            ws.send("PING")
        except Exception:
            break
        time.sleep(10)


def start_websocket(token_id_yes, token_id_no):
    def on_open(ws):
        ws.send(json.dumps({"assets_ids": [token_id_yes, token_id_no], "type": "market"}))
        log.info("WSS subscribed to 2 asset_ids")

    url = config.CLOB_WSS_URL + "/ws/market"
    ws = WebSocketApp(url, on_message=_on_message, on_error=_on_error,
                      on_close=_on_close, on_open=on_open)
    threading.Thread(target=ws.run_forever, daemon=True).start()
    threading.Thread(target=_ping_loop, args=(ws,), daemon=True).start()
    return ws


# ── REST ─────────────────────────────────────────────────────────────────────

def fetch_book(token_id: str):
    try:
        resp = requests.get(f"{config.CLOB_REST_URL}/book",
                            params={"token_id": token_id}, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning("REST /book failed: %s", e)
        return None


# ── Parsing helpers ──────────────────────────────────────────────────────────

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
        p, s = float(lv["price"]), float(lv["size"])
        if side == "bid" and p >= best - delta:
            total += s
        elif side == "ask" and p <= best + delta:
            total += s
    return total


# ── Notes ────────────────────────────────────────────────────────────────────

def _parse_json_field(raw):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return [raw] if raw else []
    return []


def write_notes(session_dir, meta, params, start_utc, end_utc, duration):
    question = meta.get("question", "N/A")
    slug = meta.get("slug", "N/A")
    condition_id = meta.get("conditionId", params["market_id"])
    outcomes = _parse_json_field(meta.get("outcomes", "[]"))
    outcome_prices = _parse_json_field(meta.get("outcomePrices", "[]"))
    token_ids = _parse_json_field(meta.get("clobTokenIds", "[]"))
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
- **Sampling**: {params['interval']}s interval

## Description
{description}

## Files
- `ob_data.db` — SQLite database
  - Table `pm_sports_market_1s`: per-second L1 + volume + spread + depth
  - Table `pm_sports_orderbook_top5_1s`: per-second Top-{params['top_levels']} bid/ask levels
"""
    path = os.path.join(session_dir, "notes.md")
    with open(path, "w") as f:
        f.write(notes)
    log.info("Notes written to %s", path)


# ── Validation ───────────────────────────────────────────────────────────────

def validate(conn):
    log.info("── Validation ──")
    cur = conn.execute("SELECT COUNT(*) FROM pm_sports_market_1s")
    count_a = cur.fetchone()[0]
    cur = conn.execute("SELECT MIN(ts_utc), MAX(ts_utc) FROM pm_sports_market_1s")
    ts_min, ts_max = cur.fetchone()
    log.info("Table A: %d rows, range [%s .. %s]", count_a, ts_min, ts_max)

    cur = conn.execute("SELECT COUNT(*) FROM pm_sports_orderbook_top5_1s")
    log.info("Table B: %d rows", cur.fetchone()[0])

    neg = conn.execute("SELECT COUNT(*) FROM pm_sports_market_1s WHERE volume_1s < 0").fetchone()[0]
    if neg:
        log.warning("Found %d rows with negative volume_1s!", neg)
    else:
        log.info("No negative volume_1s values.")

    for side_col, side_name in [("best_bid_p", "bid"), ("best_ask_p", "ask")]:
        mis = conn.execute(f"""
            SELECT COUNT(*) FROM pm_sports_market_1s a
            JOIN pm_sports_orderbook_top5_1s b
              ON a.market_id = b.market_id AND a.ts_utc = b.ts_utc
            WHERE b.side = '{side_name}' AND b.level = 1
              AND a.{side_col} IS NOT NULL AND b.price IS NOT NULL
              AND ABS(a.{side_col} - b.price) > 0.0001
        """).fetchone()[0]
        if mis:
            log.warning("L1 %s mismatch: %d rows", side_name, mis)
        else:
            log.info("L1 %s consistency check passed.", side_name)


# ── Main recording loop ─────────────────────────────────────────────────────

def record(params: dict):
    """
    params keys:
      slug, condition_id, market_id, token_id_yes, token_id_no, meta,
      duration, interval, top_levels, output_dir
    """
    slug = params["slug"]
    market_id = params["market_id"]
    token_yes = params["token_id_yes"]
    token_no = params["token_id_no"]
    meta = params["meta"]
    duration = params["duration"]
    interval = params["interval"]
    top_levels = params["top_levels"]
    output_dir = params["output_dir"]
    depth_cents = config.DEPTH_CENTS

    start_utc = datetime.now(timezone.utc)
    question = meta.get("question", market_id)
    log.info("Market: %s", question)

    ts_tag = start_utc.strftime("%Y%m%dT%H%M%S")
    session_dir = os.path.join(output_dir, f"{slug}_{ts_tag}")
    os.makedirs(session_dir, exist_ok=True)
    db_path = os.path.join(session_dir, "ob_data.db")
    log.info("Session dir: %s", session_dir)

    conn = get_conn(db_path)
    init_tables(conn)

    ws = start_websocket(token_yes, token_no)
    time.sleep(2)

    total_ticks = int(duration / interval)
    log.info("Recording %ds (%d snapshots at %.1fs interval)...", duration, total_ticks, interval)

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

            book_yes = fetch_book(token_yes)
            book_no = fetch_book(token_no)
            volume_1s, trades_1s = flush_trades()

            # --- Parse YES book ---
            best_bid_p = best_bid_sz = best_ask_p = best_ask_sz = None
            spread = None
            depths = {f"depth_{s}_{c}c": 0.0 for s in ("bid", "ask") for c in depth_cents}
            ob_rows = []

            if book_yes:
                bids_raw = book_yes.get("bids") or []
                asks_raw = book_yes.get("asks") or []
                top_bids = parse_levels(bids_raw, "bid", top_levels)
                top_asks = parse_levels(asks_raw, "ask", top_levels)

                if top_bids:
                    best_bid_p, best_bid_sz = top_bids[0]["price"], top_bids[0]["size"]
                if top_asks:
                    best_ask_p, best_ask_sz = top_asks[0]["price"], top_asks[0]["size"]
                if best_bid_p is not None and best_ask_p is not None:
                    spread = round(best_ask_p - best_bid_p, 4)

                for c in depth_cents:
                    if best_bid_p is not None:
                        depths[f"depth_bid_{c}c"] = round(compute_depth(bids_raw, best_bid_p, "bid", c), 6)
                    if best_ask_p is not None:
                        depths[f"depth_ask_{c}c"] = round(compute_depth(asks_raw, best_ask_p, "ask", c), 6)

                for side_name, top_list in [("bid", top_bids), ("ask", top_asks)]:
                    for i, lv in enumerate(top_list, 1):
                        ob_rows.append(dict(ts_utc=ts_utc, market_id=market_id,
                                            token_side="yes", side=side_name, level=i,
                                            price=lv["price"], size=lv["size"],
                                            ingest_ts_utc=ingest_ts))
                    for i in range(len(top_list) + 1, top_levels + 1):
                        ob_rows.append(dict(ts_utc=ts_utc, market_id=market_id,
                                            token_side="yes", side=side_name, level=i,
                                            price=None, size=None,
                                            ingest_ts_utc=ingest_ts))

            # --- Parse NO book ---
            best_bid_p_no = best_bid_sz_no = best_ask_p_no = best_ask_sz_no = None
            spread_no = None
            depths_no = {f"depth_{s}_{c}c_no": 0.0 for s in ("bid", "ask") for c in depth_cents}

            if book_no:
                bids_raw_no = book_no.get("bids") or []
                asks_raw_no = book_no.get("asks") or []
                top_bids_no = parse_levels(bids_raw_no, "bid", top_levels)
                top_asks_no = parse_levels(asks_raw_no, "ask", top_levels)

                if top_bids_no:
                    best_bid_p_no, best_bid_sz_no = top_bids_no[0]["price"], top_bids_no[0]["size"]
                if top_asks_no:
                    best_ask_p_no, best_ask_sz_no = top_asks_no[0]["price"], top_asks_no[0]["size"]
                if best_bid_p_no is not None and best_ask_p_no is not None:
                    spread_no = round(best_ask_p_no - best_bid_p_no, 4)

                for c in depth_cents:
                    if best_bid_p_no is not None:
                        depths_no[f"depth_bid_{c}c_no"] = round(compute_depth(bids_raw_no, best_bid_p_no, "bid", c), 6)
                    if best_ask_p_no is not None:
                        depths_no[f"depth_ask_{c}c_no"] = round(compute_depth(asks_raw_no, best_ask_p_no, "ask", c), 6)

                for side_name, top_list in [("bid", top_bids_no), ("ask", top_asks_no)]:
                    for i, lv in enumerate(top_list, 1):
                        ob_rows.append(dict(ts_utc=ts_utc, market_id=market_id,
                                            token_side="no", side=side_name, level=i,
                                            price=lv["price"], size=lv["size"],
                                            ingest_ts_utc=ingest_ts))
                    for i in range(len(top_list) + 1, top_levels + 1):
                        ob_rows.append(dict(ts_utc=ts_utc, market_id=market_id,
                                            token_side="no", side=side_name, level=i,
                                            price=None, size=None,
                                            ingest_ts_utc=ingest_ts))

            market_row = dict(
                ts_utc=ts_utc, market_id=market_id,
                volume_1s=round(volume_1s, 6), trades_1s=trades_1s,
                best_bid_p=best_bid_p, best_bid_sz=best_bid_sz,
                best_ask_p=best_ask_p, best_ask_sz=best_ask_sz,
                spread=spread,
                best_bid_p_no=best_bid_p_no, best_bid_sz_no=best_bid_sz_no,
                best_ask_p_no=best_ask_p_no, best_ask_sz_no=best_ask_sz_no,
                spread_no=spread_no,
                ingest_ts_utc=ingest_ts,
                **{k: v for k, v in depths.items()},
                **{k: v for k, v in depths_no.items()},
            )

            insert_market_1s(conn, market_row)
            if ob_rows:
                insert_orderbook_levels(conn, ob_rows)
            conn.commit()

            tick += 1
            if tick % max(1, int(60 / interval)) == 0:
                elapsed = tick * interval
                log.info("Progress: %d/%d snapshots (%.0fs / %.0fs, %.0f%%)",
                         tick, total_ticks, elapsed, duration, tick / total_ticks * 100)

    except KeyboardInterrupt:
        log.info("Interrupted by user after %d snapshots", tick)
    finally:
        ws.close()
        end_utc = datetime.now(timezone.utc)
        actual_seconds = int((end_utc - start_utc).total_seconds())
        write_notes(session_dir, meta, params, start_utc, end_utc, actual_seconds)
        validate(conn)
        conn.close()
        log.info("Done. %d snapshots recorded to %s", tick, session_dir)

    return session_dir
