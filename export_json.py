#!/usr/bin/env python3
"""Export a recording session's SQLite data to replay.json for the HTML player.

Supports three DB schemas produced by different recording scripts:
  - recorder.py  → pm_sports_orderbook_top5_1s
  - monitor.py   → pm_orderbook_full_1s
  - auto_monitor.py → pm_snapshot + pm_ob_level

Each schema now exports both YES and NO token order book data (when available).
"""

import json
import os
import sqlite3
import sys
import re
from datetime import datetime, timezone


# ── Notes parser ─────────────────────────────────────────────────────────────

def parse_notes(notes_path):
    """Parse notes.md for metadata, compatible with all three session formats."""
    meta = {}
    if not os.path.exists(notes_path):
        return meta
    with open(notes_path) as f:
        text = f.read()

    m = re.search(r"\*\*Question\*\*:\s*(.+)", text)
    if m:
        meta["question"] = m.group(1).strip()

    m = re.search(r"\*\*Slug\*\*:\s*(.+)", text)
    if m:
        meta["slug"] = m.group(1).strip()

    m = re.search(r"\*\*Sport Type\*\*:\s*(.+)", text)
    if m:
        meta["sport_type"] = m.group(1).strip()

    m = re.search(r"\*\*Sampling\*\*:\s*([\d.]+)s", text)
    if m:
        meta["poll_interval"] = float(m.group(1))

    m = re.search(r"\*\*Concurrency\*\*:\s*(\d+)", text)
    if m:
        meta["concurrency"] = int(m.group(1))

    m = re.search(r"\*\*Duration\*\*:\s*(\d+)s", text)
    if m:
        meta["duration"] = int(m.group(1))

    outcomes = re.findall(r"^\s+-\s+(.+?):\s+price=([\d.?]+)", text, re.MULTILINE)
    if outcomes:
        meta["outcomes"] = [o[0] for o in outcomes]
        meta["outcome_start_prices"] = [o[1] for o in outcomes]

    return meta


# ── Market metadata from DB ──────────────────────────────────────────────────

def _extract_market_meta(conn):
    """Extract rich metadata from pm_market_meta table if it exists."""
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if "pm_market_meta" not in tables:
        return {}

    row = conn.execute("SELECT * FROM pm_market_meta LIMIT 1").fetchone()
    if not row:
        return {}

    extra = {}
    meta_json_str = row["meta_json"] if "meta_json" in row.keys() else None
    if meta_json_str:
        try:
            gm = json.loads(meta_json_str)
            # Description (settlement rules)
            desc = gm.get("description", "")
            if desc:
                extra["description"] = desc

            # Outcomes
            outcomes = gm.get("outcomes")
            if outcomes:
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except (json.JSONDecodeError, TypeError):
                        outcomes = [outcomes]
                extra["outcomes"] = outcomes

            # Outcome prices
            oprices = gm.get("outcomePrices")
            if oprices:
                if isinstance(oprices, str):
                    try:
                        oprices = json.loads(oprices)
                    except (json.JSONDecodeError, TypeError):
                        oprices = [oprices]
                extra["outcome_start_prices"] = oprices

            # Game start / end
            for key in ("gameStartTime", "endDate"):
                val = gm.get(key)
                if val:
                    extra["game_start"] = val
                    break

            # Volume, liquidity
            vol = gm.get("volumeNum") or gm.get("volume")
            if vol is not None:
                try:
                    extra["volume"] = float(vol)
                except (ValueError, TypeError):
                    pass

            liq = gm.get("liquidityNum") or gm.get("liquidity")
            if liq is not None:
                try:
                    extra["liquidity"] = float(liq)
                except (ValueError, TypeError):
                    pass

            # Token IDs
            tokens = gm.get("clobTokenIds")
            if tokens:
                if isinstance(tokens, str):
                    try:
                        tokens = json.loads(tokens)
                    except (json.JSONDecodeError, TypeError):
                        tokens = []
                if len(tokens) >= 2:
                    extra["token_id_yes"] = tokens[0]
                    extra["token_id_no"] = tokens[1]

        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: direct columns
    for col_key, meta_key in [("question", "question"), ("slug", "slug")]:
        if meta_key not in extra and col_key in row.keys():
            val = row[col_key]
            if val:
                extra[meta_key] = val

    # Token IDs from direct columns (various column names across schemas)
    if "token_id_yes" not in extra:
        for col in ("token_id_yes", "token_yes"):
            if col in row.keys() and row[col]:
                extra["token_id_yes"] = row[col]
                break
    if "token_id_no" not in extra:
        for col in ("token_id_no", "token_no"):
            if col in row.keys() and row[col]:
                extra["token_id_no"] = row[col]
                break

    return extra


# ── Schema detection ─────────────────────────────────────────────────────────

def detect_schema(conn):
    """Detect which recording script produced this database.

    Returns: "recorder", "monitor", or "auto"
    """
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}

    if "pm_snapshot" in tables and "pm_ob_level" in tables:
        return "auto"
    if "pm_orderbook_full_1s" in tables:
        return "monitor"
    if "pm_sports_orderbook_top5_1s" in tables:
        return "recorder"

    # Fallback: if pm_sports_market_1s exists, treat as monitor (no OB table)
    if "pm_sports_market_1s" in tables:
        return "monitor"

    raise ValueError(
        f"Unrecognised DB schema. Tables found: {sorted(tables)}")


# ── Helper: check if column exists ───────────────────────────────────────────

def _has_column(conn, table, column):
    """Check if a column exists in a table."""
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    return column in cols


# ── Helper: compute depth buckets from full bid/ask lists ────────────────────

DEPTH_CENTS = [1, 5, 10, 15]


def _compute_depths(bids, asks, best_bid_p, best_ask_p, suffix=""):
    """Compute depth_bid_Nc and depth_ask_Nc from full level lists."""
    depths = {}
    for nc in DEPTH_CENTS:
        threshold = nc / 100.0
        bid_depth = sum(
            b["size"] for b in bids
            if best_bid_p is not None and b["price"] is not None
            and b["price"] >= best_bid_p - threshold
        )
        ask_depth = sum(
            a["size"] for a in asks
            if best_ask_p is not None and a["price"] is not None
            and a["price"] <= best_ask_p + threshold
        )
        depths[f"depth_bid_{nc}c{suffix}"] = round(bid_depth, 2)
        depths[f"depth_ask_{nc}c{suffix}"] = round(ask_depth, 2)
    return depths


# ── Export: recorder schema ──────────────────────────────────────────────────

def _export_recorder(conn):
    """Export from recorder.py schema (pm_sports_orderbook_top5_1s)."""
    timestamps = [r[0] for r in conn.execute(
        "SELECT DISTINCT ts_utc FROM pm_sports_market_1s ORDER BY ts_utc")]

    has_token_side = _has_column(conn, "pm_sports_orderbook_top5_1s", "token_side")
    has_no_cols = _has_column(conn, "pm_sports_market_1s", "best_bid_p_no")

    frames = []
    for ts in timestamps:
        row = conn.execute(
            "SELECT * FROM pm_sports_market_1s WHERE ts_utc = ?",
            (ts,)).fetchone()

        # YES book
        bids, asks = [], []
        if has_token_side:
            query = """SELECT side, level, price, size
                       FROM pm_sports_orderbook_top5_1s
                       WHERE ts_utc = ? AND market_id = ? AND token_side = 'yes'
                       ORDER BY side, level"""
        else:
            query = """SELECT side, level, price, size
                       FROM pm_sports_orderbook_top5_1s
                       WHERE ts_utc = ? AND market_id = ?
                       ORDER BY side, level"""
        for ob in conn.execute(query, (ts, row["market_id"])):
            entry = {"price": ob["price"], "size": ob["size"]}
            if ob["side"] == "bid":
                bids.append(entry)
            else:
                asks.append(entry)

        frame = {
            "ts": ts,
            "best_bid_p": row["best_bid_p"],
            "best_bid_sz": row["best_bid_sz"],
            "best_ask_p": row["best_ask_p"],
            "best_ask_sz": row["best_ask_sz"],
            "spread": row["spread"],
            "volume": row["volume_1s"],
            "trades": row["trades_1s"],
            "depth_bid_1c": row["depth_bid_1c"],
            "depth_ask_1c": row["depth_ask_1c"],
            "depth_bid_5c": row["depth_bid_5c"],
            "depth_ask_5c": row["depth_ask_5c"],
            "depth_bid_10c": row["depth_bid_10c"],
            "depth_ask_10c": row["depth_ask_10c"],
            "depth_bid_15c": row["depth_bid_15c"],
            "depth_ask_15c": row["depth_ask_15c"],
            "bids": bids,
            "asks": asks,
        }

        # NO book (if available)
        if has_token_side:
            bids_no, asks_no = [], []
            for ob in conn.execute(
                """SELECT side, level, price, size
                   FROM pm_sports_orderbook_top5_1s
                   WHERE ts_utc = ? AND market_id = ? AND token_side = 'no'
                   ORDER BY side, level""",
                    (ts, row["market_id"])):
                entry = {"price": ob["price"], "size": ob["size"]}
                if ob["side"] == "bid":
                    bids_no.append(entry)
                else:
                    asks_no.append(entry)
            frame["bids_no"] = bids_no
            frame["asks_no"] = asks_no

        if has_no_cols:
            frame["best_bid_p_no"] = row["best_bid_p_no"]
            frame["best_bid_sz_no"] = row["best_bid_sz_no"]
            frame["best_ask_p_no"] = row["best_ask_p_no"]
            frame["best_ask_sz_no"] = row["best_ask_sz_no"]
            frame["spread_no"] = row["spread_no"]
            for nc in DEPTH_CENTS:
                frame[f"depth_bid_{nc}c_no"] = row[f"depth_bid_{nc}c_no"]
                frame[f"depth_ask_{nc}c_no"] = row[f"depth_ask_{nc}c_no"]

        frames.append(frame)
    return frames


# ── Export: monitor schema ───────────────────────────────────────────────────

def _export_monitor(conn):
    """Export from monitor.py schema (pm_orderbook_full_1s)."""
    timestamps = [r[0] for r in conn.execute(
        "SELECT DISTINCT ts_utc FROM pm_sports_market_1s ORDER BY ts_utc")]

    has_token_side = _has_column(conn, "pm_orderbook_full_1s", "token_side")
    has_no_cols = _has_column(conn, "pm_sports_market_1s", "best_bid_p_no")

    frames = []
    for ts in timestamps:
        row = conn.execute(
            "SELECT * FROM pm_sports_market_1s WHERE ts_utc = ?",
            (ts,)).fetchone()

        # YES book
        bids, asks = [], []
        if has_token_side:
            query = """SELECT side, level, price, size
                       FROM pm_orderbook_full_1s
                       WHERE ts_utc = ? AND market_id = ? AND token_side = 'yes'
                       ORDER BY side, level"""
        else:
            query = """SELECT side, level, price, size
                       FROM pm_orderbook_full_1s
                       WHERE ts_utc = ? AND market_id = ?
                       ORDER BY side, level"""
        for ob in conn.execute(query, (ts, row["market_id"])):
            entry = {"price": ob["price"], "size": ob["size"]}
            if ob["side"] == "bid":
                bids.append(entry)
            else:
                asks.append(entry)

        frame = {
            "ts": ts,
            "best_bid_p": row["best_bid_p"],
            "best_bid_sz": row["best_bid_sz"],
            "best_ask_p": row["best_ask_p"],
            "best_ask_sz": row["best_ask_sz"],
            "spread": row["spread"],
            "volume": row["volume_1s"],
            "trades": row["trades_1s"],
            "depth_bid_1c": row["depth_bid_1c"],
            "depth_ask_1c": row["depth_ask_1c"],
            "depth_bid_5c": row["depth_bid_5c"],
            "depth_ask_5c": row["depth_ask_5c"],
            "depth_bid_10c": row["depth_bid_10c"],
            "depth_ask_10c": row["depth_ask_10c"],
            "depth_bid_15c": row["depth_bid_15c"],
            "depth_ask_15c": row["depth_ask_15c"],
            "bids": bids,
            "asks": asks,
        }

        # NO book (if available)
        if has_token_side:
            bids_no, asks_no = [], []
            for ob in conn.execute(
                """SELECT side, level, price, size
                   FROM pm_orderbook_full_1s
                   WHERE ts_utc = ? AND market_id = ? AND token_side = 'no'
                   ORDER BY side, level""",
                    (ts, row["market_id"])):
                entry = {"price": ob["price"], "size": ob["size"]}
                if ob["side"] == "bid":
                    bids_no.append(entry)
                else:
                    asks_no.append(entry)
            frame["bids_no"] = bids_no
            frame["asks_no"] = asks_no

        if has_no_cols:
            frame["best_bid_p_no"] = row["best_bid_p_no"]
            frame["best_bid_sz_no"] = row["best_bid_sz_no"]
            frame["best_ask_p_no"] = row["best_ask_p_no"]
            frame["best_ask_sz_no"] = row["best_ask_sz_no"]
            frame["spread_no"] = row["spread_no"]
            for nc in DEPTH_CENTS:
                frame[f"depth_bid_{nc}c_no"] = row[f"depth_bid_{nc}c_no"]
                frame[f"depth_ask_{nc}c_no"] = row[f"depth_ask_{nc}c_no"]

        frames.append(frame)
    return frames


# ── Export: auto_monitor schema ──────────────────────────────────────────────

def _export_auto(conn):
    """Export from auto_monitor.py schema (pm_snapshot + pm_ob_level).

    Downsamples to 1 frame/s by taking the first snapshot per second.
    If token_side column exists, exports both YES and NO books.
    """
    has_token_side = _has_column(conn, "pm_snapshot", "token_side")

    if has_token_side:
        # Pick one representative snap_id per second per token_side
        rep_yes = conn.execute("""
            SELECT MIN(snap_id) AS snap_id, ts_ms / 1000 AS ts_sec
            FROM pm_snapshot WHERE token_side = 'yes'
            GROUP BY ts_sec ORDER BY ts_sec
        """).fetchall()

        rep_no = conn.execute("""
            SELECT MIN(snap_id) AS snap_id, ts_ms / 1000 AS ts_sec
            FROM pm_snapshot WHERE token_side = 'no'
            GROUP BY ts_sec ORDER BY ts_sec
        """).fetchall()

        no_by_sec = {ts_sec: snap_id for snap_id, ts_sec in rep_no}

        frames = []
        for snap_id_yes, ts_sec in rep_yes:
            snap = conn.execute(
                "SELECT * FROM pm_snapshot WHERE snap_id = ?",
                (snap_id_yes,)).fetchone()
            if snap is None:
                continue

            ts_ms = snap["ts_ms"]
            ts_str = datetime.fromtimestamp(
                ts_ms / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%SZ")

            # YES book
            bids_yes, asks_yes = [], []
            for lv in conn.execute(
                "SELECT side, price, size FROM pm_ob_level WHERE snap_id = ? ORDER BY side, level",
                    (snap_id_yes,)):
                entry = {"price": lv["price"], "size": lv["size"]}
                if lv["side"] == "bid":
                    bids_yes.append(entry)
                else:
                    asks_yes.append(entry)

            depths_yes = _compute_depths(
                bids_yes, asks_yes, snap["best_bid_p"], snap["best_ask_p"])

            # NO book (same second)
            bids_no, asks_no = [], []
            snap_no = None
            snap_id_no = no_by_sec.get(ts_sec)
            if snap_id_no:
                snap_no = conn.execute(
                    "SELECT * FROM pm_snapshot WHERE snap_id = ?",
                    (snap_id_no,)).fetchone()
                for lv in conn.execute(
                    "SELECT side, price, size FROM pm_ob_level WHERE snap_id = ? ORDER BY side, level",
                        (snap_id_no,)):
                    entry = {"price": lv["price"], "size": lv["size"]}
                    if lv["side"] == "bid":
                        bids_no.append(entry)
                    else:
                        asks_no.append(entry)

            depths_no = {}
            if snap_no:
                depths_no = _compute_depths(
                    bids_no, asks_no,
                    snap_no["best_bid_p"], snap_no["best_ask_p"],
                    suffix="_no")

            # Trade volume/count in this second
            ts_ms_lo = ts_sec * 1000
            ts_ms_hi = ts_ms_lo + 999
            trade_row = conn.execute(
                """SELECT COALESCE(SUM(size), 0) AS vol, COUNT(*) AS cnt
                   FROM pm_trades_raw WHERE ts_ms BETWEEN ? AND ?""",
                (ts_ms_lo, ts_ms_hi)).fetchone()

            frame = {
                "ts": ts_str,
                "best_bid_p": snap["best_bid_p"],
                "best_bid_sz": snap["best_bid_sz"],
                "best_ask_p": snap["best_ask_p"],
                "best_ask_sz": snap["best_ask_sz"],
                "spread": snap["spread"],
                "volume": trade_row["vol"] if trade_row else 0,
                "trades": trade_row["cnt"] if trade_row else 0,
                **depths_yes,
                "bids": bids_yes,
                "asks": asks_yes,
                "bids_no": bids_no,
                "asks_no": asks_no,
            }

            if snap_no:
                frame["best_bid_p_no"] = snap_no["best_bid_p"]
                frame["best_bid_sz_no"] = snap_no["best_bid_sz"]
                frame["best_ask_p_no"] = snap_no["best_ask_p"]
                frame["best_ask_sz_no"] = snap_no["best_ask_sz"]
                frame["spread_no"] = snap_no["spread"]
                frame.update(depths_no)

            frames.append(frame)
        return frames

    else:
        # Legacy: no token_side column, YES-only export
        rep_snaps = conn.execute("""
            SELECT MIN(snap_id) AS snap_id, ts_ms / 1000 AS ts_sec
            FROM pm_snapshot
            GROUP BY ts_sec
            ORDER BY ts_sec
        """).fetchall()

        if not rep_snaps:
            return []

        frames = []
        for snap_id, ts_sec in rep_snaps:
            snap = conn.execute(
                "SELECT * FROM pm_snapshot WHERE snap_id = ?",
                (snap_id,)).fetchone()
            if snap is None:
                continue

            ts_ms = snap["ts_ms"]
            ts_str = datetime.fromtimestamp(
                ts_ms / 1000.0, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%SZ")

            best_bid_p = snap["best_bid_p"]
            best_ask_p = snap["best_ask_p"]

            bids, asks = [], []
            for lv in conn.execute(
                "SELECT side, price, size FROM pm_ob_level WHERE snap_id = ? ORDER BY side, level",
                    (snap_id,)):
                entry = {"price": lv["price"], "size": lv["size"]}
                if lv["side"] == "bid":
                    bids.append(entry)
                else:
                    asks.append(entry)

            depths = _compute_depths(bids, asks, best_bid_p, best_ask_p)

            ts_ms_lo = ts_sec * 1000
            ts_ms_hi = ts_ms_lo + 999
            trade_row = conn.execute(
                """SELECT COALESCE(SUM(size), 0) AS vol, COUNT(*) AS cnt
                   FROM pm_trades_raw WHERE ts_ms BETWEEN ? AND ?""",
                (ts_ms_lo, ts_ms_hi)).fetchone()

            frame = {
                "ts": ts_str,
                "best_bid_p": best_bid_p,
                "best_bid_sz": snap["best_bid_sz"],
                "best_ask_p": best_ask_p,
                "best_ask_sz": snap["best_ask_sz"],
                "spread": snap["spread"],
                "volume": trade_row["vol"] if trade_row else 0,
                "trades": trade_row["cnt"] if trade_row else 0,
                **depths,
                "bids": bids,
                "asks": asks,
            }
            frames.append(frame)
        return frames


# ── Unified entry point ──────────────────────────────────────────────────────

def export(session_dir):
    db_path = os.path.join(session_dir, "ob_data.db")
    notes_path = os.path.join(session_dir, "notes.md")

    if not os.path.exists(db_path):
        print(f"ERROR: {db_path} not found")
        sys.exit(1)

    meta = parse_notes(notes_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    schema = detect_schema(conn)
    meta["schema"] = schema
    print(f"Detected schema: {schema}")

    # Extract rich metadata from pm_market_meta table
    db_meta = _extract_market_meta(conn)
    # DB meta fills in gaps; notes meta takes precedence
    for k, v in db_meta.items():
        if k not in meta:
            meta[k] = v

    if schema == "recorder":
        frames = _export_recorder(conn)
    elif schema == "monitor":
        frames = _export_monitor(conn)
    elif schema == "auto":
        frames = _export_auto(conn)
    else:
        raise ValueError(f"Unknown schema: {schema}")

    conn.close()

    out = {"meta": meta, "frames": frames}
    out_path = os.path.join(session_dir, "replay.json")
    with open(out_path, "w") as f:
        json.dump(out, f)

    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"Exported {len(frames)} frames to {out_path} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 export_json.py <session_dir>")
        print("Example: python3 export_json.py data/btc-updown-5m-1770965100_20260213T063946")
        sys.exit(1)
    export(sys.argv[1])
