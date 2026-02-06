#!/usr/bin/env python3
"""Export a recording session's SQLite data to replay.json for the HTML player."""

import json
import os
import sqlite3
import sys
import re


def parse_notes(notes_path):
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

    outcomes = re.findall(r"^\s+-\s+(.+?):\s+price=([\d.?]+)", text, re.MULTILINE)
    if outcomes:
        meta["outcomes"] = [o[0] for o in outcomes]
        meta["outcome_start_prices"] = [o[1] for o in outcomes]

    return meta


def export(session_dir):
    db_path = os.path.join(session_dir, "ob_data.db")
    notes_path = os.path.join(session_dir, "notes.md")

    if not os.path.exists(db_path):
        print(f"ERROR: {db_path} not found")
        sys.exit(1)

    meta = parse_notes(notes_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    timestamps = [r[0] for r in conn.execute(
        "SELECT DISTINCT ts_utc FROM pm_sports_market_1s ORDER BY ts_utc")]

    frames = []
    for ts in timestamps:
        row = conn.execute(
            "SELECT * FROM pm_sports_market_1s WHERE ts_utc = ?", (ts,)).fetchone()

        bids = []
        asks = []
        for ob in conn.execute(
            """SELECT side, level, price, size FROM pm_sports_orderbook_top5_1s
               WHERE ts_utc = ? AND market_id = ? ORDER BY side, level""",
            (ts, row["market_id"])):
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
        frames.append(frame)

    conn.close()

    out = {"meta": meta, "frames": frames}
    out_path = os.path.join(session_dir, "replay.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)

    print(f"Exported {len(frames)} frames to {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 export_json.py <session_dir>")
        print("Example: python3 export_json.py data/lol-tsw-mvk-2026-02-06_20260206T061617")
        sys.exit(1)
    export(sys.argv[1])
