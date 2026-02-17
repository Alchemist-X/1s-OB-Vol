#!/usr/bin/env python3
"""
Polymarket Order Book Recorder — CLI entry point.

Usage:
  python3 main.py record <url> [options]
  python3 main.py export <session_dir>
"""

import argparse
import logging
import sys

import config


def cmd_record(args):
    from resolve import resolve_market
    from recorder import record

    market = resolve_market(args.url)

    params = {
        "slug":         market["slug"],
        "condition_id": market["condition_id"],
        "market_id":    market["market_id"],
        "token_id_yes": market["token_id_yes"],
        "token_id_no":  market["token_id_no"],
        "meta":         market["meta"],
        "duration":     args.duration,
        "interval":     args.interval,
        "top_levels":   args.top,
        "output_dir":   args.output,
    }

    session_dir = record(params)

    if args.export:
        from export_json import export
        export(session_dir)


def cmd_export(args):
    from export_json import export
    export(args.session_dir)


def main():
    parser = argparse.ArgumentParser(
        prog="pm-ob",
        description="Polymarket Order Book Recorder & Exporter",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── record ───────────────────────────────────────────────────────────
    rec = sub.add_parser("record", help="Record order book data from a Polymarket event")
    rec.add_argument("url", help="Polymarket event URL (e.g. https://polymarket.com/event/...)")
    rec.add_argument("-d", "--duration", type=int, default=config.DEFAULT_DURATION,
                     help=f"Recording duration in seconds (default: {config.DEFAULT_DURATION})")
    rec.add_argument("-i", "--interval", type=float, default=config.DEFAULT_INTERVAL,
                     help=f"Polling interval in seconds (default: {config.DEFAULT_INTERVAL})")
    rec.add_argument("-t", "--top", type=int, default=config.DEFAULT_TOP_LEVELS,
                     help=f"Number of top order book levels (default: {config.DEFAULT_TOP_LEVELS})")
    rec.add_argument("-o", "--output", default=config.DEFAULT_OUTPUT,
                     help=f"Output data directory (default: {config.DEFAULT_OUTPUT})")
    rec.add_argument("-e", "--export", action="store_true",
                     help="Auto-export replay.json after recording")
    rec.set_defaults(func=cmd_record)

    # ── export ───────────────────────────────────────────────────────────
    exp = sub.add_parser("export", help="Export a recorded session to replay.json")
    exp.add_argument("session_dir", help="Path to a recording session directory")
    exp.set_defaults(func=cmd_export)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args.func(args)


if __name__ == "__main__":
    main()
