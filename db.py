import sqlite3
import os


def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_tables(conn: sqlite3.Connection):
    conn.executescript("""
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
        ingest_ts_utc   TEXT    NOT NULL,
        PRIMARY KEY (market_id, ts_utc)
    );

    CREATE TABLE IF NOT EXISTS pm_sports_orderbook_top5_1s (
        ts_utc      TEXT    NOT NULL,
        market_id   TEXT    NOT NULL,
        side        TEXT    NOT NULL,
        level       INTEGER NOT NULL,
        price       REAL,
        size        REAL,
        ingest_ts_utc TEXT  NOT NULL,
        PRIMARY KEY (market_id, ts_utc, side, level)
    );
    """)
    conn.commit()


def insert_market_1s(conn: sqlite3.Connection, row: dict):
    conn.execute("""
        INSERT OR REPLACE INTO pm_sports_market_1s
        (ts_utc, market_id, volume_1s, trades_1s,
         best_bid_p, best_bid_sz, best_ask_p, best_ask_sz,
         depth_bid_1c, depth_ask_1c, depth_bid_5c, depth_ask_5c,
         depth_bid_10c, depth_ask_10c, depth_bid_15c, depth_ask_15c,
         spread, ingest_ts_utc)
        VALUES
        (:ts_utc, :market_id, :volume_1s, :trades_1s,
         :best_bid_p, :best_bid_sz, :best_ask_p, :best_ask_sz,
         :depth_bid_1c, :depth_ask_1c, :depth_bid_5c, :depth_ask_5c,
         :depth_bid_10c, :depth_ask_10c, :depth_bid_15c, :depth_ask_15c,
         :spread, :ingest_ts_utc)
    """, row)


def insert_orderbook_levels(conn: sqlite3.Connection, rows: list[dict]):
    conn.executemany("""
        INSERT OR REPLACE INTO pm_sports_orderbook_top5_1s
        (ts_utc, market_id, side, level, price, size, ingest_ts_utc)
        VALUES
        (:ts_utc, :market_id, :side, :level, :price, :size, :ingest_ts_utc)
    """, rows)
