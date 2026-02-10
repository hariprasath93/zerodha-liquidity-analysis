"""
Persistence module. Periodically dumps tick data from Redis buffer to SQLite on disk.
"""

import logging
import os
import sqlite3
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)


class TickPersistence:
    """Persists tick data to SQLite."""

    def __init__(self, config: dict):
        self.db_path = config.get("db_path", "./data/ticks.db")
        self.dump_interval = config.get("dump_interval_seconds", 300)
        self.parquet_enabled = config.get("parquet_enabled", False)
        self.parquet_dir = config.get("parquet_dir", "./data/parquet")
        self._conn: Optional[sqlite3.Connection] = None
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        if self.parquet_enabled:
            os.makedirs(self.parquet_dir, exist_ok=True)

    def initialize_db(self) -> None:
        """Create SQLite database and tables if not exist."""
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_token INTEGER NOT NULL,
                tradingsymbol TEXT NOT NULL,
                exchange_timestamp TEXT,
                last_price REAL,
                last_traded_quantity INTEGER,
                average_traded_price REAL,
                volume_traded INTEGER,
                total_buy_quantity INTEGER,
                total_sell_quantity INTEGER,
                open REAL, high REAL, low REAL, close REAL,
                change_pct REAL, oi INTEGER, oi_day_high INTEGER, oi_day_low INTEGER,
                tick_mode TEXT, received_at TEXT NOT NULL, trade_date TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tick_depths (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick_id INTEGER NOT NULL,
                side TEXT NOT NULL, level INTEGER NOT NULL,
                price REAL, quantity INTEGER, orders INTEGER,
                FOREIGN KEY (tick_id) REFERENCES ticks(id)
            );
            CREATE INDEX IF NOT EXISTS idx_ticks_token_date ON ticks(instrument_token, trade_date);
            CREATE INDEX IF NOT EXISTS idx_ticks_symbol_date ON ticks(tradingsymbol, trade_date);
            CREATE INDEX IF NOT EXISTS idx_ticks_timestamp ON ticks(exchange_timestamp);
            CREATE INDEX IF NOT EXISTS idx_depths_tick_id ON tick_depths(tick_id);
        """)
        self._conn.commit()
        logger.info("SQLite initialized at %s", self.db_path)

    def persist_ticks(self, ticks_data: list) -> int:
        """Write a batch of ticks to SQLite."""
        if not self._conn:
            self.initialize_db()
        if not ticks_data:
            return 0
        persisted = 0
        cursor = self._conn.cursor()
        try:
            for tick in ticks_data:
                ohlc = tick.get("ohlc", {})
                exchange_ts = tick.get("exchange_timestamp", "")
                trade_date = self._trade_date(exchange_ts)
                cursor.execute("""
                    INSERT INTO ticks (
                        instrument_token, tradingsymbol, exchange_timestamp,
                        last_price, last_traded_quantity, average_traded_price,
                        volume_traded, total_buy_quantity, total_sell_quantity,
                        open, high, low, close, change_pct, oi, oi_day_high, oi_day_low,
                        tick_mode, received_at, trade_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tick.get("instrument_token"),
                    tick.get("tradingsymbol", ""),
                    exchange_ts,
                    tick.get("last_price"),
                    tick.get("last_traded_quantity"),
                    tick.get("average_traded_price"),
                    tick.get("volume_traded"),
                    tick.get("total_buy_quantity"),
                    tick.get("total_sell_quantity"),
                    ohlc.get("open"), ohlc.get("high"), ohlc.get("low"), ohlc.get("close"),
                    tick.get("change"),
                    tick.get("oi"), tick.get("oi_day_high"), tick.get("oi_day_low"),
                    tick.get("mode", ""),
                    tick.get("received_at", datetime.now().isoformat()),
                    trade_date,
                ))
                tick_id = cursor.lastrowid
                depth = tick.get("depth", {})
                for side in ("buy", "sell"):
                    for level, entry in enumerate(depth.get(side, [])):
                        cursor.execute(
                            "INSERT INTO tick_depths (tick_id, side, level, price, quantity, orders) VALUES (?, ?, ?, ?, ?, ?)",
                            (tick_id, side, level, entry.get("price"), entry.get("quantity"), entry.get("orders")),
                        )
                persisted += 1
            self._conn.commit()
            logger.info("Persisted %d ticks to SQLite", persisted)
        except sqlite3.Error as e:
            logger.error("SQLite error: %s", e)
            self._conn.rollback()
        return persisted

    @staticmethod
    def _trade_date(ts_str: str) -> str:
        try:
            if ts_str:
                return datetime.fromisoformat(ts_str).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
        return date.today().isoformat()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            logger.info("SQLite connection closed")
