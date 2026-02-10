"""
Redis tick receiver module.

Consumes tick data from the Redis stream, segregates by symbol and time,
stores structured data in Redis for fast access, and periodically
persists to SQLite on disk.

This runs as a separate process from the WebSocket connector to ensure
the socket pipeline is never blocked by processing work.
"""

import json
import logging
import threading
import time
from datetime import datetime, date
from typing import Optional

import redis

from src.persistence import TickPersistence

logger = logging.getLogger(__name__)


class TickReceiver:
    """
    Consumes ticks from Redis stream, organizes in Redis, and
    periodically persists to disk.
    """

    def __init__(self, redis_config: dict, persistence_config: dict):
        """
        Args:
            redis_config: The 'redis' section from config.yaml.
            persistence_config: The 'persistence' section from config.yaml.
        """
        self.host = redis_config.get("host", "localhost")
        self.port = redis_config.get("port", 6379)
        self.db = redis_config.get("db", 0)
        self.password = redis_config.get("password", None)
        self.stream_name = redis_config.get("stream_name", "ticks:raw")
        self.consumer_group = redis_config.get("consumer_group", "tick_processors")
        self.consumer_name = redis_config.get("consumer_name", "receiver_1")
        self.redis_key_ttl = persistence_config.get("redis_key_ttl_seconds", 86400)

        self._client: Optional[redis.Redis] = None
        self._persistence = TickPersistence(persistence_config)
        self._dump_interval = persistence_config.get("dump_interval_seconds", 300)

        self._running = False
        self._ticks_processed = 0
        self._persist_thread: Optional[threading.Thread] = None
        self._pending_persist_buffer: list[dict] = []
        self._buffer_lock = threading.Lock()

    def connect(self) -> None:
        """Establish Redis connection and ensure consumer group exists."""
        logger.info("Connecting to Redis at %s:%d/%d", self.host, self.port, self.db)
        self._client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True,
            retry_on_timeout=True,
        )
        self._client.ping()
        logger.info("Redis connection established")

        # Ensure consumer group exists
        try:
            self._client.xgroup_create(
                self.stream_name, self.consumer_group, id="0", mkstream=True
            )
            logger.info("Created consumer group '%s'", self.consumer_group)
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(
                    "Consumer group '%s' already exists", self.consumer_group
                )
            else:
                raise

        # Initialize persistence
        self._persistence.initialize_db()

    def start(self) -> None:
        """
        Start the receiver loop and periodic persistence thread.

        This is the main entry point. It blocks the calling thread.
        """
        self._running = True

        # Start periodic persistence in background
        self._persist_thread = threading.Thread(
            target=self._periodic_persist,
            name="persist-worker",
            daemon=True,
        )
        self._persist_thread.start()

        logger.info(
            "Receiver started. Consuming from stream '%s' as '%s' in group '%s'",
            self.stream_name,
            self.consumer_name,
            self.consumer_group,
        )

        # Main consumption loop
        self._consume_loop()

    def _consume_loop(self) -> None:
        """
        Main loop: read from Redis stream using consumer groups.

        Uses XREADGROUP for reliable consumption with acknowledgement.
        """
        while self._running:
            try:
                # Block for up to 1000ms waiting for new messages
                messages = self._client.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_name,
                    streams={self.stream_name: ">"},
                    count=100,
                    block=1000,
                )

                if not messages:
                    continue

                for stream_name, entries in messages:
                    for entry_id, fields in entries:
                        try:
                            self._process_entry(entry_id, fields)
                        except Exception as e:
                            logger.error(
                                "Error processing entry %s: %s", entry_id, e
                            )

                        # Acknowledge the message
                        self._client.xack(
                            self.stream_name, self.consumer_group, entry_id
                        )

            except redis.ConnectionError as e:
                logger.error("Redis connection lost: %s. Reconnecting...", e)
                time.sleep(2)
                try:
                    self.connect()
                except Exception:
                    logger.error("Reconnection failed. Retrying in 5s...")
                    time.sleep(5)

            except Exception as e:
                logger.error("Unexpected error in consume loop: %s", e)
                time.sleep(1)

    def _process_entry(self, entry_id: str, fields: dict) -> None:
        """
        Process a single stream entry:
          1. Deserialize the tick data
          2. Store organized data in Redis (by symbol + time)
          3. Buffer for periodic SQLite persistence

        Args:
            entry_id: Redis stream entry ID.
            fields: Entry fields (contains 'data' key with JSON tick).
        """
        raw_data = fields.get("data", "")
        if not raw_data:
            return

        tick = json.loads(raw_data)
        tick["received_at"] = datetime.now().isoformat()
        self._ticks_processed += 1

        # Extract key fields
        instrument_token = tick.get("instrument_token", 0)
        tradingsymbol = tick.get("tradingsymbol", "TOKEN_%d" % instrument_token)
        exchange_ts = tick.get("exchange_timestamp", "")
        trade_date = self._extract_date(exchange_ts)

        # Store structured data in Redis
        self._store_in_redis(tick, tradingsymbol, trade_date, exchange_ts)

        # Buffer for periodic persistence to disk
        tick["tradingsymbol"] = tradingsymbol
        with self._buffer_lock:
            self._pending_persist_buffer.append(tick)

    def _store_in_redis(
        self,
        tick: dict,
        tradingsymbol: str,
        trade_date: str,
        exchange_ts: str,
    ) -> None:
        """
        Store the processed tick in Redis organized by symbol and time.

        Redis key structure:
          - Sorted set  ticks:{symbol}:{date}
              Score = unix timestamp, Value = serialized tick JSON
          - Hash  latest:{symbol}
              Latest tick snapshot for quick access
          - Set  symbols:{date}
              Tracks all active symbols for a given date
          - Sorted set  depth:{symbol}:{date}
              Depth snapshots (level 5 or 20) with timestamp scores
        """
        try:
            pipe = self._client.pipeline(transaction=False)

            # 1. Sorted set: full tick history by symbol and date
            ts_score = self._timestamp_to_score(exchange_ts)
            tick_key = "ticks:%s:%s" % (tradingsymbol, trade_date)
            tick_json = json.dumps(tick)
            pipe.zadd(tick_key, {tick_json: ts_score})
            pipe.expire(tick_key, self.redis_key_ttl)

            # 2. Hash: latest tick snapshot for quick access
            latest_key = "latest:%s" % tradingsymbol
            latest_data = {
                "last_price": str(tick.get("last_price", "")),
                "volume": str(tick.get("volume_traded", "")),
                "oi": str(tick.get("oi", "")),
                "bid": str(self._get_best_bid(tick)),
                "ask": str(self._get_best_ask(tick)),
                "exchange_timestamp": exchange_ts,
                "total_buy_qty": str(tick.get("total_buy_quantity", "")),
                "total_sell_qty": str(tick.get("total_sell_quantity", "")),
            }
            pipe.hset(latest_key, mapping=latest_data)
            pipe.expire(latest_key, self.redis_key_ttl)

            # 3. Track active symbols per date
            symbols_key = "symbols:%s" % trade_date
            pipe.sadd(symbols_key, tradingsymbol)
            pipe.expire(symbols_key, self.redis_key_ttl)

            # 4. Store depth data separately (level 5 or 20)
            depth = tick.get("depth", {})
            if depth:
                depth_key = "depth:%s:%s" % (tradingsymbol, trade_date)
                depth_entry = json.dumps({
                    "timestamp": exchange_ts,
                    "buy": depth.get("buy", []),
                    "sell": depth.get("sell", []),
                })
                pipe.zadd(depth_key, {depth_entry: ts_score})
                pipe.expire(depth_key, self.redis_key_ttl)

            pipe.execute()

        except redis.RedisError as e:
            logger.error("Failed to store tick in Redis: %s", e)

    def _periodic_persist(self) -> None:
        """
        Background thread: periodically flush buffered ticks to SQLite.

        Ensures data is saved to persistent disk storage (not just RAM).
        """
        logger.info(
            "Persistence thread started. Dumping every %ds", self._dump_interval
        )

        while self._running:
            time.sleep(self._dump_interval)

            # Swap out the buffer atomically
            with self._buffer_lock:
                if not self._pending_persist_buffer:
                    logger.debug("No ticks to persist in this cycle")
                    continue
                batch = self._pending_persist_buffer
                self._pending_persist_buffer = []

            logger.info("Persisting %d ticks to SQLite...", len(batch))
            try:
                count = self._persistence.persist_ticks(batch)
                logger.info("Successfully persisted %d ticks", count)
            except Exception as e:
                logger.error("Persistence failed: %s", e)
                # Put the failed batch back so we don't lose data
                with self._buffer_lock:
                    self._pending_persist_buffer = (
                        batch + self._pending_persist_buffer
                    )

        # Final flush on shutdown
        self._flush_remaining()

    def _flush_remaining(self) -> None:
        """Flush any remaining buffered ticks on shutdown."""
        with self._buffer_lock:
            if self._pending_persist_buffer:
                logger.info(
                    "Final flush: persisting %d remaining ticks",
                    len(self._pending_persist_buffer),
                )
                try:
                    self._persistence.persist_ticks(
                        self._pending_persist_buffer
                    )
                    self._pending_persist_buffer = []
                except Exception as e:
                    logger.error("Final flush failed: %s", e)

    @staticmethod
    def _timestamp_to_score(ts_str: str) -> float:
        """Convert ISO timestamp to Unix float for sorted set scoring."""
        try:
            if ts_str:
                dt = datetime.fromisoformat(ts_str)
                return dt.timestamp()
        except (ValueError, TypeError):
            pass
        return time.time()

    @staticmethod
    def _extract_date(ts_str: str) -> str:
        """Extract YYYY-MM-DD from an ISO timestamp."""
        try:
            if ts_str:
                dt = datetime.fromisoformat(ts_str)
                return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
        return date.today().isoformat()

    @staticmethod
    def _get_best_bid(tick: dict) -> float:
        """Extract best bid price from depth data."""
        depth = tick.get("depth", {})
        buy_levels = depth.get("buy", [])
        if buy_levels:
            return buy_levels[0].get("price", 0)
        return 0

    @staticmethod
    def _get_best_ask(tick: dict) -> float:
        """Extract best ask price from depth data."""
        depth = tick.get("depth", {})
        sell_levels = depth.get("sell", [])
        if sell_levels:
            return sell_levels[0].get("price", 0)
        return 0

    def stop(self) -> None:
        """Gracefully stop the receiver."""
        logger.info("Stopping receiver...")
        self._running = False

        # Wait for persistence thread to finish
        if self._persist_thread and self._persist_thread.is_alive():
            self._persist_thread.join(timeout=10)

        # Final flush
        self._flush_remaining()

        # Close persistence
        self._persistence.close()

        logger.info(
            "Receiver stopped. Total ticks processed: %d", self._ticks_processed
        )

    def get_stats(self) -> dict:
        """Return current receiver statistics."""
        with self._buffer_lock:
            buffer_size = len(self._pending_persist_buffer)

        return {
            "ticks_processed": self._ticks_processed,
            "buffer_pending": buffer_size,
            "running": self._running,
        }
