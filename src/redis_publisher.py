"""
Redis stream publisher. Pushes tick data to a Redis stream (non-blocking).
"""

import json
import logging
from datetime import datetime
from typing import Optional

import redis

logger = logging.getLogger(__name__)


class RedisPublisher:
    """Publishes tick data to a Redis stream."""

    def __init__(self, config: dict):
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 6379)
        self.db = config.get("db", 0)
        self.password = config.get("password", None)
        self.stream_name = config.get("stream_name", "ticks:raw")
        self.max_stream_length = config.get("max_stream_length", 100000)
        self._client: Optional[redis.Redis] = None

    def connect(self) -> None:
        """Establish connection to Redis."""
        logger.info("Connecting to Redis at %s:%d/%d", self.host, self.port, self.db)
        self._client = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_keepalive=True,
            retry_on_timeout=True,
        )
        self._client.ping()
        logger.info("Redis connection established")

    def publish_ticks(self, ticks: list) -> None:
        """Publish a batch of ticks to the Redis stream (non-blocking)."""
        if self._client is None:
            logger.error("Redis not connected. Call connect() first.")
            return
        try:
            pipe = self._client.pipeline(transaction=False)
            for tick in ticks:
                serialized = self._serialize_tick(tick)
                pipe.xadd(
                    self.stream_name,
                    {"data": serialized},
                    maxlen=self.max_stream_length,
                    approximate=True,
                )
            pipe.execute()
        except redis.RedisError as e:
            logger.error("Failed to publish ticks: %s", e)

    @staticmethod
    def _serialize_tick(tick: dict) -> bytes:
        def _default(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        return json.dumps(tick, default=_default).encode("utf-8")

    def ensure_consumer_group(self, group_name: str) -> None:
        try:
            self._client.xgroup_create(self.stream_name, group_name, id="0", mkstream=True)
            logger.info("Created consumer group '%s'", group_name)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def get_stream_info(self) -> dict:
        try:
            return self._client.xinfo_stream(self.stream_name)
        except redis.ResponseError:
            return {"length": 0}

    def close(self) -> None:
        if self._client:
            self._client.close()
            logger.info("Redis publisher closed")
