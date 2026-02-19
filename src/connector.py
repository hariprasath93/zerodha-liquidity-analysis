"""
Zerodha WebSocket connector. Manages up to 3 KiteTicker connections,
distributes subscriptions, forwards ticks to Redis stream (non-blocking).
"""

import logging
import threading
import time
from typing import Optional

# Twisted (used by KiteTicker) installs signal handlers when the reactor runs.
# We run each WebSocket in a daemon thread, so signal.signal() would raise
# "signal only works in main thread". Patch signal installation to no-op in non-main threads.
def _patch_twisted_signals():
    try:
        import twisted.internet._signals as _sigmod
        # _WithSignalHandling.install() sets SIGINT/SIGTERM/SIGBREAK
        _original_with_install = _sigmod._WithSignalHandling.install

        def _guarded_with_install(self):
            if threading.current_thread() is threading.main_thread():
                _original_with_install(self)

        _sigmod._WithSignalHandling.install = _guarded_with_install
        # installHandler() sets SIGCHLD (used by _SIGCHLDWaker)
        _original_install_handler = _sigmod.installHandler

        def _guarded_install_handler(fd: int) -> int:
            if threading.current_thread() is not threading.main_thread():
                return -1
            return _original_install_handler(fd)

        _sigmod.installHandler = _guarded_install_handler
    except Exception:
        pass


_patch_twisted_signals()

from kiteconnect import KiteTicker

from src.redis_publisher import RedisPublisher

logger = logging.getLogger(__name__)

MODE_LTP, MODE_QUOTE, MODE_FULL = "ltp", "quote", "full"
MODE_MAP = {"ltp": MODE_LTP, "quote": MODE_QUOTE, "full": MODE_FULL}


class SocketConnection:
    """Single KiteTicker WebSocket connection."""

    def __init__(self, socket_id: int, api_key: str, access_token: str, tokens: list,
                 tick_mode: str, publisher: RedisPublisher, token_to_symbol: dict,
                 reconnect_max_retries: int = 50, reconnect_max_delay: int = 60):
        self.socket_id = socket_id
        self.tokens = tokens
        self.tick_mode = MODE_MAP.get(tick_mode, MODE_FULL)
        self.publisher = publisher
        self.token_to_symbol = token_to_symbol
        self._tick_count = 0
        self._connected = False
        self._last_tick_time: float = time.time()

        self.kws = KiteTicker(
            api_key=api_key,
            access_token=access_token,
            reconnect=True,
            reconnect_max_tries=reconnect_max_retries,
            reconnect_max_delay=reconnect_max_delay,
        )
        self.kws.on_ticks = self._on_ticks
        self.kws.on_connect = self._on_connect
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.kws.on_reconnect = lambda ws, n: logger.info("Socket %d reconnecting attempt %d", self.socket_id, n)
        self.kws.on_noreconnect = lambda ws: logger.critical("Socket %d max reconnects exhausted", self.socket_id)
        self.kws.on_order_update = lambda ws, data: None

    def _on_ticks(self, ws, ticks: list) -> None:
        self._tick_count += len(ticks)
        self._last_tick_time = time.time()
        try:
            for tick in ticks:
                token = tick.get("instrument_token")
                info = self.token_to_symbol.get(token, {})
                tick["tradingsymbol"] = info.get("tradingsymbol", f"TOKEN_{token}" if token else "UNKNOWN")
                if "name" in info:
                    tick["name"] = info["name"]
            self.publisher.publish_ticks(ticks)
        except Exception as e:
            logger.error("Socket %d publish error: %s", self.socket_id, e)

    def _on_connect(self, ws, response) -> None:
        self._connected = True
        logger.info("Socket %d connected, subscribing %d tokens", self.socket_id, len(self.tokens))
        if self.tokens:
            ws.subscribe(self.tokens)
            ws.set_mode(self.tick_mode, self.tokens)

    def _on_close(self, ws, code, reason) -> None:
        self._connected = False
        logger.warning("Socket %d closed [%s] %s", self.socket_id, code, reason)

    def _on_error(self, ws, code, reason) -> None:
        logger.error("Socket %d error [%s] %s", self.socket_id, code, reason)

    def start(self) -> threading.Thread:
        t = threading.Thread(target=lambda: self.kws.connect(threaded=False), name=f"ws-{self.socket_id}", daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        try:
            self.kws.close()
        except Exception as e:
            logger.warning("Socket %d close error: %s", self.socket_id, e)
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def last_tick_time(self) -> float:
        return self._last_tick_time


class MultiSocketConnector:
    """Manages multiple WebSocket connections for distributed tick subscriptions."""

    def __init__(self, api_key: str, access_token: str, publisher: RedisPublisher, config: dict,
                 token_to_symbol: dict):
        self.api_key = api_key
        self.access_token = access_token
        self.publisher = publisher
        self.token_to_symbol = token_to_symbol
        self.num_sockets = min(config.get("num_sockets", 3), 3)
        self.tick_mode = config.get("tick_mode", "full")
        self.reconnect_max_retries = config.get("reconnect_max_retries", 50)
        self.reconnect_max_delay = config.get("reconnect_max_delay", 60)
        self.sockets: list = []

    def start(self, token_buckets: list) -> None:
        self.sockets = []
        active = [b for b in token_buckets if len(b) > 0]
        if not active:
            logger.warning("No tokens to subscribe")
            return
        for idx, bucket in enumerate(active):
            sock = SocketConnection(
                socket_id=idx,
                api_key=self.api_key,
                access_token=self.access_token,
                tokens=bucket,
                tick_mode=self.tick_mode,
                publisher=self.publisher,
                token_to_symbol=self.token_to_symbol,
                reconnect_max_retries=self.reconnect_max_retries,
                reconnect_max_delay=self.reconnect_max_delay,
            )
            sock.start()
            self.sockets.append(sock)
            if idx < len(active) - 1:
                time.sleep(1)
        logger.info("Started %d WebSocket connections", len(self.sockets))

    def stop(self) -> None:
        for s in self.sockets:
            s.stop()
        self.sockets.clear()
        logger.info("All WebSocket connections stopped")

    def reconnect_all(self, token_buckets: list) -> None:
        logger.warning("Watchdog triggered — forcing reconnect of all sockets")
        self.stop()
        time.sleep(2)
        self.start(token_buckets)

    @property
    def total_tick_count(self) -> int:
        return sum(s.tick_count for s in self.sockets)

    @property
    def all_connected(self) -> bool:
        return all(s.is_connected for s in self.sockets)

    @property
    def last_tick_time(self) -> float:
        if not self.sockets:
            return time.time()
        return max(s.last_tick_time for s in self.sockets)
