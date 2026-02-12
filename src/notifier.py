"""Telegram notifications for the tick data pipeline."""

import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Sends status messages to a Telegram chat via the Bot API."""

    def __init__(self, bot_token: str, chat_id: str, enabled: bool = True):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled and bool(bot_token) and bool(chat_id)
        if not self.enabled:
            logger.warning("Telegram notifications disabled (missing token/chat_id or enabled=false)")

    @classmethod
    def from_config(cls, config: dict) -> "TelegramNotifier":
        """Create from config dict, with env var overrides."""
        import os
        tg = config.get("telegram", {})
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN") or tg.get("bot_token", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID") or tg.get("chat_id", "")
        enabled = tg.get("enabled", True)
        return cls(bot_token=bot_token, chat_id=chat_id, enabled=enabled)

    def send(self, message: str) -> bool:
        """Send a message. Returns True on success. Never raises."""
        if not self.enabled:
            return False
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            resp = requests.post(url, json={
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
            }, timeout=10)
            if not resp.ok:
                logger.warning("Telegram send failed: %s %s", resp.status_code, resp.text[:200])
                return False
            return True
        except Exception as e:
            logger.warning("Telegram send error: %s", e)
            return False

    def send_login_success(self, symbols: list[str], instrument_count: int) -> None:
        self.send(
            f"✅ <b>Login successful</b>\n"
            f"Subscribed to <b>{instrument_count}</b> instruments for {', '.join(symbols)}"
        )

    def send_market_open(self, symbol_count: int) -> None:
        self.send(f"📊 <b>Market open</b>. Recording ticks for {symbol_count} symbols.")

    def send_hourly_stats(self, total_ticks: int, elapsed_hours: int) -> None:
        self.send(
            f"📈 <b>Hourly update</b> ({elapsed_hours}h elapsed)\n"
            f"Total ticks so far: <b>{total_ticks:,}</b>"
        )

    def send_session_end(self, total_ticks: int, symbol_count: int) -> None:
        self.send(
            f"🏁 <b>Session ended</b>\n"
            f"Total: <b>{total_ticks:,}</b> ticks saved across {symbol_count} symbols."
        )

    def send_error(self, error: str) -> None:
        self.send(f"❌ <b>Error</b>: {error}")
