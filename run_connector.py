#!/usr/bin/env python3
"""
Zerodha Tick Connector. Logs in, subscribes to derivative ticks for the given
symbol(s) across up to 3 WebSockets, forwards ticks to Redis stream.

Usage:
  python run_connector.py --symbol NIFTY --login-now
  python run_connector.py --symbol NIFTY,BANKNIFTY,FINNIFTY --login-now
  python run_connector.py --symbol NIFTY  # scheduled login at config login_time
  python run_connector.py --symbol NIFTY --debug
"""

import argparse
import logging
import logging.handlers
import os
import signal
import sys
import time
from datetime import datetime
from typing import Optional

import pytz
import schedule
import yaml

from src.auth import ZerodhaAuth, AuthenticationError
from src.connector import MultiSocketConnector
from src.instruments import InstrumentManager
from src.notifier import TelegramNotifier
from src.redis_publisher import RedisPublisher

IST = pytz.timezone("Asia/Kolkata")
_connector = None
_publisher = None


def setup_logging(config: dict, log_level_override: Optional[str] = None) -> None:
    log_config = config.get("logging", {})
    level_name = log_level_override or log_config.get("level", "INFO")
    log_level = getattr(logging, level_name.upper(), logging.INFO)
    log_file = log_config.get("log_file", "./logs/app.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(logging.handlers.RotatingFileHandler(log_file, maxBytes=log_config.get("max_bytes", 10485760), backupCount=log_config.get("backup_count", 5)))
    root.addHandler(logging.StreamHandler(sys.stdout))
    for h in root.handlers:
        h.setFormatter(formatter)


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    # Override config values from environment variables
    env_overrides = {
        "zerodha": {
            "api_key": "ZERODHA_API_KEY",
            "api_secret": "ZERODHA_API_SECRET",
            "user_id": "ZERODHA_USER_ID",
            "password": "ZERODHA_PASSWORD",
            "totp_secret": "ZERODHA_TOTP_SECRET",
        },
        "redis": {
            "host": "REDIS_HOST",
        },
    }
    for section, keys in env_overrides.items():
        for key, env_var in keys.items():
            val = os.environ.get(env_var)
            if val:
                config.setdefault(section, {})[key] = val
    return config


def signal_handler(signum, frame):
    global _connector, _publisher
    if _connector:
        _connector.stop()
    if _publisher:
        _publisher.close()
    sys.exit(0)


def run_trading_session(symbols: list[str], config: dict) -> None:
    global _connector, _publisher
    logger = logging.getLogger(__name__)
    notifier = TelegramNotifier.from_config(config)
    token_to_symbol = {}

    try:
        logger.info("STARTING TRADING SESSION FOR: %s", ", ".join(symbols))
        auth = ZerodhaAuth(config["zerodha"])
        kite = auth.login()
        logger.info("Authentication successful")

        inst_mgr = InstrumentManager(kite, config.get("instruments", {}))
        all_instruments = inst_mgr.get_instruments_for_symbols(symbols)

        if not all_instruments:
            logger.error("No instruments for %s", ", ".join(symbols))
            notifier.send_error(f"No instruments found for {', '.join(symbols)}")
            return

        num_sockets = config.get("connector", {}).get("num_sockets", 3)
        token_buckets = inst_mgr.distribute_tokens(all_instruments, num_sockets)
        token_to_symbol = inst_mgr.get_token_symbol_map()

        notifier.send_login_success(symbols, len(all_instruments))

        _publisher = RedisPublisher(config.get("redis", {}))
        _publisher.connect()
        _publisher.ensure_consumer_group(config.get("redis", {}).get("consumer_group", "tick_processors"))

        _connector = MultiSocketConnector(
            api_key=config["zerodha"]["api_key"],
            access_token=auth.get_access_token(),
            publisher=_publisher,
            config=config.get("connector", {}),
            token_to_symbol=token_to_symbol,
        )
        _connector.start(token_buckets)
        logger.info("Connector running. Ctrl+C to stop.")

        market_open_sent = False
        last_hourly_hour = None
        session_start = datetime.now(IST)

        while True:
            time.sleep(10)
            now = datetime.now(IST)
            logger.info("ticks=%d connected=%s", _connector.total_tick_count, _connector.all_connected)

            # Send market-open notification once after 9:15
            if not market_open_sent and now.hour >= 9 and now.minute >= 15:
                notifier.send_market_open(len(token_to_symbol))
                market_open_sent = True

            # Send hourly stats on the hour (10:00, 11:00, ..., 15:00)
            if now.hour >= 10 and now.minute < 1 and last_hourly_hour != now.hour:
                elapsed = now.hour - session_start.hour
                notifier.send_hourly_stats(_connector.total_tick_count, elapsed)
                last_hourly_hour = now.hour

            # Shutdown after 15:45
            if now.hour >= 15 and now.minute >= 45:
                logger.info("Market closed. Shutting down.")
                break

    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
        notifier.send_error(f"Authentication failed: {e}")
    except KeyboardInterrupt:
        pass
    finally:
        total_ticks = _connector.total_tick_count if _connector else 0
        symbol_count = len(token_to_symbol)
        if _connector:
            _connector.stop()
        if _publisher:
            _publisher.close()
        if total_ticks > 0:
            notifier.send_session_end(total_ticks, symbol_count)
        logger.info("Trading session ended")


def main():
    parser = argparse.ArgumentParser(description="Zerodha Tick Connector")
    parser.add_argument(
        "--symbol",
        required=True,
        help="Symbol(s), comma-separated. e.g. NIFTY or NIFTY,BANKNIFTY,FINNIFTY",
    )
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument("--login-now", action="store_true", help="Login immediately")
    parser.add_argument("--debug", action="store_true", help="DEBUG log level")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    log_override = args.log_level or ("DEBUG" if args.debug else None)
    setup_logging(config, log_level_override=log_override)
    logger = logging.getLogger(__name__)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    symbols = [s.strip().upper() for s in args.symbol.split(",") if s.strip()]
    if not symbols:
        logger.error("No valid symbols provided")
        sys.exit(1)

    if args.login_now:
        run_trading_session(symbols, config)
    else:
        login_time = config.get("connector", {}).get("login_time", "08:50")
        schedule.every().day.at(login_time).do(run_trading_session, symbols=symbols, config=config)
        logger.info("Scheduled daily login at %s IST for %s", login_time, ", ".join(symbols))
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    main()
