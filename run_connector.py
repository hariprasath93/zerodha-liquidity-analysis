#!/usr/bin/env python3
"""
Zerodha Tick Connector. Logs in, subscribes to derivative ticks for the given
symbol across up to 3 WebSockets, forwards ticks to Redis stream.

Usage:
  python run_connector.py --symbol NIFTY --login-now
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
        return yaml.safe_load(f)


def signal_handler(signum, frame):
    global _connector, _publisher
    if _connector:
        _connector.stop()
    if _publisher:
        _publisher.close()
    sys.exit(0)


def run_trading_session(symbol: str, config: dict) -> None:
    global _connector, _publisher
    logger = logging.getLogger(__name__)
    try:
        logger.info("STARTING TRADING SESSION FOR: %s", symbol)
        auth = ZerodhaAuth(config["zerodha"])
        kite = auth.login()
        logger.info("Authentication successful")

        inst_mgr = InstrumentManager(kite, config.get("instruments", {}))
        inst_mgr.fetch_instruments()
        derivatives = inst_mgr.get_derivative_tokens(symbol)
        logger.info("Found %d derivative instruments for %s", len(derivatives), symbol)
        all_instruments = list(derivatives)
        underlying = inst_mgr.get_underlying_token(symbol)
        if underlying:
            all_instruments.append(underlying)

        if not all_instruments:
            logger.error("No instruments for %s", symbol)
            return

        num_sockets = config.get("connector", {}).get("num_sockets", 3)
        token_buckets = inst_mgr.distribute_tokens(all_instruments, num_sockets)

        _publisher = RedisPublisher(config.get("redis", {}))
        _publisher.connect()
        _publisher.ensure_consumer_group(config.get("redis", {}).get("consumer_group", "tick_processors"))

        _connector = MultiSocketConnector(
            api_key=config["zerodha"]["api_key"],
            access_token=auth.get_access_token(),
            publisher=_publisher,
            config=config.get("connector", {}),
        )
        _connector.start(token_buckets)
        logger.info("Connector running. Ctrl+C to stop.")

        while True:
            time.sleep(10)
            logger.info("ticks=%d connected=%s", _connector.total_tick_count, _connector.all_connected)
            now = datetime.now(IST)
            if now.hour >= 15 and now.minute >= 35:
                logger.info("Market closed. Shutting down.")
                break
    except AuthenticationError as e:
        logger.error("Auth failed: %s", e)
    except KeyboardInterrupt:
        pass
    finally:
        if _connector:
            _connector.stop()
        if _publisher:
            _publisher.close()
        logger.info("Trading session ended")


def main():
    parser = argparse.ArgumentParser(description="Zerodha Tick Connector")
    parser.add_argument("--symbol", required=True, help="e.g. NIFTY, BANKNIFTY")
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

    symbol = args.symbol.upper()
    if args.login_now:
        run_trading_session(symbol, config)
    else:
        login_time = config.get("connector", {}).get("login_time", "08:45")
        schedule.every().day.at(login_time).do(run_trading_session, symbol=symbol, config=config)
        logger.info("Scheduled daily login at %s IST", login_time)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    main()
