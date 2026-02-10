#!/usr/bin/env python3
"""
Redis Tick Receiver. Consumes from Redis stream, segregates by symbol/time,
stores in Redis, periodically persists to SQLite. Run as separate process.

Usage:
  python run_receiver.py
  python run_receiver.py --config my_config.yaml
  python run_receiver.py --debug
"""

import argparse
import logging
import logging.handlers
import os
import signal
import sys
from typing import Optional

import yaml

from src.receiver import TickReceiver

_receiver = None


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
    global _receiver
    if _receiver:
        _receiver.stop()
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="Redis Tick Receiver")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--consumer-name", default=None)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default=None)
    args = parser.parse_args()

    config = load_config(args.config)
    log_override = args.log_level or ("DEBUG" if args.debug else None)
    setup_logging(config, log_level_override=log_override)
    logger = logging.getLogger(__name__)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    redis_config = config.get("redis", {})
    if args.consumer_name:
        redis_config["consumer_name"] = args.consumer_name
    persistence_config = config.get("persistence", {})

    global _receiver
    _receiver = TickReceiver(redis_config, persistence_config)
    try:
        _receiver.connect()
        logger.info("Receiver started. Ctrl+C to stop.")
        _receiver.start()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("Fatal: %s", e)
    finally:
        if _receiver:
            _receiver.stop()
        logger.info("Receiver shut down")


if __name__ == "__main__":
    main()
