# Zerodha Liquidity Analysis

Tick data pipeline: Zerodha WebSocket → Redis stream → receiver (Redis + SQLite).

## Features

- **Automated login** with TOTP 2FA (scheduled or `--login-now`)
- **Multi-socket**: up to 3 WebSocket connections, instruments distributed across them
- **Non-blocking**: ticks pushed to Redis stream; receiver consumes and persists to SQLite
- **Expiry filter**: current+next week and current+next month (configurable)
- **Instruments**: options (CE/PE) and futures (FUT); configurable in `config.yaml` → `instruments.instrument_types`

## Prerequisites

- Python 3.10+
- Redis 6+
- Zerodha Kite Connect API key (app authorized for your user)

## Install

```bash
cd zerodha-liquidity-analysis
pip install -r requirements.txt
```

## Config

Edit `config.yaml`:

```yaml
zerodha:
  api_key: "YOUR_API_KEY"
  api_secret: "YOUR_API_SECRET"
  user_id: "YOUR_USER_ID"
  password: "YOUR_PASSWORD"
  totp_secret: "YOUR_TOTP_SECRET"   # Base32 from Zerodha
```

## Usage

**Terminal 1 – Connector (login, subscribe, push to Redis):**

```bash
python run_connector.py --symbol NIFTY --login-now
# Or schedule daily at 08:45:  python run_connector.py --symbol NIFTY
# Debug:  python run_connector.py --symbol NIFTY --login-now --debug
```

**Terminal 2 – Receiver (consume stream, store in Redis + SQLite):**

```bash
python run_receiver.py
```

## Project layout

```
zerodha-liquidity-analysis/
├── config.yaml
├── requirements.txt
├── run_connector.py    # Entry: WebSocket connector
├── run_receiver.py     # Entry: Redis receiver + persistence
├── prompt.md
└── src/
    ├── auth.py         # Zerodha login (credentials + TOTP)
    ├── instruments.py  # Instrument filter + distribution
    ├── connector.py    # Multi-socket WebSocket manager
    ├── redis_publisher.py
    ├── receiver.py     # Stream consumer, Redis + persistence
    └── persistence.py  # SQLite dump
```

## Data

- **Redis stream**: `ticks:raw` (configurable)
- **Redis keys**: `ticks:{symbol}:{date}`, `latest:{symbol}`, `depth:{symbol}:{date}`, etc.
- **SQLite**: `./data/ticks.db` (ticks + tick_depths), dump interval in config
