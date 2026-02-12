# Zerodha Liquidity Analysis

Tick data pipeline: Zerodha WebSocket → Redis stream → receiver (Redis + SQLite).

## Features

- **Automated login** with TOTP 2FA (scheduled or `--login-now`)
- **Multi-symbol**: NIFTY, BANKNIFTY, FINNIFTY (and more) in one run; instruments shared across sockets
- **Multi-socket**: up to 3 WebSocket connections, instruments distributed across them
- **Non-blocking**: ticks pushed to Redis stream; receiver consumes and persists to SQLite
- **Expiry filter**: current+next week and current+next month (configurable)
- **Instruments**: spot (underlying), futures (FUT), options (CE/PE) for weekly and monthly expiries; configurable in `config.yaml` → `instruments.instrument_types`

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
# Multiple symbols (NIFTY, BANKNIFTY, FINNIFTY in one run, shared sockets):
python run_connector.py --symbol NIFTY,BANKNIFTY,FINNIFTY --login-now
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

## Checking Redis and SQLite (verify dumping)

Use these to confirm the receiver is consuming the stream and dumping correctly.

**Redis** (default: `localhost:6379`, db 0, stream `ticks:raw`)

```bash
# Stream length (should stay bounded; old entries trimmed by maxlen)
redis-cli XLEN ticks:raw

# Consumer group: pending messages (should be low if receiver keeps up)
redis-cli XINFO GROUPS ticks:raw

# Keys created by receiver (symbols per date, tick keys, latest snapshots)
redis-cli KEYS "ticks:*" | head -20
redis-cli KEYS "latest:*" | head -20
redis-cli KEYS "symbols:*"

# Count ticks stored for a symbol+date (replace with your symbol/date)
redis-cli ZCARD "ticks:NIFTY 50:2025-02-11"

# Latest snapshot for one symbol (replace SYMBOL with e.g. NIFTY25FEB24500CE)
redis-cli HGETALL "latest:SYMBOL"

# All symbols seen for a date
redis-cli SMEMBERS "symbols:2025-02-11"
```

**SQLite** (default: `./data/ticks.db`)

```bash
# From project root (zerodha-liquidity-analysis/)
DB=./data/ticks.db

# Total ticks and depth rows
sqlite3 "$DB" "SELECT 'ticks', count(*) FROM ticks UNION ALL SELECT 'tick_depths', count(*) FROM tick_depths;"

# Ticks per symbol and date (confirm symbols and dates match Redis)
sqlite3 "$DB" "SELECT tradingsymbol, trade_date, count(*) AS n FROM ticks GROUP BY tradingsymbol, trade_date ORDER BY n DESC LIMIT 25;"

# Date range in DB
sqlite3 "$DB" "SELECT min(trade_date) AS first, max(trade_date) AS last FROM ticks;"

# Sample rows (check tradingsymbol, timestamps, last_price)
sqlite3 -header -column "$DB" "SELECT id, instrument_token, tradingsymbol, exchange_timestamp, last_price, trade_date FROM ticks ORDER BY id DESC LIMIT 10;"
```

**What “dumping is right” looks like**

- **Redis**: `XLEN ticks:raw` bounded (e.g. &lt; max_stream_length); consumer group has small or zero pending; `KEYS ticks:*` and `symbols:*` exist and grow with traffic; `ZCARD ticks:SYMBOL:DATE` and `HGETALL latest:SYMBOL` show recent data.
- **SQLite**: Row counts increase after each dump interval (see receiver log “Persisting N ticks”); same symbols/dates as in Redis; `exchange_timestamp` and `trade_date` are sane; `tradingsymbol` has real names (e.g. NIFTY25FEB24500CE), not `TOKEN_*`.

## Memory and logging

**Seeing memory usage**

- By process name (connector or receiver):
  ```bash
  ps -o pid,rss,vsz,cmd -C python
  ```
  `RSS` = resident set size (KB); divide by 1024 for MB.
- Live, refresh every 2s:
  ```bash
  top -p $(pgrep -f "run_connector|run_receiver" | tr '\n' ',' | sed 's/,$//')
  ```
- One-liner to watch RSS of both scripts (MB):
  ```bash
  while true; do ps -o pid,rss,cmd -C python | awk 'NR==1 || /run_connector|run_receiver/{rss=$2/1024; print $0, rss" MB"}'; sleep 5; done
  ```

**Why logging is fine**

- Logging uses `RotatingFileHandler`: `max_bytes` (default 10 MB) and `backup_count` (5) in `config.yaml` → **logging.log_file**, so disk use is bounded and old logs are rotated away; the handler does not keep large buffers in memory.
- At **INFO** level the connector logs every 10s and the receiver only on persist; at **DEBUG** volume increases but messages are short.

**How to conclude logging is OK**

- Run connector and/or receiver for 30–60 minutes (or a full session).
- If **RSS stays roughly stable** (or grows once then plateaus), memory is fine and logging is not the cause.
- If **RSS grows without bound**, the leak is likely elsewhere (e.g. in-process buffers, Redis client, or SQLite); keep logging at INFO and profile the process (e.g. `tracemalloc`, `memory_profiler`) to find the source.
