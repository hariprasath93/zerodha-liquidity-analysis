#!/bin/bash
# Start tick pipeline only on NSE trading days (skip weekends + holidays)

# NSE holidays 2026 (weekdays only)
HOLIDAYS="
2026-01-26
2026-03-03
2026-03-26
2026-03-31
2026-04-03
2026-05-01
2026-06-26
2026-10-02
2026-10-21
2026-11-09
2026-11-19
2026-12-25
"

TODAY=$(date +%Y-%m-%d)

if echo "$HOLIDAYS" | grep -q "$TODAY"; then
    echo "$(date) — $TODAY is an NSE holiday, skipping."
    exit 0
fi

echo "$(date) — $TODAY is a trading day, starting containers."
cd /mnt/workbench/harry/research/wkspc/zerodha-liquidity-analysis
/usr/bin/docker compose up -d --build
