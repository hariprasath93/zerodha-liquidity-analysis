"""Streamlit dashboard for NIFTY / BANKNIFTY / FINNIFTY liquidity analysis."""

import re
import sqlite3

import pandas as pd
import streamlit as st

DB_PATH = "data/ticks.db"

INSTRUMENTS = {
    "NIFTY": dict(lot=65, strike_gap=50, spot_sym="NIFTY 50", fut_prefix="NIFTY", opt_prefix="NIFTY"),
    "BANKNIFTY": dict(lot=30, strike_gap=100, spot_sym="NIFTY BANK", fut_prefix="BANKNIFTY", opt_prefix="BANKNIFTY"),
    "FINNIFTY": dict(lot=25, strike_gap=50, spot_sym="NIFTY FIN SERVICE", fut_prefix="FINNIFTY", opt_prefix="FINNIFTY"),
}

MARKET_OPEN, MARKET_CLOSE = "09:15:00", "15:30:00"


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


# ── cached query helpers ────────────────────────────────────────────


@st.cache_data(ttl=300)
def available_dates():
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT trade_date FROM ticks WHERE trade_date > '2000-01-01' ORDER BY trade_date"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def futures_symbols(prefix: str):
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT tradingsymbol FROM ticks WHERE tradingsymbol LIKE ? ORDER BY tradingsymbol",
        (f"{prefix}%FUT",),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def spot_last_price(spot_sym: str, trade_date: str) -> float | None:
    conn = get_conn()
    row = conn.execute(
        "SELECT last_price FROM ticks WHERE tradingsymbol = ? AND trade_date = ? ORDER BY exchange_timestamp DESC LIMIT 1",
        (spot_sym, trade_date),
    ).fetchone()
    conn.close()
    return row[0] if row else None


@st.cache_data(ttl=300)
def option_symbols_for_date(prefix: str, trade_date: str):
    conn = get_conn()
    # Use prefix + 2-digit year to avoid matching spot symbols like "NIFTY 50" or "NIFTY BANK"
    rows = conn.execute(
        "SELECT DISTINCT tradingsymbol FROM ticks WHERE tradingsymbol LIKE ? AND trade_date = ? AND (tradingsymbol LIKE '%CE' OR tradingsymbol LIKE '%PE')",
        (f"{prefix}26%", trade_date),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


_MONTHS = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"

def parse_strike(sym: str, prefix: str) -> tuple[float, str] | None:
    """Extract (strike, type) from e.g. NIFTY2621723100CE → (23100, 'CE')."""
    # Monthly: PREFIX + YY + MON + STRIKE + CE/PE  (e.g. NIFTY26FEB23100CE)
    m = re.match(rf"^{prefix}\d{{2}}(?:{_MONTHS})(\d+)(CE|PE)$", sym)
    if m:
        return float(m.group(1)), m.group(2)
    # Weekly: PREFIX + YYMDD + STRIKE + CE/PE  (e.g. NIFTY2621723100CE — 5 date digits)
    m = re.match(rf"^{prefix}\d{{5}}(\d+)(CE|PE)$", sym)
    if m:
        return float(m.group(1)), m.group(2)
    return None


@st.cache_data(ttl=300)
def load_futures_spread(symbol: str, trade_date: str) -> pd.DataFrame:
    conn = get_conn()
    q = """
    SELECT t.id, t.exchange_timestamp, t.last_price, t.total_buy_quantity, t.total_sell_quantity,
           buy.price AS best_bid, buy.quantity AS bid_qty, buy.orders AS bid_orders,
           sell.price AS best_ask, sell.quantity AS ask_qty, sell.orders AS ask_orders
    FROM ticks t
    JOIN tick_depths buy  ON buy.tick_id  = t.id AND buy.side  = 'buy'  AND buy.level = 0
    JOIN tick_depths sell ON sell.tick_id  = t.id AND sell.side = 'sell' AND sell.level = 0
    WHERE t.tradingsymbol = ? AND t.trade_date = ?
      AND time(t.exchange_timestamp) BETWEEN ? AND ?
    ORDER BY t.exchange_timestamp
    """
    df = pd.read_sql_query(q, conn, params=(symbol, trade_date, MARKET_OPEN, MARKET_CLOSE))
    conn.close()
    if df.empty:
        return df
    df["exchange_timestamp"] = pd.to_datetime(df["exchange_timestamp"], format="ISO8601")
    df["spread_pts"] = df["best_ask"] - df["best_bid"]
    mid = (df["best_ask"] + df["best_bid"]) / 2
    df["spread_bps"] = (df["spread_pts"] / mid) * 10000
    df["ofi"] = df["total_buy_quantity"] - df["total_sell_quantity"]
    return df


@st.cache_data(ttl=300)
def load_option_spreads(symbols: list[str], trade_date: str) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    conn = get_conn()
    placeholders = ",".join("?" * len(symbols))
    q = f"""
    SELECT t.tradingsymbol, AVG(sell.price - buy.price) AS avg_spread,
           AVG((sell.price - buy.price) / NULLIF((sell.price + buy.price) / 2, 0)) * 10000 AS avg_spread_bps,
           SUM(t.volume_traded) AS total_volume
    FROM ticks t
    JOIN tick_depths buy  ON buy.tick_id  = t.id AND buy.side  = 'buy'  AND buy.level = 0
    JOIN tick_depths sell ON sell.tick_id  = t.id AND sell.side = 'sell' AND sell.level = 0
    WHERE t.tradingsymbol IN ({placeholders}) AND t.trade_date = ?
      AND time(t.exchange_timestamp) BETWEEN ? AND ?
      AND buy.price > 0 AND sell.price > 0
    GROUP BY t.tradingsymbol
    """
    df = pd.read_sql_query(q, conn, params=(*symbols, trade_date, MARKET_OPEN, MARKET_CLOSE))
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_slippage_minute(fut_sym: str, ce_sym: str, pe_sym: str, trade_date: str) -> pd.DataFrame:
    conn = get_conn()
    q = """
    SELECT strftime('%H:%M', t.exchange_timestamp) AS minute,
           AVG(sell.price - buy.price) / 2 AS half_spread
    FROM ticks t
    JOIN tick_depths buy  ON buy.tick_id  = t.id AND buy.side  = 'buy'  AND buy.level = 0
    JOIN tick_depths sell ON sell.tick_id  = t.id AND sell.side = 'sell' AND sell.level = 0
    WHERE t.tradingsymbol = ? AND t.trade_date = ?
      AND time(t.exchange_timestamp) BETWEEN ? AND ?
      AND buy.price > 0 AND sell.price > 0
    GROUP BY minute ORDER BY minute
    """
    fut_df = pd.read_sql_query(q, conn, params=(fut_sym, trade_date, MARKET_OPEN, MARKET_CLOSE))
    fut_df = fut_df.rename(columns={"half_spread": "fut_half_spread"})

    # Synthetic = avg of CE half-spread + PE half-spread
    ce_df = pd.read_sql_query(q, conn, params=(ce_sym, trade_date, MARKET_OPEN, MARKET_CLOSE))
    pe_df = pd.read_sql_query(q, conn, params=(pe_sym, trade_date, MARKET_OPEN, MARKET_CLOSE))
    conn.close()

    if ce_df.empty or pe_df.empty or fut_df.empty:
        return pd.DataFrame()

    merged = fut_df.merge(
        ce_df.rename(columns={"half_spread": "ce_half_spread"}), on="minute", how="inner"
    ).merge(
        pe_df.rename(columns={"half_spread": "pe_half_spread"}), on="minute", how="inner"
    )
    merged["syn_half_spread"] = merged["ce_half_spread"] + merged["pe_half_spread"]
    return merged


@st.cache_data(ttl=300)
def load_depth_stats(symbols: list[str], trade_date: str) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    conn = get_conn()
    placeholders = ",".join("?" * len(symbols))
    q = f"""
    SELECT t.tradingsymbol,
           MAX(t.volume_traded) AS session_volume,
           AVG(CASE WHEN d.side='buy'  AND d.level=0 THEN d.quantity END) AS avg_bid_qty,
           AVG(CASE WHEN d.side='sell' AND d.level=0 THEN d.quantity END) AS avg_ask_qty,
           AVG(CASE WHEN d.side='buy'  AND d.level=0 THEN d.orders END)  AS avg_bid_orders,
           AVG(CASE WHEN d.side='sell' AND d.level=0 THEN d.orders END)  AS avg_ask_orders
    FROM ticks t
    JOIN tick_depths d ON d.tick_id = t.id
    WHERE t.tradingsymbol IN ({placeholders}) AND t.trade_date = ?
      AND time(t.exchange_timestamp) BETWEEN ? AND ?
    GROUP BY t.tradingsymbol
    """
    df = pd.read_sql_query(q, conn, params=(*symbols, trade_date, MARKET_OPEN, MARKET_CLOSE))
    conn.close()
    return df


# ── UI ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Liquidity Dashboard", layout="wide")
st.title("Liquidity Analysis Dashboard")

# Sidebar
instrument = st.sidebar.selectbox("Instrument", list(INSTRUMENTS.keys()))
cfg = INSTRUMENTS[instrument]

dates = available_dates()
if not dates:
    st.error("No data found in database.")
    st.stop()
trade_date = st.sidebar.selectbox("Date", dates, index=len(dates) - 1)

# Resolve symbols
fut_syms = futures_symbols(cfg["fut_prefix"])
if not fut_syms:
    st.warning(f"No futures data for {instrument}")

# Spot price → ATM strike
spot = spot_last_price(cfg["spot_sym"], trade_date)
if spot:
    atm_strike = round(spot / cfg["strike_gap"]) * cfg["strike_gap"]
    st.sidebar.metric("Spot", f"{spot:,.2f}")
    st.sidebar.metric("ATM Strike", f"{atm_strike:,.0f}")
else:
    atm_strike = None
    st.sidebar.warning("No spot data")

# Option symbols
all_opt_syms = option_symbols_for_date(cfg["opt_prefix"], trade_date)

# Parse into structured list
opt_parsed = []
for s in all_opt_syms:
    p = parse_strike(s, cfg["opt_prefix"])
    if p:
        opt_parsed.append({"symbol": s, "strike": p[0], "type": p[1]})
opt_df = pd.DataFrame(opt_parsed) if opt_parsed else pd.DataFrame(columns=["symbol", "strike", "type"])

# Identify weekly vs monthly
# Weekly options: NIFTY26217... Monthly: NIFTY26FEB...
monthly_pat = re.compile(rf"^{cfg['opt_prefix']}26(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)")
weekly_syms = [s for s in all_opt_syms if not monthly_pat.match(s)]
monthly_syms = [s for s in all_opt_syms if monthly_pat.match(s)]

# Auto-select: show weekly if available, otherwise monthly; let user toggle if both exist
expiry_options = []
if weekly_syms:
    expiry_options.append("Weekly")
if monthly_syms:
    expiry_options.append("Monthly")

if len(expiry_options) > 1:
    expiry_type = st.sidebar.radio("Option Expiry", expiry_options, horizontal=True)
elif expiry_options:
    expiry_type = expiry_options[0]
    st.sidebar.caption(f"Expiry: {expiry_type} (only type available)")
else:
    expiry_type = None
    st.sidebar.warning("No options data")

active_opt_syms = weekly_syms if expiry_type == "Weekly" else monthly_syms if expiry_type == "Monthly" else []

# ── Tabs ────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(["Futures Spread", "Options Spread Smile", "Slippage: FUT vs Synthetic", "Volume & Depth"])

# ── Tab 1: Futures Spread ───────────────────────────────────────────
with tab1:
    if not fut_syms:
        st.info("No futures symbols found.")
    else:
        fut_sel = st.selectbox("Futures Contract", fut_syms, key="fut_sel")
        with st.spinner("Loading futures spread..."):
            df = load_futures_spread(fut_sel, trade_date)
        if df.empty:
            st.warning("No data for this contract/date.")
        else:
            st.subheader(f"{fut_sel} — Bid-Ask Spread")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Avg Spread (pts)", f"{df['spread_pts'].mean():.2f}")
            col2.metric("Avg Spread (bps)", f"{df['spread_bps'].mean():.2f}")
            col3.metric("Median Bid Qty", f"{df['bid_qty'].median():.0f}")
            col4.metric("Median Ask Qty", f"{df['ask_qty'].median():.0f}")

            # Resample to 1-min for smoother chart
            df_m = df.set_index("exchange_timestamp").resample("1min").agg(
                spread_pts=("spread_pts", "mean"),
                spread_bps=("spread_bps", "mean"),
                ofi=("ofi", "mean"),
            ).dropna()

            st.line_chart(df_m["spread_pts"], y_label="Spread (pts)", use_container_width=True)
            st.line_chart(df_m["spread_bps"], y_label="Spread (bps)", use_container_width=True)
            st.line_chart(df_m["ofi"], y_label="Order Flow Imbalance", use_container_width=True)

# ── Tab 2: Options Spread Smile ─────────────────────────────────────
with tab2:
    if atm_strike is None:
        st.warning("Cannot determine ATM — no spot data.")
    elif opt_df.empty:
        st.warning("No option data.")
    else:
        # Filter active expiry options within ±2000 pts of ATM
        active_opt_df = opt_df[opt_df["symbol"].isin(active_opt_syms)]
        nearby = active_opt_df[
            (active_opt_df["strike"] >= atm_strike - 2000)
            & (active_opt_df["strike"] <= atm_strike + 2000)
        ]
        if nearby.empty:
            st.warning("No options near ATM for selected expiry.")
        else:
            with st.spinner("Loading option spreads..."):
                spread_df = load_option_spreads(nearby["symbol"].tolist(), trade_date)
            if spread_df.empty:
                st.warning("No spread data for options.")
            else:
                # Merge strike info
                spread_df = spread_df.merge(
                    nearby[["symbol", "strike", "type"]], left_on="tradingsymbol", right_on="symbol"
                )
                spread_df["distance"] = spread_df["strike"] - atm_strike

                st.subheader("Spread vs Strike Distance from ATM")
                for opt_type in ["CE", "PE"]:
                    sub = spread_df[spread_df["type"] == opt_type].sort_values("distance")
                    if sub.empty:
                        continue
                    st.markdown(f"**{opt_type}**")
                    chart_data = sub.set_index("distance")[["avg_spread"]].rename(columns={"avg_spread": f"{opt_type} Spread (pts)"})
                    st.line_chart(chart_data, use_container_width=True)

                # BPS view
                st.subheader("Spread (bps) vs Strike Distance")
                for opt_type in ["CE", "PE"]:
                    sub = spread_df[spread_df["type"] == opt_type].sort_values("distance")
                    if sub.empty:
                        continue
                    st.markdown(f"**{opt_type}**")
                    chart_data = sub.set_index("distance")[["avg_spread_bps"]].rename(columns={"avg_spread_bps": f"{opt_type} Spread (bps)"})
                    st.line_chart(chart_data, use_container_width=True)

# ── Tab 3: Slippage FUT vs Synthetic ────────────────────────────────
with tab3:
    if atm_strike is None or opt_df.empty or not fut_syms:
        st.warning("Need futures + options + spot data for this tab.")
    else:
        near_fut = fut_syms[0]
        # Find ATM CE and PE in active expiry
        active_opt_df = opt_df[opt_df["symbol"].isin(active_opt_syms)]
        atm_ce = active_opt_df[(active_opt_df["strike"] == atm_strike) & (active_opt_df["type"] == "CE")]
        atm_pe = active_opt_df[(active_opt_df["strike"] == atm_strike) & (active_opt_df["type"] == "PE")]

        if atm_ce.empty or atm_pe.empty:
            st.warning(f"ATM CE/PE not found at strike {atm_strike} for {expiry_type} expiry.")
        else:
            ce_sym = atm_ce.iloc[0]["symbol"]
            pe_sym = atm_pe.iloc[0]["symbol"]
            st.caption(f"FUT: {near_fut} | CE: {ce_sym} | PE: {pe_sym}")

            with st.spinner("Loading slippage data..."):
                slip = load_slippage_minute(near_fut, ce_sym, pe_sym, trade_date)
            if slip.empty:
                st.warning("No slippage data.")
            else:
                lot = cfg["lot"]
                slip["fut_cost"] = slip["fut_half_spread"] * lot
                slip["syn_cost"] = slip["syn_half_spread"] * lot
                slip["ratio"] = slip["fut_cost"] / slip["syn_cost"].replace(0, float("nan"))

                st.subheader("Per-Minute Half-Spread Cost (₹ per lot)")
                cost_chart = slip.set_index("minute")[["fut_cost", "syn_cost"]]
                cost_chart.columns = ["Futures", "Synthetic"]
                st.bar_chart(cost_chart, use_container_width=True)

                st.subheader("Cumulative Cost")
                cum = slip.set_index("minute")[["fut_cost", "syn_cost"]].cumsum()
                cum.columns = ["Futures (cum)", "Synthetic (cum)"]
                st.line_chart(cum, use_container_width=True)

                st.subheader("Cost Ratio (FUT / Synthetic)")
                ratio_chart = slip.set_index("minute")[["ratio"]]
                ratio_chart.columns = ["Ratio"]
                st.line_chart(ratio_chart, use_container_width=True)

                col1, col2, col3 = st.columns(3)
                col1.metric("Avg FUT cost/lot", f"₹{slip['fut_cost'].mean():.1f}")
                col2.metric("Avg SYN cost/lot", f"₹{slip['syn_cost'].mean():.1f}")
                col3.metric("Avg Ratio", f"{slip['ratio'].mean():.1f}x")

# ── Tab 4: Volume & Depth ──────────────────────────────────────────
with tab4:
    syms_to_check = []
    labels = []
    if fut_syms:
        syms_to_check.append(fut_syms[0])
        labels.append(f"{fut_syms[0]} (Near FUT)")
    if atm_strike is not None and not opt_df.empty:
        active_opt_df = opt_df[opt_df["symbol"].isin(active_opt_syms)]
        atm_ce = active_opt_df[(active_opt_df["strike"] == atm_strike) & (active_opt_df["type"] == "CE")]
        atm_pe = active_opt_df[(active_opt_df["strike"] == atm_strike) & (active_opt_df["type"] == "PE")]
        if not atm_ce.empty:
            syms_to_check.append(atm_ce.iloc[0]["symbol"])
            labels.append(f"{atm_ce.iloc[0]['symbol']} (ATM CE)")
        if not atm_pe.empty:
            syms_to_check.append(atm_pe.iloc[0]["symbol"])
            labels.append(f"{atm_pe.iloc[0]['symbol']} (ATM PE)")

    if not syms_to_check:
        st.warning("No symbols to analyze.")
    else:
        with st.spinner("Loading depth stats..."):
            stats = load_depth_stats(syms_to_check, trade_date)
        if stats.empty:
            st.warning("No depth data.")
        else:
            # Map labels
            label_map = dict(zip(syms_to_check, labels))
            stats["label"] = stats["tradingsymbol"].map(label_map)
            display = stats[["label", "session_volume", "avg_bid_qty", "avg_ask_qty", "avg_bid_orders", "avg_ask_orders"]].copy()
            display.columns = ["Symbol", "Session Volume", "Avg Bid Qty", "Avg Ask Qty", "Avg Bid Orders", "Avg Ask Orders"]
            for col in display.columns[1:]:
                display[col] = display[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
            st.subheader("Depth & Volume Summary")
            st.dataframe(display.set_index("Symbol"), use_container_width=True)
