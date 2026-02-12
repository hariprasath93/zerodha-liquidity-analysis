"""
Instrument management module.

Downloads the full instrument list from Zerodha, filters for the
requested symbol's derivatives (options CE/PE and futures FUT:
weekly + monthly expiries) and distributes instrument tokens across
multiple WebSocket connections.
"""

import logging
from datetime import date
from typing import Optional

import pandas as pd
import pytz
from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")


class InstrumentManager:
    """Fetches, filters, and distributes instruments for subscription."""

    def __init__(self, kite: KiteConnect, config: dict):
        self.kite = kite
        self.exchange = config.get("exchange", "NFO")
        self.strike_range_pct = config.get("strike_range_pct", None)
        self.instrument_types = config.get("instrument_types", ["CE", "PE"])
        self.include_underlying = config.get("include_underlying", True)
        self.underlying_exchange = config.get("underlying_exchange", "NSE")

        expiry_cfg = config.get("expiry_filter", {})
        self.weekly_expiries_count = expiry_cfg.get("weekly_expiries", 2)
        self.monthly_expiries_count = expiry_cfg.get("monthly_expiries", 2)

        self._instruments_df: Optional[pd.DataFrame] = None
        self._token_symbol_map: dict = {}

    def fetch_instruments(self) -> pd.DataFrame:
        """Download the full instrument dump from Zerodha."""
        logger.info("Downloading instrument list from Zerodha...")
        instruments = self.kite.instruments()
        self._instruments_df = pd.DataFrame(instruments)
        logger.info("Downloaded %d instruments", len(self._instruments_df))
        return self._instruments_df

    def get_instruments_for_symbols(self, symbols: list[str]) -> list:
        """
        Get all instruments (derivatives + spot) for multiple symbols.
        Aggregates CE/PE/FUT for current+next weekly and monthly expiries,
        plus underlying spot for each symbol. Distributes across sockets
        when used with distribute_tokens().
        """
        if self._instruments_df is None:
            self.fetch_instruments()

        all_instruments = []
        for symbol in symbols:
            derivatives = self.get_derivative_tokens(symbol)
            all_instruments.extend(derivatives)
            underlying = self.get_underlying_token(symbol)
            if underlying:
                all_instruments.append(underlying)

        logger.info(
            "Total instruments for %s: %d",
            ", ".join(symbols),
            len(all_instruments),
        )
        return all_instruments

    def get_derivative_tokens(self, symbol: str) -> list:
        """Filter instruments for the given symbol's derivatives (CE/PE/FUT, current+next week, current+next month)."""
        if self._instruments_df is None:
            self.fetch_instruments()

        df = self._instruments_df
        today = date.today()

        deriv_df = df[
            (df["exchange"] == self.exchange)
            & (df["name"] == symbol)
            & (df["instrument_type"].isin(self.instrument_types))
        ].copy()

        deriv_df["expiry"] = pd.to_datetime(deriv_df["expiry"]).dt.date
        deriv_df = deriv_df[deriv_df["expiry"] >= today]

        logger.info("Found %d derivative contracts for %s", len(deriv_df), symbol)

        unique_expiries = sorted(deriv_df["expiry"].unique())
        selected_expiries = self._select_target_expiries(unique_expiries, today)
        deriv_df = deriv_df[deriv_df["expiry"].isin(selected_expiries)]
        logger.info("Filtered to %d target expiries: %s", len(selected_expiries), [str(e) for e in sorted(selected_expiries)])

        # Strike filter applies only to options (CE/PE); futures (FUT) have no strike
        if self.strike_range_pct is not None:
            spot_price = self._get_spot_price(symbol)
            if spot_price:
                lower = spot_price * (1 - self.strike_range_pct / 100)
                upper = spot_price * (1 + self.strike_range_pct / 100)
                options_mask = deriv_df["instrument_type"].isin(["CE", "PE"])
                strike_ok = (deriv_df["strike"] >= lower) & (deriv_df["strike"] <= upper)
                deriv_df = deriv_df[~options_mask | strike_ok]

        instruments_list = deriv_df.to_dict("records")
        for inst in instruments_list:
            self._token_symbol_map[inst["instrument_token"]] = {
                "tradingsymbol": inst["tradingsymbol"],
                "expiry": str(inst["expiry"]),
                "strike": inst["strike"],
                "instrument_type": inst["instrument_type"],
                "name": symbol,
            }

        logger.info("Total derivative instruments to subscribe: %d", len(instruments_list))
        return instruments_list

    def _select_target_expiries(self, unique_expiries: list, today: date) -> set:
        """Select current+next week and current+next month expiries."""
        if not unique_expiries:
            return set()
        selected = set()
        weekly = unique_expiries[: self.weekly_expiries_count]
        selected.update(weekly)
        expiries_by_month = {}
        for exp in unique_expiries:
            key = (exp.year, exp.month)
            expiries_by_month.setdefault(key, []).append(exp)
        target_months = []
        y, m = today.year, today.month
        for _ in range(self.monthly_expiries_count):
            target_months.append((y, m))
            m += 1
            if m > 12:
                m, y = 1, y + 1
        monthly_selected = []
        for month_key in target_months:
            if month_key in expiries_by_month:
                monthly_expiry = max(expiries_by_month[month_key])
                if monthly_expiry >= today:
                    monthly_selected.append(monthly_expiry)
        selected.update(monthly_selected)
        return selected

    def get_underlying_token(self, symbol: str) -> Optional[dict]:
        """Get the underlying index/equity instrument token."""
        if not self.include_underlying:
            return None
        if self._instruments_df is None:
            self.fetch_instruments()
        df = self._instruments_df
        spot_symbol_map = {
            "NIFTY": "NIFTY 50",
            "BANKNIFTY": "NIFTY BANK",
            "FINNIFTY": "NIFTY FIN SERVICE",
            "MIDCPNIFTY": "NIFTY MID SELECT",
            "SENSEX": "SENSEX",
        }
        spot_name = spot_symbol_map.get(symbol, symbol)
        underlying = df[
            (df["exchange"] == self.underlying_exchange)
            & (df["tradingsymbol"] == spot_name)
            & (df["instrument_type"] == "EQ")
        ]
        if underlying.empty:
            underlying = df[
                (df["exchange"] == self.underlying_exchange)
                & (df["name"] == spot_name)
            ]
        if underlying.empty:
            logger.warning("Could not find underlying for %s", symbol)
            return None
        inst = underlying.iloc[0].to_dict()
        self._token_symbol_map[inst["instrument_token"]] = {
            "tradingsymbol": inst["tradingsymbol"],
            "name": symbol,
            "instrument_type": "INDEX",
            "expiry": None,
            "strike": None,
        }
        return inst

    def _get_spot_price(self, symbol: str) -> Optional[float]:
        try:
            spot_symbol_map = {
                "NIFTY": "NSE:NIFTY 50",
                "BANKNIFTY": "NSE:NIFTY BANK",
                "FINNIFTY": "NSE:NIFTY FIN SERVICE",
                "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
                "SENSEX": "BSE:SENSEX",
            }
            exchange_symbol = spot_symbol_map.get(symbol, f"NSE:{symbol}")
            ltp_data = self.kite.ltp([exchange_symbol])
            if ltp_data:
                key = list(ltp_data.keys())[0]
                return ltp_data[key]["last_price"]
        except Exception as e:
            logger.warning("Could not fetch spot price for %s: %s", symbol, e)
        return None

    def distribute_tokens(self, instruments: list, num_sockets: int = 3) -> list:
        """Distribute instrument tokens across N WebSocket connections."""
        num_sockets = min(num_sockets, 3)
        tokens = [inst["instrument_token"] for inst in instruments]
        if not tokens:
            return [[] for _ in range(num_sockets)]
        buckets = [[] for _ in range(num_sockets)]
        for i, token in enumerate(tokens):
            buckets[i % num_sockets].append(token)
        for idx, bucket in enumerate(buckets):
            logger.info("Socket %d: %d instruments", idx, len(bucket))
        return buckets

    def get_symbol_for_token(self, instrument_token: int) -> dict:
        return self._token_symbol_map.get(
            instrument_token,
            {"tradingsymbol": f"UNKNOWN_{instrument_token}", "name": "UNKNOWN"},
        )

    def get_token_symbol_map(self) -> dict:
        """Return token -> {tradingsymbol, name, ...} for all subscribed instruments (for tick enrichment)."""
        return dict(self._token_symbol_map)
