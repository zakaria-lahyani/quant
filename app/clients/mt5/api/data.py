"""
Data client for MT5 API - historical data and market data.
"""

from datetime import datetime
from typing import  Dict, List, Optional

from app.clients.mt5.base import BaseClient
from app.clients.mt5.models.response import HistoricalBar
from app.clients.mt5.utils import validate_symbol, format_datetime, parse_datetime


class DataClient(BaseClient):
    """Client for retrieving historical and market data from MT5."""

    def fetch_bars(
            self,
            symbol: str,
            timeframe: str = "M1",
            num_bars: Optional[int] = 10,
            start: Optional[datetime] = None,
            end: Optional[datetime] = None,
    ) -> List[HistoricalBar]:
        """
        Fetch historical price bars for a symbol.

        This method can be used in two ways:
        1. **Latest bars** mode: Provide `num_bars` to fetch the most recent bars.
        2. **Time range** mode: Provide `start` and `end` timestamps to fetch bars in that range.

        Args:
            symbol: Symbol name to fetch data for
            timeframe: Timeframe for the data (e.g., M1, M5, H1)
            num_bars: Number of bars to fetch (default: 10)
            start: Start datetime
            end: End datetime

        Returns:
            List of price bars with time, open, high, low, close, and volume data

        Raises:
            MT5APIError: If parameters are invalid or data fetching fails
        """
        symbol = validate_symbol(symbol)

        params = {
            'timeframe': timeframe,
        }

        if start or end:
            # Time range mode
            if start:
                params['start'] = format_datetime(start)
            if end:
                params['end'] = format_datetime(end)
        else:
            # Latest bars mode
            params['num_bars'] = num_bars

        data = self.get(f"symbols/{symbol}/bars", params=params)

        bars = []
        if data:
            for bar_data in data:
                # Parse datetime if it's a string
                if 'time' in bar_data and isinstance(bar_data['time'], str):
                    bar_data['time'] = parse_datetime(bar_data['time'])
                bars.append(HistoricalBar(**bar_data))

        return bars

    def get_latest_bars(
            self,
            symbol: str,
            timeframe: str = "M1",
            count: int = 10,
    ) -> List[HistoricalBar]:
        """
        Get the latest price bars for a symbol.

        Args:
            symbol: Symbol name
            timeframe: Timeframe (e.g., M1, M5, H1)
            count: Number of bars to fetch

        Returns:
            List of latest price bars
        """
        return self.fetch_bars(symbol=symbol, timeframe=timeframe, num_bars=count)

    def get_bars_range(
            self,
            symbol: str,
            timeframe: str,
            start: str,
            end: str,
    ) -> List[HistoricalBar]:
        """
        Get price bars for a specific time range.

        Args:
            symbol: Symbol name
            timeframe: Timeframe (e.g., M1, M5, H1)
            start: Start datetime
            end: End datetime

        Returns:
            List of price bars in the specified range
        """
        start_dt = format_datetime(start)
        end_dt = format_datetime(end)

        return self.fetch_bars(symbol=symbol, timeframe=timeframe, start=start_dt, end=end_dt)

    def get_ohlc_data(
            self,
            symbol: str,
            timeframe: str = "M1",
            num_bars: int = 10,
    ) -> Dict[str, List[float]]:
        """
        Get OHLC data in a structured format.

        Args:
            symbol: Symbol name
            timeframe: Timeframe
            num_bars: Number of bars to fetch

        Returns:
            Dictionary with 'open', 'high', 'low', 'close', 'volume' lists
        """
        bars = self.get_latest_bars(symbol, timeframe, num_bars)

        return {
            'time': [bar.time for bar in bars],
            'open': [bar.open for bar in bars],
            'high': [bar.high for bar in bars],
            'low': [bar.low for bar in bars],
            'close': [bar.close for bar in bars],
            'volume': [bar.tick_volume for bar in bars],
        }

    def get_current_price(self, symbol: str) -> Dict[str, float]:
        """
        Get current price from the latest bar.

        Args:
            symbol: Symbol name

        Returns:
            Dictionary with current OHLC prices
        """
        bars = self.get_latest_bars(symbol, "M1", 1)
        if not bars:
            return {}

        latest_bar = bars[0]
        return {
            'open': latest_bar.open,
            'high': latest_bar.high,
            'low': latest_bar.low,
            'close': latest_bar.close,
            'volume': latest_bar.tick_volume,
            'time': latest_bar.time,
        }

