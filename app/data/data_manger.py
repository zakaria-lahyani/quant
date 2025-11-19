from typing import Optional
import pandas as pd

from app.data.backtest_data import BacktestDataSource
from app.data.live_data import LiveDataSource


class DataSourceManager:
    """Manager class to handle different data source modes"""

    def __init__(self, mode: str, **kwargs):
        """
        Initialize data source based on mode

        Args:
            mode: "live" or "backtest"
            **kwargs: Additional arguments based on mode
                For live mode: client, date_helper
                For backtest mode: data_path, symbol
        """
        self.mode = mode

        if mode == "live":
            if "client" not in kwargs or "date_helper" not in kwargs:
                raise ValueError("Live mode requires 'client' and 'date_helper' parameters")
            self.data_source = LiveDataSource(kwargs["client"], kwargs["date_helper"])

        elif mode == "backtest":
            if "data_path" not in kwargs or "symbol" not in kwargs:
                raise ValueError("Backtest mode requires 'data_path' and 'symbol' parameters")
            self.data_source = BacktestDataSource(kwargs["data_path"], kwargs["symbol"])

        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'live' or 'backtest'")

    def get_historical_data(self, symbol: str, timeframe: str ) -> pd.DataFrame:
        """Get historical data from the appropriate source"""
        return self.data_source.get_historical_data(symbol, timeframe )

    def get_stream_data(self, symbol: str, timeframe: str, nbr_bars: int = 3) -> pd.DataFrame:
        """Get streaming data from the appropriate source"""
        return self.data_source.get_stream_data(symbol, timeframe, nbr_bars)

    def load_backtest_data(self, timeframes: list):
        """Load backtest data if in backtest mode"""
        if self.mode == "backtest" and hasattr(self.data_source, 'load_data'):
            self.data_source.load_data(timeframes)

    def reset_backtest_index(self, timeframe: Optional[str] = None):
        """Reset backtest index if in backtest mode"""
        if self.mode == "backtest" and hasattr(self.data_source, 'reset_index'):
            self.data_source.reset_index(timeframe)

