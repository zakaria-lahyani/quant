from abc import ABC, abstractmethod
import pandas as pd

class DataSourceInterface(ABC):
    """Abstract interface for data sources"""

    @abstractmethod
    def get_historical_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """Get historical data for a timeframe"""
        pass

    @abstractmethod
    def get_stream_data(self, symbol: str, timeframe: str, nbr_bars: int = 3) -> pd.DataFrame:
        """Get streaming/recent data"""
        pass
