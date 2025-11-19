from pathlib import Path
from typing import Optional, Dict
import pandas as pd

from app.data.data_interface import DataSourceInterface


class BacktestDataSource(DataSourceInterface):
    """Data source for backtesting using parquet files"""

    def __init__(self, data_path: str, symbol: str):
        self.data_path = Path(data_path)
        self.symbol = symbol.lower()
        self.historical_data: Dict[str, pd.DataFrame] = {}
        self.current_index: Dict[str, int] = {}

    def load_data(self, timeframes: list):
        """Load all parquet files for the specified timeframes"""

        for tf in timeframes:
            file_path = self.data_path / f"{self.symbol}_{tf}.parquet"
            print(f"Looking for file: {file_path}")
            
            if file_path.exists():
                df = pd.read_parquet(file_path)
                df["time"] = pd.to_datetime(df["time"])
                self.historical_data[tf] = df
                self.current_index[tf] = 0
                print(f"Loaded {len(df)} rows for timeframe {tf}")
            else:
                print(f"WARNING: Parquet file not found for timeframe {tf}: {file_path}")

    def get_historical_data(self, symbol: str, timeframe: str) -> pd.DataFrame:
        """Get historical data from parquet file within date range"""

        if timeframe not in self.historical_data:
            print(f"No data loaded for timeframe {timeframe}")
            print(f"Available timeframes: {list(self.historical_data.keys())}")
            return pd.DataFrame()

        df = self.historical_data[timeframe].copy()
        print(f"Total rows in loaded data: {len(df)}")

        return df

    def get_stream_data(self, symbol: str, timeframe: str, nbr_bars: int = 3) -> pd.DataFrame:
        """Simulate streaming data by returning next bars from parquet"""
        if timeframe not in self.historical_data:
            return pd.DataFrame()

        df = self.historical_data[timeframe]
        current_idx = self.current_index.get(timeframe, 0)

        # Get the next nbr_bars from current position
        end_idx = min(current_idx + nbr_bars, len(df))
        result = df.iloc[current_idx:end_idx].copy()

        # Update index for next call
        self.current_index[timeframe] = end_idx

        return result.reset_index(drop=True)

    def reset_index(self, timeframe: Optional[str] = None):
        """Reset the current index for streaming simulation"""
        if timeframe:
            self.current_index[timeframe] = 0
        else:
            for tf in self.current_index:
                self.current_index[tf] = 0

