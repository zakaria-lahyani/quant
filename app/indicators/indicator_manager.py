import pandas as pd
from typing import Dict

from app.indicators.indicator_factory import IndicatorFactory


class IndicatorManager:
    """
    Orchestrates the computation of multiple technical indicators over historical or live market data.

    Attributes:
        original_historical (pd.DataFrame): Original market data before indicators are applied.
        handlers (Dict[str, IndicatorHandler]): Dictionary of handlers created from config.
        historical_data (pd.DataFrame): DataFrame with computed indicators.
    """

    def __init__(self, historical_data: pd.DataFrame, config: Dict[str, dict], is_bulk: bool):
        """
        Initializes the manager and computes indicators on the data.

        Args:
            historical_data (pd.DataFrame): The input market data.
            config (Dict[str, dict]): A configuration dictionary for the indicators.
            is_bulk (bool): Whether to use bulk computation (vectorized) or row-wise.
        """
        self.original_historical = historical_data.copy()
        self.handlers = IndicatorFactory(config).create_handlers()
        if is_bulk:
            self.historical_data = self.bulk_compute()
        else:
            self.historical_data = self.warmup_historical()

    def warmup_historical(self) -> pd.DataFrame:
        """
        Applies all indicator handlers row-by-row (slower but flexible).

        Returns:
            pd.DataFrame: Data with indicator columns added.
        """
        data = self.original_historical.copy()
        result_rows = []
        for _, row in data.iterrows():
            for handler in self.handlers.values():
                row = handler.compute(row)
            result_rows.append(row)
        return pd.DataFrame(result_rows)

    def bulk_compute(self) -> pd.DataFrame:
        """
        Applies all indicator handlers using their bulk methods (faster).

        Returns:
            pd.DataFrame: Data with indicator columns added.
        """
        data = self.original_historical.copy()
        for handler in self.handlers.values():
            data = handler.bulk_compute(data)
        return data

    def get_historical_data(self) -> pd.DataFrame:
        """
        Returns the processed DataFrame with all indicators.

        Returns:
            pd.DataFrame: The historical data with indicators.
        """
        return self.historical_data

    def compute_indicators(self, row: pd.Series) -> pd.Series:
        """
        Compute indicators for a single row (e.g., for live or streaming data).

        Args:
            row (pd.Series): Input data row.

        Returns:
            pd.Series: Row with computed indicator values.
        """
        for handler in self.handlers.values():
            row = handler.compute(row)
        return row
