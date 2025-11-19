import pandas as pd
from typing import Dict, List, Callable, Any, Tuple, Union

from app.indicators.registry import INDICATOR_CONFIG


class IndicatorHandler:
    """
    Handles the application of technical indicators to data rows or DataFrames.
    Uses a configuration-driven approach to eliminate code duplication.
    """

    # Configuration mapping for each indicator type
    def __init__(self, name: str, indicator):
        """
        Initialize the IndicatorHandler.

        Args:
            name: Full indicator name (e.g., 'macd_1h')
            indicator: The indicator instance
        """
        self.name = name
        self.base_name = name.split('_')[0]
        self.indicator = indicator
        self.config = INDICATOR_CONFIG.get(self.base_name)

        if not self.config:
            print(f"No configuration found for indicator: {self.base_name}")

    def compute(self, row: pd.Series) -> pd.Series:
        """
        Apply the indicator to a single row of data.

        Args:
            row: A pandas Series representing a single data row

        Returns:
            The row with indicator values added

        Raises:
            KeyError: If required fields are missing from the row (CRASHES THE APP)
        """
        row = row.copy()

        if not self.config:
            return row

        return self._apply_indicator(
            data=row,
            is_bulk=False,
            update_method='update'
        )

    def bulk_compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the indicator to an entire DataFrame.

        Args:
            df: A pandas DataFrame with market data

        Returns:
            The DataFrame with indicator columns added

        Raises:
            KeyError: If required fields are missing from the DataFrame (CRASHES THE APP)
        """
        df = df.copy()

        if not self.config:
            return df

        return self._apply_indicator(
            data=df,
            is_bulk=True,
            update_method='batch_update'
        )

    def _apply_indicator(self, data: Union[pd.Series, pd.DataFrame],
                        is_bulk: bool, update_method: str) -> Union[pd.Series, pd.DataFrame]:
        """
        Core method that applies the indicator using configuration.

        NO ERROR HANDLING - CRASHES ON ANY ERROR INCLUDING MISSING FIELDS!

        Args:
            data: Input data (Series for single row, DataFrame for bulk)
            is_bulk: Whether this is bulk processing
            update_method: Method name to call on the indicator

        Returns:
            Data with indicator outputs added

        Raises:
            KeyError: If required fields are missing (CRASHES THE APP)
            Exception: Any other error during indicator computation (CRASHES THE APP)
        """
        # Get inputs based on configuration - will crash if fields missing
        input_func = self.config['bulk_inputs'] if is_bulk else self.config['inputs']
        inputs = input_func(data)

        # Call the indicator method - will crash if indicator fails
        indicator_method = getattr(self.indicator, update_method)
        result = indicator_method(*inputs)

        # Ensure result is a tuple
        if not isinstance(result, tuple):
            result = (result,)

        # Get output column names
        output_names = self.config['outputs'](self.name)

        # Handle case where indicator returns fewer outputs than expected
        result = self._pad_result(result, len(output_names))

        # Assign outputs to data
        self._assign_outputs(data, output_names, result)

        return data

    def _pad_result(self, result: Tuple, expected_length: int) -> List:
        """
        Pad the result tuple with None values if it's shorter than expected.

        Args:
            result: The result tuple from the indicator
            expected_length: Expected number of outputs

        Returns:
            Padded result list
        """
        result_list = list(result)
        if len(result_list) < expected_length:
            result_list.extend([None] * (expected_length - len(result_list)))
        return result_list

    def _assign_outputs(self, data: Union[pd.Series, pd.DataFrame],
                       output_names: List[str], result: List) -> None:
        """
        Assign indicator outputs to the data structure.

        Args:
            data: The data structure to modify
            output_names: List of output column names
            result: List of result values
        """
        for col_name, value in zip(output_names, result):
            data[col_name] = value

    def get_output_columns(self) -> List[str]:
        """
        Get the list of output column names for this indicator.

        Returns:
            List of output column names
        """
        if not self.config:
            return []
        return self.config['outputs'](self.name)

    def is_supported(self) -> bool:
        """
        Check if this indicator type is supported.

        Returns:
            True if supported, False otherwise
        """
        return self.config is not None

    @classmethod
    def add_indicator_config(cls, base_name: str, config: Dict) -> None:
        """
        Add configuration for a new indicator type.

        Args:
            base_name: The base name of the indicator
            config: Configuration dictionary with 'inputs', 'bulk_inputs', and 'outputs'
        """
        INDICATOR_CONFIG[base_name] = config
        print(f"Added configuration for indicator: {base_name}")

    @classmethod
    def get_supported_indicators(cls) -> List[str]:
        """
        Get list of all supported indicator types.

        Returns:
            List of supported indicator base names
        """
        return list(INDICATOR_CONFIG.keys())
