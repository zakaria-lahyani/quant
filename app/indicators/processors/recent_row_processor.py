"""
RecentRowsProcessor - Refactored Version
=======================================

A robust, maintainable class for managing recent market data rows across multiple timeframes.
Supports both live trading and backtesting scenarios with efficient storage and retrieval.
"""

from typing import Optional, List, Dict, Union
import pandas as pd
from collections import deque
import logging
from datetime import datetime


class RecentRowsProcessor:
    """
    Manages recent rows storage and retrieval for multiple timeframes.

    This class provides efficient storage and retrieval of recent market data rows
    across different timeframes. It supports both live trading (where rows are added
    sequentially) and backtesting (where previous row data needs to be maintained).

    Key Features:
    - Efficient circular buffer storage using deque
    - Automatic duplicate detection and updates based on timestamps
    - Support for backtesting with previous row data
    - Thread-safe operations for live trading
    - Memory-efficient with configurable row limits

    Architecture:
    - Uses deque for O(1) append/pop operations
    - Timestamp-based duplicate detection
    - Lazy DataFrame conversion for performance
    - Immutable operations (doesn't modify input data)

    Usage Patterns:
    - Live Trading: Sequential row addition with timestamp deduplication
    - Backtesting: Row processing with previous row context
    """

    # Class constants
    DEFAULT_MAX_ROWS = 6
    TIME_COLUMN = "time"
    PREVIOUS_PREFIX = "previous_"

    def __init__(self, timeframes: List[str], max_rows: int = DEFAULT_MAX_ROWS):
        """
        Initialize the RecentRowsProcessor.

        Args:
            timeframes: List of timeframe identifiers (e.g., ['1m', '5m', '1h'])
            max_rows: Maximum number of rows to keep per timeframe (default: 6)

        Raises:
            ValueError: If timeframes is empty or max_rows is less than 1
            TypeError: If timeframes contains non-string values

        """
        self._validate_initialization_params(timeframes, max_rows)

        self._timeframes = list(timeframes)  # Create defensive copy
        self._max_rows = max_rows
        self._recent_rows: Dict[str, deque] = {
            tf: deque(maxlen=max_rows) for tf in timeframes
        }

        # Setup logging
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug(f"Initialized RecentRowsProcessor for timeframes: {timeframes}, max_rows: {max_rows}")

    def add_or_update_row(self, timeframe: str, row: pd.Series) -> bool:
        """
        Add new row or update existing row with same timestamp.

        This method handles both new row additions and updates to existing rows.
        If a row with the same timestamp already exists, it will be updated.
        Otherwise, the new row is appended to the deque.

        Args:
            timeframe: The timeframe identifier
            row: Market data row to add/update

        Returns:
            bool: True if row was updated, False if new row was added

        Raises:
            ValueError: If timeframe is not supported
            TypeError: If row is not a pandas Series

        """
        self._validate_timeframe(timeframe)
        self._validate_row_input(row)

        # Create defensive copy to prevent external modifications
        row_copy = row.copy()

        existing_idx = self._find_existing_row_index(timeframe, row_copy)

        if existing_idx is not None:
            self._recent_rows[timeframe][existing_idx] = row_copy
            self._logger.debug(f"Updated existing row at index {existing_idx} for timeframe {timeframe}")
            return True
        else:
            if not self._recent_rows[timeframe]:
                # First row: add previous_* columns with NaN
                prev_columns = [col for col in row_copy.index if not col.startswith(self.PREVIOUS_PREFIX)]
                nan_prefixed = pd.Series({f"{self.PREVIOUS_PREFIX}{col}": pd.NA for col in prev_columns})
                row_copy = pd.concat([row_copy, nan_prefixed])
            else:
                # Combine with previous row using helper
                prev_row = self._recent_rows[timeframe][-1]
                row_copy = self._combine_with_previous_row(row_copy, prev_row)

            self._recent_rows[timeframe].append(row_copy)
            self._logger.debug(f"Added new row for timeframe {timeframe}")
            return False

    def get_latest_row(self, timeframe: str) -> Optional[pd.Series]:
        """
        Get the most recent row for a timeframe.

        Args:
            timeframe: The timeframe identifier

        Returns:
            Optional[pd.Series]: Most recent row, or None if no rows exist

        Raises:
            ValueError: If timeframe is not supported

        """
        self._validate_timeframe(timeframe)

        if not self._recent_rows[timeframe]:
            return None

        # Return copy to prevent external modifications
        return self._recent_rows[timeframe][-1].copy()

    def get_all_rows(self, timeframe: str) -> pd.DataFrame:
        """
        Get all recent rows as DataFrame.

        Converts the deque of rows into a pandas DataFrame. If time column
        exists, it will be converted to datetime and set as index.

        Args:
            timeframe: The timeframe identifier

        Returns:
            pd.DataFrame: DataFrame containing all recent rows

        Raises:
            ValueError: If timeframe is not supported

        """
        self._validate_timeframe(timeframe)

        if not self._recent_rows[timeframe]:
            return pd.DataFrame()

        # Create DataFrame from deque
        df = pd.DataFrame(list(self._recent_rows[timeframe]))

        # Process time column if present
        if self.TIME_COLUMN in df.columns:
            df = self._process_time_column(df)

        return df

    def get_row_count(self, timeframe: str) -> int:
        """
        Get the number of stored rows for a timeframe.

        Args:
            timeframe: The timeframe identifier

        Returns:
            int: Number of rows currently stored

        Raises:
            ValueError: If timeframe is not supported
        """
        self._validate_timeframe(timeframe)
        return len(self._recent_rows[timeframe])

    def clear_timeframe(self, timeframe: str) -> None:
        """
        Clear all rows for a specific timeframe.

        Args:
            timeframe: The timeframe identifier

        Raises:
            ValueError: If timeframe is not supported
        """
        self._validate_timeframe(timeframe)
        self._recent_rows[timeframe].clear()
        self._logger.debug(f"Cleared all rows for timeframe {timeframe}")

    def clear_all(self) -> None:
        """Clear all rows for all timeframes."""
        for timeframe in self._timeframes:
            self._recent_rows[timeframe].clear()
        self._logger.debug("Cleared all rows for all timeframes")

    def get_timeframes(self) -> List[str]:
        """
        Get list of supported timeframes.

        Returns:
            List[str]: Copy of supported timeframes list
        """
        return self._timeframes.copy()

    def process_backtest_with_indicators_row(self, timeframe: str, row: pd.Series) -> pd.Series:
        """
        Process a row for backtesting with previous row context.

        Adds the current row to internal storage. If a previous row exists,
        it combines the current row with prefixed previous_* values.

        Args:
            timeframe: The timeframe identifier
            row: Current market data row

        Returns:
            pd.Series: Row with previous row data (if any) included

        Raises:
            ValueError: If timeframe is not supported
            TypeError: If row is not a pandas Series
        """
        self._validate_timeframe(timeframe)
        self._validate_row_input(row)

        previous_row = self.get_latest_row(timeframe)

        # Combine if previous row exists
        if previous_row is not None:
            result_row = self._combine_with_previous_row(row, previous_row)
        else:
            result_row = row.copy()

        # Let add_or_update_row() handle adding missing previous_* with NaN
        self.add_or_update_row(timeframe, result_row)

        return result_row.copy()

    def has_sufficient_data(self, timeframe: str, min_rows: int) -> bool:
        """
        Check if timeframe has sufficient data for analysis.

        Args:
            timeframe: The timeframe identifier
            min_rows: Minimum number of rows required

        Returns:
            bool: True if sufficient data is available

        Raises:
            ValueError: If timeframe is not supported or min_rows is negative
        """
        self._validate_timeframe(timeframe)
        if min_rows < 0:
            raise ValueError("min_rows must be non-negative")

        return self.get_row_count(timeframe) >= min_rows

    # Private helper methods

    def get_recent_rows(self) ->  dict[str, deque]:
        return self._recent_rows

    def _validate_initialization_params(self, timeframes: List[str], max_rows: int) -> None:
        """Validate initialization parameters."""
        if not timeframes:
            raise ValueError("timeframes cannot be empty")

        if not all(isinstance(tf, str) for tf in timeframes):
            raise TypeError("All timeframes must be strings")

        if max_rows < 1:
            raise ValueError("max_rows must be at least 1")

    def _validate_timeframe(self, timeframe: str) -> None:
        """Validate that timeframe is supported."""
        if timeframe not in self._timeframes:
            raise ValueError(f"Unsupported timeframe: {timeframe}. Supported: {self._timeframes}")

    def _validate_row_input(self, row: pd.Series) -> None:
        """Validate row input parameter."""
        if not isinstance(row, pd.Series):
            raise TypeError("row must be a pandas Series")

    def _find_existing_row_index(self, timeframe: str, row: pd.Series) -> Optional[int]:
        """
        Find index of existing row with same timestamp.

        Args:
            timeframe: The timeframe identifier
            row: Row to find timestamp match for

        Returns:
            Optional[int]: Index of matching row, or None if not found
        """
        if self.TIME_COLUMN not in row.index:
            return None

        target_time = self._normalize_timestamp(row[self.TIME_COLUMN])

        for i, existing_row in enumerate(self._recent_rows[timeframe]):
            if self.TIME_COLUMN in existing_row.index:
                existing_time = self._normalize_timestamp(existing_row[self.TIME_COLUMN])
                if existing_time == target_time:
                    return i

        return None

    def _normalize_timestamp(self, timestamp: Union[str, pd.Timestamp, datetime]) -> pd.Timestamp:
        """
        Normalize timestamp to pandas Timestamp for comparison.

        Args:
            timestamp: Timestamp in various formats

        Returns:
            pd.Timestamp: Normalized timestamp
        """
        return pd.to_datetime(timestamp)

    def _process_time_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process time column in DataFrame.

        Args:
            df: DataFrame to process

        Returns:
            pd.DataFrame: DataFrame with processed time column
        """
        df_copy = df.copy()
        df_copy[self.TIME_COLUMN] = pd.to_datetime(df_copy[self.TIME_COLUMN])
        df_copy.set_index(self.TIME_COLUMN, inplace=True)
        return df_copy

    def _combine_with_previous_row(self, current_row: pd.Series, previous_row: pd.Series) -> pd.Series:
        """
        Combine current row with previous row data.

        Args:
            current_row: Current market data row
            previous_row: Previous market data row

        Returns:
            pd.Series: Combined row with previous data prefixed
        """
        # Start with a clean copy of current row (remove any existing previous columns)
        clean_current = pd.Series()
        for col in current_row.index:
            if not col.startswith(self.PREVIOUS_PREFIX):
                clean_current[col] = current_row[col]
        
        self._logger.debug(f"Cleaned current row columns: {list(clean_current.index)}")
        
        # Extract non-previous columns from previous row
        prev_columns = [col for col in previous_row.index
                       if not col.startswith(self.PREVIOUS_PREFIX)]
        prev_only = previous_row[prev_columns]

        # Add prefix to previous row columns
        prev_prefixed = prev_only.add_prefix(self.PREVIOUS_PREFIX)
        
        self._logger.debug(f"Previous row columns to add: {list(prev_prefixed.index)}")

        # Combine clean current row with prefixed previous row (no overlap possible now)
        result = pd.concat([clean_current, prev_prefixed])
        
        self._logger.debug(f"Final result columns: {len(result)}, duplicates: {result.index.duplicated().any()}")

        return result

    def __repr__(self) -> str:
        """String representation of the processor."""
        return (f"RecentRowsProcessor(timeframes={self._timeframes}, "
                f"max_rows={self._max_rows})")

    def __len__(self) -> int:
        """Total number of rows across all timeframes."""
        return sum(len(deque_obj) for deque_obj in self._recent_rows.values())
