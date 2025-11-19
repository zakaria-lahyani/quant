"""
IndicatorProcessor
======================================

A comprehensive, maintainable class for processing technical indicators across multiple timeframes.
Orchestrates indicator computation, data storage, and multi-timeframe analysis for both live trading
and backtesting scenarios.

Key Features:
- Multi-timeframe indicator processing
- Historical data initialization
- Recent row management with circular buffers
- Bulk and single-row processing modes
- Thread-safe operations for live trading
- Comprehensive error handling and validation

Architecture:
- Composition-based design with clear separation of concerns
- Dependency injection for testability
- Immutable operations preserving original data
- Lazy evaluation for performance optimization
"""
from collections import deque
from typing import Dict, Optional, List
import pandas as pd
import logging
from app.indicators.indicator_manager import IndicatorManager
from app.indicators.processors.historical_data_processor import HistoricalDataProcessor
from app.indicators.processors.recent_row_processor import RecentRowsProcessor


class IndicatorProcessor:
    """
    Orchestrates technical indicator processing across multiple timeframes.

    This class serves as the main coordinator for indicator computation, managing
    the interaction between indicator managers, historical data processors, and
    recent row storage. It supports both live trading and backtesting scenarios
    with efficient multi-timeframe processing.

    Key Responsibilities:
    - Initialize and manage indicator managers for each timeframe
    - Process new market data rows with indicator computation
    - Maintain recent row history for analysis
    - Provide access to historical and recent indicator data
    - Handle multi-timeframe data synchronization

    Design Principles:
    - Composition over inheritance for flexibility
    - Clear separation of concerns between components
    - Immutable operations to prevent data corruption
    - Comprehensive validation and error handling
    - Performance optimization through lazy evaluation

    Usage Patterns:
    - Live Trading: Sequential row processing with real-time indicators
    - Backtesting: Bulk processing with historical context
    - Analysis: Access to both historical and recent indicator data

    Thread Safety:
    - Safe for concurrent read operations
    - Write operations should be synchronized externally
    - Each timeframe operates independently
    """

    # Class constants for better maintainability
    DEFAULT_RECENT_ROWS_LIMIT = 6

    def __init__(self,
                 configs: Dict[str, dict],
                 historicals: Dict[str, pd.DataFrame],
                 is_bulk: bool,
                 recent_rows_limit: int = DEFAULT_RECENT_ROWS_LIMIT):
        """
        Initialize the IndicatorProcessor.

        Args:
            configs: Dictionary mapping timeframe to indicator configuration
                    Format: {'1m': {'sma': {'period': 20}}, '5m': {...}}
            historicals: Dictionary mapping timeframe to historical DataFrame
                        Format: {'1m': DataFrame, '5m': DataFrame}
            is_bulk: Whether to use bulk processing mode for indicators
            recent_rows_limit: Maximum number of recent rows to keep per timeframe

        Raises:
            ValueError: If configs and historicals don't have matching timeframes
            TypeError: If inputs are not of expected types
            KeyError: If required configuration keys are missing

        """
        # Validate inputs
        self._validate_initialization_params(configs, historicals, is_bulk, recent_rows_limit)

        # Store configuration
        self._is_bulk = is_bulk
        self._recent_rows_limit = recent_rows_limit
        self._timeframes = set(configs.keys())

        # Setup logging
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.info(f"Initializing IndicatorProcessor for timeframes: {list(self._timeframes)}")

        # Initialize core components
        self._managers = self._create_managers(configs, historicals)
        self._recent_rows_manager = RecentRowsProcessor(
            list(configs.keys()),
            max_rows=recent_rows_limit
        )

        # Initialize historical data
        self._historical_initializer = HistoricalDataProcessor(self._recent_rows_manager, max_rows=recent_rows_limit)
        self._initialize_historical_data()

        self._logger.info("IndicatorProcessor initialization completed successfully")

    def compute_indicators(self, timeframe: str, row: pd.Series) -> pd.Series:
        """
        Compute indicators for a single market data row.

        This method applies all configured indicators to the input row,
        returning a new Series with indicator values added. The original
        row data is preserved and not modified.

        Args:
            timeframe: The timeframe identifier (e.g., '1m', '5m', '1h')
            row: Market data row containing OHLCV data

        Returns:
            pd.Series: Row with computed indicator values added

        Raises:
            ValueError: If timeframe is not supported
            TypeError: If row is not a pandas Series
            KeyError: If required columns are missing from row

        """
        self._validate_timeframe(timeframe)
        self._validate_row_input(row)

        # Create defensive copy to prevent external modifications
        row_copy = row.copy()

        try:
            # Delegate indicator computation to the appropriate manager
            row_copy = self._managers[timeframe].compute_indicators(row_copy)

            self._logger.debug(f"Computed indicators for timeframe {timeframe}")
            return row_copy

        except Exception as e:
            self._logger.error(f"Failed to compute indicators for timeframe {timeframe}: {str(e)}")
            raise

    def process_new_row(self, timeframe: str, row: pd.Series, regime_data: Optional[Dict] = None) -> pd.Series:
        """
        Process a new market data row with indicator computation and storage.

        This is the main processing method that:
        1. Computes indicators for the input row
        2. Adds regime data if provided
        3. Stores the processed row in recent rows manager
        4. Returns the stored row for immediate use

        The method ensures data consistency by returning the actual stored
        row rather than the computed row, which may have been modified
        during storage (e.g., timestamp normalization).

        Args:
            timeframe: The timeframe identifier
            row: Raw market data row to process
            regime_data: Optional dict with regime, regime_confidence, is_transition keys

        Returns:
            pd.Series: Processed row with indicators and regime data, as stored in the system

        Raises:
            ValueError: If timeframe is not supported
            TypeError: If row is not a pandas Series
            ProcessingError: If indicator computation fails

        """
        self._validate_timeframe(timeframe)
        self._validate_row_input(row)

        try:
            # Step 1: Compute indicators
            row_with_indicators = self.compute_indicators(timeframe, row)

            # Step 2: Add regime data if provided
            if regime_data:
                row_with_indicators['regime'] = regime_data.get('regime', 'unknown')
                row_with_indicators['regime_confidence'] = regime_data.get('regime_confidence', 0.0)
                row_with_indicators['is_transition'] = regime_data.get('is_transition', False)

            # Step 3: Process the row with previous values and store it
            # This will add previous_* columns from the last stored row
            processed_row = self._recent_rows_manager.process_backtest_with_indicators_row(
                timeframe, row_with_indicators
            )

            # Step 4: Return the processed row with previous values and regime data
            return processed_row

        except Exception as e:
            self._logger.error(f"Failed to process new row for timeframe {timeframe}: {str(e)}")
            raise

    def process_new_row_mtf(self, new_rows: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
        """
        Process new rows for multiple timeframes simultaneously.

        This method provides efficient batch processing for multi-timeframe
        scenarios, such as when receiving synchronized market data across
        different timeframes. Each timeframe is processed independently,
        ensuring isolation and consistency.

        Args:
            new_rows: Dictionary mapping timeframe to new row data
                     Format: {'1m': Series, '5m': Series, '1h': Series}

        Returns:
            Dict[str, pd.Series]: Dictionary mapping timeframe to processed row

        Raises:
            ValueError: If any timeframe is not supported
            TypeError: If new_rows is not a dictionary or contains invalid data
            ProcessingError: If processing fails for any timeframe

        """
        self._validate_mtf_input(new_rows)

        results = {}
        failed_timeframes = []

        for timeframe, row in new_rows.items():
            try:
                results[timeframe] = self.process_new_row(timeframe, row)
            except Exception as e:
                failed_timeframes.append((timeframe, str(e)))
                self._logger.error(f"Failed to process row for timeframe {timeframe}: {str(e)}")

        if failed_timeframes:
            error_msg = f"Processing failed for timeframes: {failed_timeframes}"
            raise RuntimeError(error_msg)

        self._logger.debug(f"Successfully processed MTF data for {len(results)} timeframes")
        return results

    def get_recent_rows(self) ->  dict[str, deque]:
        """
        Get recent processed rows for a specific timeframe.

        Returns a DataFrame containing the most recent processed rows,
        including all computed indicators. Useful for analysis and
        visualization of recent market activity.

        Args:
            timeframe: The timeframe identifier
            min_rows: Minimum number of rows required (optional validation)

        Returns:
            pd.DataFrame: DataFrame of recent processed rows with indicators

        Raises:
            ValueError: If timeframe is not supported or insufficient data

        """
        # self._validate_timeframe(timeframe)

        df = self._recent_rows_manager.get_recent_rows()

        return df

    def get_latest_row(self, timeframe: str) -> Optional[pd.Series]:
        """
        Get the most recent processed row for a timeframe.

        Returns the latest row with all computed indicators, or None
        if no rows have been processed for the timeframe.

        Args:
            timeframe: The timeframe identifier

        Returns:
            Optional[pd.Series]: Most recent processed row or None

        Raises:
            ValueError: If timeframe is not supported

        """
        self._validate_timeframe(timeframe)
        return self._recent_rows_manager.get_latest_row(timeframe)

    def get_historical_indicator_data(self, timeframe: str) -> pd.DataFrame:
        """
        Get the complete historical indicator DataFrame for a timeframe.

        Returns the full historical dataset with all computed indicators,
        useful for backtesting, analysis, and visualization of long-term
        indicator behavior.

        Args:
            timeframe: The timeframe identifier

        Returns:
            pd.DataFrame: Complete historical data with indicators

        Raises:
            ValueError: If timeframe is not supported

        """
        self._validate_timeframe(timeframe)
        return self._managers[timeframe].get_historical_data()

    def get_supported_timeframes(self) -> List[str]:
        """
        Get list of supported timeframes.

        Returns:
            List[str]: List of supported timeframe identifiers
        """
        return sorted(list(self._timeframes))


    def clear_recent_data(self, timeframe: Optional[str] = None) -> None:
        """
        Clear recent row data for specified timeframe or all timeframes.

        Args:
            timeframe: Specific timeframe to clear, or None for all timeframes

        Raises:
            ValueError: If specified timeframe is not supported
        """
        if timeframe is not None:
            self._validate_timeframe(timeframe)
            self._recent_rows_manager.clear_timeframe(timeframe)
            self._logger.info(f"Cleared recent data for timeframe {timeframe}")
        else:
            self._recent_rows_manager.clear_all()
            self._logger.info("Cleared recent data for all timeframes")

    def has_sufficient_recent_data(self, timeframe: str, min_rows: int) -> bool:
        """
        Check if timeframe has sufficient recent data for analysis.

        Args:
            timeframe: The timeframe identifier
            min_rows: Minimum number of rows required

        Returns:
            bool: True if sufficient recent data is available

        Raises:
            ValueError: If timeframe is not supported
        """
        self._validate_timeframe(timeframe)
        return self._recent_rows_manager.has_sufficient_data(timeframe, min_rows)

    # Private helper methods

    def _validate_initialization_params(self,
                                      configs: Dict[str, dict],
                                      historicals: Dict[str, pd.DataFrame],
                                      is_bulk: bool,
                                      recent_rows_limit: int) -> None:
        """Validate initialization parameters."""
        if not isinstance(configs, dict) or not configs:
            raise ValueError("configs must be a non-empty dictionary")

        if not isinstance(historicals, dict) or not historicals:
            raise ValueError("historicals must be a non-empty dictionary")

        if not isinstance(is_bulk, bool):
            raise TypeError("is_bulk must be a boolean")

        if not isinstance(recent_rows_limit, int) or recent_rows_limit < 1:
            raise ValueError("recent_rows_limit must be a positive integer")

        # Validate timeframe consistency
        config_timeframes = set(configs.keys())
        historical_timeframes = set(historicals.keys())

        if config_timeframes != historical_timeframes:
            raise ValueError(
                f"Timeframe mismatch: configs has {config_timeframes}, "
                f"historicals has {historical_timeframes}"
            )

        # Validate DataFrame inputs
        for tf, df in historicals.items():
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Historical data for timeframe {tf} must be a DataFrame")
            if df.empty:
                raise ValueError(f"Historical data for timeframe {tf} cannot be empty")

    def _validate_timeframe(self, timeframe: str) -> None:
        """Validate that timeframe is supported."""
        if timeframe not in self._timeframes:
            raise ValueError(
                f"Unsupported timeframe: {timeframe}. "
                f"Supported timeframes: {sorted(list(self._timeframes))}"
            )

    def _validate_row_input(self, row: pd.Series) -> None:
        """Validate row input parameter."""
        if not isinstance(row, pd.Series):
            raise TypeError("row must be a pandas Series")

        if row.empty:
            raise ValueError("row cannot be empty")

    def _validate_mtf_input(self, new_rows: Dict[str, pd.Series]) -> None:
        """Validate multi-timeframe input."""
        if not isinstance(new_rows, dict):
            raise TypeError("new_rows must be a dictionary")

        if not new_rows:
            raise ValueError("new_rows cannot be empty")

        for tf, row in new_rows.items():
            self._validate_timeframe(tf)
            self._validate_row_input(row)

    def _create_managers(self,
                        configs: Dict[str, dict],
                        historicals: Dict[str, pd.DataFrame]) -> Dict[str, IndicatorManager]:
        """
        Create indicator managers for each timeframe.

        Args:
            configs: Indicator configurations by timeframe
            historicals: Historical data by timeframe

        Returns:
            Dict[str, IndicatorManager]: Managers by timeframe
        """
        managers = {}

        for tf in configs:
            try:
                managers[tf] = IndicatorManager(
                    historicals[tf],
                    configs[tf],
                    self._is_bulk
                )
                self._logger.debug(f"Created IndicatorManager for timeframe {tf}")
            except Exception as e:
                self._logger.error(f"Failed to create manager for timeframe {tf}: {str(e)}")
                raise

        return managers

    def _initialize_historical_data(self) -> None:
        """Initialize historical data in recent rows manager."""
        try:
            self._historical_initializer.initialize_from_historical(self._managers)
            self._logger.info("Historical data initialization completed")
        except Exception as e:
            self._logger.error(f"Failed to initialize historical data: {str(e)}")
            raise

    def __repr__(self) -> str:
        """String representation of the processor."""
        return (
            f"IndicatorProcessor("
            f"timeframes={sorted(list(self._timeframes))}, "
            f"is_bulk={self._is_bulk}, "
            f"recent_rows_limit={self._recent_rows_limit})"
        )

    def __len__(self) -> int:
        """Total number of recent rows across all timeframes."""
        return len(self._recent_rows_manager)
