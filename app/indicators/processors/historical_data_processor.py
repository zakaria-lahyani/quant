from app.indicators.indicator_manager import IndicatorManager
from app.indicators.processors.recent_row_processor import RecentRowsProcessor

from typing import Dict
import logging
import pandas as pd

class HistoricalDataProcessor:
    DEFAULT_MAX_ROWS = 7
    
    def __init__(self, recent_rows_manager: RecentRowsProcessor, max_rows: int = DEFAULT_MAX_ROWS):
        self.recent_rows_manager = recent_rows_manager
        self.max_rows = max_rows
        self.logger = logging.getLogger(self.__class__.__name__)

    def initialize_from_historical(self, managers: Dict[str, IndicatorManager]) -> None:
        """Initialize recent rows from historical data with computed indicators"""
        for tf, manager in managers.items():
            self._initialize_timeframe_with_clean_previous(tf, manager)
    
    def _create_clean_row_with_previous(self, current_row: pd.Series, previous_row: pd.Series) -> pd.Series:
        """
        Create a clean row with previous values, avoiding any duplicates.
        
        Args:
            current_row: The current row data with indicators
            previous_row: The previous row to extract previous values from
            
        Returns:
            A new Series with current values and clean previous_ columns
        """
        # Create a new series with only non-previous columns from current row
        clean_current = pd.Series()
        for col in current_row.index:
            if not col.startswith('previous_'):
                clean_current[col] = current_row[col]
        
        # Add previous values from the previous row (only non-previous columns)
        for col in previous_row.index:
            if not col.startswith('previous_'):
                clean_current[f'previous_{col}'] = previous_row[col]
        
        return clean_current

    def _initialize_timeframe_with_clean_previous(self, tf: str, manager: IndicatorManager) -> None:
        """
        Initialize recent rows for a single timeframe with clean previous values.
        
        This ensures:
        1. We get max_rows + 1 from historical data
        2. Each row in the deque has previous values from the row before it
        3. No duplicate previous columns are created
        """
        # Get the computed indicator data
        indicator_data = manager.get_historical_data()
        
        if indicator_data.empty:
            self.logger.warning(f"No indicator data available for timeframe {tf}")
            return
        
        # Get max_rows + 1 so we have a "previous" row for the first row in deque
        last_indicator_rows = indicator_data.tail(self.max_rows + 1)
        
        if len(last_indicator_rows) < 2:
            self.logger.warning(f"Not enough historical data for timeframe {tf} (need at least 2 rows)")
            return
        
        self.logger.info(f"Initializing {self.max_rows} rows for timeframe {tf} with clean previous values")
        
        # Convert to list for easier processing
        rows_list = list(last_indicator_rows.iterrows())
        
        # Process each row starting from index 1 (so we have a previous row)
        for i in range(1, len(rows_list)):
            _, current_row = rows_list[i]
            _, previous_row = rows_list[i-1]
            
            # Create a clean row with previous values
            clean_row = self._create_clean_row_with_previous(current_row, previous_row)
            
            # Add to the deque - use native add without processing
            # This avoids any additional previous column manipulation
            self.recent_rows_manager._recent_rows[tf].append(clean_row)
            
            if i == 1:  # First row being added to deque
                self.logger.debug(f"First deque row - time: {clean_row.get('time', 'N/A')}")
                self.logger.debug(f"Previous values from time: {previous_row.get('time', 'N/A')}")
                # Log a sample of previous values to verify
                prev_cols = [col for col in clean_row.index if col.startswith('previous_')]
                if prev_cols:
                    sample = [(col, clean_row[col]) for col in prev_cols[:3]]
                    self.logger.debug(f"Sample previous values: {sample}")
        
        self.logger.info(f"Successfully initialized {len(self.recent_rows_manager._recent_rows[tf])} rows for {tf}")