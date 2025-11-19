"""
Indicator-based stop loss implementation.
"""

from typing import Optional, Dict, Any
import logging

from ..core.base import BaseStopLossCalculator
from ..core.exceptions import ValidationError, InsufficientDataError
from ...strategy_builder.data.dtos import StopLossResult
from ...strategy_builder.core.domain.models import IndicatorBasedSlTp


class IndicatorStopLossCalculator(BaseStopLossCalculator):
    """Calculator for indicator-based stop loss levels."""
    
    def __init__(self, config: IndicatorBasedSlTp, pip_value: float, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != "indicator":
            raise ValidationError(
                f"Expected 'indicator' stop loss type, got {config.type}",
                field_name="type"
            )
        
        if not config.source or not config.source.strip():
            raise ValidationError(
                "Indicator source must be specified",
                field_name="source"
            )
        
        if pip_value <= 0:
            raise ValidationError(
                "pip_value must be positive",
                field_name="pip_value"
            )
        
        self.pip_value = pip_value
    
    def calculate_stop_loss(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> StopLossResult:
        """
        Calculate indicator-based stop loss level.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Market data containing indicator values (required)
            **kwargs: Additional parameters
            
        Returns:
            StopLossResult with calculated stop loss details
            
        Raises:
            InsufficientDataError: If required market data is not available
        """
        self._validate_inputs(entry_price, is_long, market_data)
        
        if not market_data:
            raise InsufficientDataError(
                "Market data is required for indicator-based stop loss",
                required_data="market_data"
            )
        
        # Extract indicator value from market data
        indicator_value = self._extract_indicator_value(market_data)
        
        # Apply offset if specified
        offset_value = self._calculate_offset(indicator_value)
        
        # Calculate stop loss level
        stop_level = self._calculate_indicator_stop_level(
            indicator_value, offset_value, is_long, entry_price
        )
        
        # Handle trailing configuration if present
        trailing = False
        step = None
        if hasattr(self.config, 'trailing') and self.config.trailing:
            trailing = bool(self.config.trailing.enabled) if self.config.trailing.enabled is not None else False
            step = self.config.trailing.step if trailing else None
        
        self.logger.debug(
            f"Indicator stop loss: entry={entry_price}, is_long={is_long}, "
            f"source={self.config.source}, indicator_value={indicator_value}, "
            f"offset={self.config.offset}, stop_level={stop_level}"
        )
        
        return StopLossResult(
            type="indicator",
            level=stop_level,
            source=self.config.source,
            trailing=trailing,
            step=step
        )
    
    def _extract_indicator_value(self, market_data: Dict[str, Any]) -> float:
        """
        Extract the indicator value from market data.
        
        Args:
            market_data: Market data dictionary
            
        Returns:
            Indicator value
            
        Raises:
            InsufficientDataError: If indicator data is not found
        """
        # Try different ways to access the indicator data
        indicator_source = self.config.source
        
        # Direct access
        if indicator_source in market_data:
            value = market_data[indicator_source]
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, list) and value:
                return float(value[-1])  # Use latest value
        
        # Try timeframe-specific access
        if hasattr(self.config, 'timeframe') and self.config.timeframe:
            timeframe_key = str(self.config.timeframe)
            if timeframe_key in market_data:
                tf_data = market_data[timeframe_key]
                if isinstance(tf_data, dict) and indicator_source in tf_data:
                    value = tf_data[indicator_source]
                    if isinstance(value, (int, float)):
                        return float(value)
                    elif isinstance(value, list) and value:
                        return float(value[-1])
        
        # Try nested access (e.g., indicators.RSI, technical.MACD)
        for key, data in market_data.items():
            if isinstance(data, dict) and indicator_source in data:
                value = data[indicator_source]
                if isinstance(value, (int, float)):
                    return float(value)
                elif isinstance(value, list) and value:
                    return float(value[-1])
        
        raise InsufficientDataError(
            f"Indicator '{indicator_source}' not found in market data",
            required_data=indicator_source
        )
    
    def _calculate_offset(self, indicator_value: float) -> float:
        """
        Calculate the offset value to apply to the indicator.
        
        Args:
            indicator_value: Base indicator value
            
        Returns:
            Offset value to apply
        """
        if self.config.offset == 0:
            return 0
        
        # Offset can be absolute or percentage
        # If offset is very small (< 0.01), treat as percentage
        # If offset is larger (>= 0.01), treat as absolute
        if abs(self.config.offset) < 0.01:
            # Percentage offset
            return indicator_value * self.config.offset
        else:
            # Absolute offset
            return self.config.offset
    
    def _calculate_indicator_stop_level(
        self,
        indicator_value: float,
        offset_value: float,
        is_long: bool,
        entry_price: float
    ) -> float:
        """
        Calculate the final stop loss level based on indicator and offset.
        
        Args:
            indicator_value: Base indicator value
            offset_value: Calculated offset
            is_long: Whether this is a long position
            entry_price: Entry price for validation
            
        Returns:
            Final stop loss level
        """
        if is_long:
            # For long positions, stop should be below indicator
            stop_level = indicator_value - abs(offset_value)
            
            # Ensure stop is below entry price
            if stop_level >= entry_price:
                pip_distance = 1.0 / self.pip_value  # 1 pip distance
                self.logger.warning(
                    f"Indicator stop level ({stop_level}) >= entry price ({entry_price}). "
                    f"Adjusting to entry price - 1 pip"
                )
                stop_level = entry_price - pip_distance  # Adjust by 1 pip
        else:
            # For short positions, stop should be above indicator
            stop_level = indicator_value + abs(offset_value)
            
            # Ensure stop is above entry price
            if stop_level <= entry_price:
                pip_distance = 1.0 / self.pip_value  # 1 pip distance
                self.logger.warning(
                    f"Indicator stop level ({stop_level}) <= entry price ({entry_price}). "
                    f"Adjusting to entry price + 1 pip"
                )
                stop_level = entry_price + pip_distance  # Adjust by 1 pip
        
        return stop_level
    
    def update_indicator_stop(
        self,
        current_stop: float,
        new_indicator_value: float,
        is_long: bool,
        **kwargs
    ) -> float:
        """
        Update stop loss based on new indicator value.
        
        Args:
            current_stop: Current stop loss level
            new_indicator_value: New indicator value
            is_long: Whether this is a long position
            **kwargs: Additional parameters
            
        Returns:
            Updated stop loss level
        """
        offset_value = self._calculate_offset(new_indicator_value)
        
        if is_long:
            new_stop = new_indicator_value - abs(offset_value)
            # Only move stop up for long positions
            updated_stop = max(current_stop, new_stop)
        else:
            new_stop = new_indicator_value + abs(offset_value)
            # Only move stop down for short positions
            updated_stop = min(current_stop, new_stop)
        
        self.logger.debug(
            f"Indicator stop update: current={current_stop}, "
            f"new_indicator={new_indicator_value}, updated={updated_stop}"
        )
        
        return updated_stop