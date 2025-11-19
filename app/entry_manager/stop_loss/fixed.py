"""
Fixed stop loss implementation.
"""

from typing import Optional, Dict, Any
import logging

from ..core.base import BaseStopLossCalculator
from ..core.exceptions import ValidationError
from ...strategy_builder.data.dtos import StopLossResult
from ...strategy_builder.core.domain.models import FixedStopLoss


class FixedStopLossCalculator(BaseStopLossCalculator):
    """Calculator for fixed stop loss levels."""
    
    def __init__(self, config: FixedStopLoss, pip_value: float, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != "fixed":
            raise ValidationError(
                f"Expected 'fixed' stop loss type, got {config.type}",
                field_name="type"
            )
        
        if config.value <= 0:
            raise ValidationError(
                "Stop loss value must be positive",
                field_name="value"
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
        Calculate fixed stop loss level.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Not used for fixed stop loss
            **kwargs: Additional parameters
            
        Returns:
            StopLossResult with calculated stop loss details
        """
        self._validate_inputs(entry_price, is_long, market_data)
        
        # Calculate stop loss level based on fixed value
        # The value can be interpreted as:
        # 1. Absolute price distance
        # 2. Percentage of entry price
        
        value_type = kwargs.get('value_type', 'absolute')  # 'absolute' or 'percentage'
        
        if value_type == 'percentage':
            # Treat value as percentage
            stop_distance = self._calculate_percentage_distance(entry_price, self.config.value)
        else:
            # Treat value as absolute price distance
            stop_distance = self.config.value
        
        # Convert pips to price distance using configurable pip_value
        if stop_distance >= 10:  # Likely pips, convert to price
            stop_distance = stop_distance / self.pip_value
        
        stop_level = self._calculate_stop_level(entry_price, stop_distance, is_long)
        
        # Handle trailing configuration if present
        trailing = False
        step = None
        if hasattr(self.config, 'trailing') and self.config.trailing:
            trailing = bool(self.config.trailing.enabled) if self.config.trailing.enabled is not None else False
            step = self.config.trailing.step if trailing else None
        
        self.logger.debug(
            f"Fixed stop loss: entry={entry_price}, is_long={is_long}, "
            f"value={self.config.value}, stop_level={stop_level}, "
            f"trailing={trailing}"
        )
        
        return StopLossResult(
            type="fixed",
            level=stop_level,
            trailing=trailing,
            step=step
        )
    
    def _calculate_percentage_distance(self, price: float, percentage: float) -> float:
        """Calculate distance based on percentage of price."""
        return price * (percentage / 100.0)
    
    def update_trailing_stop(
        self,
        current_stop: float,
        current_price: float,
        is_long: bool,
        step: float
    ) -> float:
        """
        Update trailing stop loss level.
        
        Args:
            current_stop: Current stop loss level
            current_price: Current market price
            is_long: Whether this is a long position
            step: Trailing step size (in pips for forex)
            
        Returns:
            Updated stop loss level
        """
        # Convert step from pips to price distance if needed
        step_distance = step
        if step >= 10:  # Likely pips, convert to price
            step_distance = step / self.pip_value
        
        if is_long:
            # For long positions, only move stop up
            potential_new_stop = current_price - step_distance
            new_stop = max(current_stop, potential_new_stop)
        else:
            # For short positions, only move stop down
            potential_new_stop = current_price + step_distance
            new_stop = min(current_stop, potential_new_stop)
        
        self.logger.debug(
            f"Trailing stop update: current_stop={current_stop}, "
            f"current_price={current_price}, new_stop={new_stop}, "
            f"is_long={is_long}, step={step}, step_distance={step_distance}"
        )
        
        return new_stop
    
    def validate_stop_level(
        self,
        entry_price: float,
        stop_level: float,
        is_long: bool,
        min_distance: Optional[float] = None
    ) -> bool:
        """
        Validate that the stop loss level is reasonable.
        
        Args:
            entry_price: Entry price
            stop_level: Calculated stop loss level
            is_long: Whether this is a long position
            min_distance: Minimum required distance from entry
            
        Returns:
            True if valid, False otherwise
        """
        # Check direction is correct
        if is_long and stop_level >= entry_price:
            return False
        if not is_long and stop_level <= entry_price:
            return False
        
        # Check minimum distance if specified
        if min_distance:
            actual_distance = abs(entry_price - stop_level)
            if actual_distance < min_distance:
                return False
        
        return True