"""
Fixed take profit implementation.
"""

from typing import Optional, Dict, Any
import logging

from ..core.base import BaseTakeProfitCalculator
from ..core.exceptions import ValidationError
from ...strategy_builder.data.dtos import TakeProfitResult
from ...strategy_builder.core.domain.models import FixedTakeProfit


class FixedTakeProfitCalculator(BaseTakeProfitCalculator):
    """Calculator for fixed take profit levels."""
    
    def __init__(self, config: FixedTakeProfit, pip_value: float, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != "fixed":
            raise ValidationError(
                f"Expected 'fixed' take profit type, got {config.type}",
                field_name="type"
            )
        
        if config.value <= 0:
            raise ValidationError(
                "Take profit value must be positive",
                field_name="value"
            )
        
        if pip_value <= 0:
            raise ValidationError(
                "pip_value must be positive",
                field_name="pip_value"
            )
        
        self.pip_value = pip_value
    
    def calculate_take_profit(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> TakeProfitResult:
        """
        Calculate fixed take profit level.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Not used for fixed take profit
            **kwargs: Additional parameters
            
        Returns:
            TakeProfitResult with calculated take profit details
        """
        self._validate_inputs(entry_price, is_long, market_data)
        
        # Calculate take profit level based on fixed value
        # The value can be interpreted as:
        # 1. Absolute price distance (in pips for forex)
        # 2. Percentage of entry price
        
        value_type = kwargs.get('value_type', 'absolute')  # 'absolute' or 'percentage'
        
        if value_type == 'percentage':
            # Treat value as percentage
            profit_distance = self._calculate_percentage_distance(entry_price, self.config.value)
        else:
            # Treat value as absolute price distance in pips
            # Convert pips to price distance using configurable pip_value
            profit_distance = self.config.value / self.pip_value
        
        profit_level = self._calculate_profit_level(entry_price, profit_distance, is_long)
        
        self.logger.debug(
            f"Fixed take profit: entry={entry_price}, is_long={is_long}, "
            f"value={self.config.value}, profit_level={profit_level}"
        )
        
        return TakeProfitResult(
            type="fixed",
            level=profit_level,
            percent=100.0
        )
    
    def _calculate_percentage_distance(self, price: float, percentage: float) -> float:
        """Calculate distance based on percentage of price."""
        return price * (percentage / 100.0)
    
    def calculate_risk_reward_ratio(
        self,
        entry_price: float,
        stop_loss: float,
        is_long: bool,
        **kwargs
    ) -> float:
        """
        Calculate the risk-reward ratio for this take profit level.
        
        Args:
            entry_price: Entry price
            stop_loss: Stop loss level
            is_long: Whether this is a long position
            **kwargs: Additional parameters
            
        Returns:
            Risk-reward ratio
        """
        tp_result = self.calculate_take_profit(entry_price, is_long, **kwargs)
        take_profit = tp_result.level
        
        if is_long:
            risk = entry_price - stop_loss
            reward = take_profit - entry_price
        else:
            risk = stop_loss - entry_price
            reward = entry_price - take_profit
        
        if risk <= 0:
            return float('inf')
        
        return reward / risk
    
    def validate_profit_level(
        self,
        entry_price: float,
        profit_level: float,
        is_long: bool,
        min_distance: Optional[float] = None
    ) -> bool:
        """
        Validate that the take profit level is reasonable.
        
        Args:
            entry_price: Entry price
            profit_level: Calculated take profit level
            is_long: Whether this is a long position
            min_distance: Minimum required distance from entry
            
        Returns:
            True if valid, False otherwise
        """
        # Check direction is correct
        if is_long and profit_level <= entry_price:
            return False
        if not is_long and profit_level >= entry_price:
            return False
        
        # Check minimum distance if specified
        if min_distance:
            actual_distance = abs(profit_level - entry_price)
            if actual_distance < min_distance:
                return False
        
        return True
    
    def calculate_position_value_at_tp(
        self,
        entry_price: float,
        position_size: float,
        is_long: bool,
        **kwargs
    ) -> Dict[str, float]:
        """
        Calculate position value and profit at take profit level.
        
        Args:
            entry_price: Entry price
            position_size: Position size in base currency
            is_long: Whether this is a long position
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with position value details
        """
        tp_result = self.calculate_take_profit(entry_price, is_long, **kwargs)
        take_profit = tp_result.level
        
        units = position_size / entry_price
        
        if is_long:
            profit_per_unit = take_profit - entry_price
        else:
            profit_per_unit = entry_price - take_profit
        
        total_profit = profit_per_unit * units
        profit_percentage = (profit_per_unit / entry_price) * 100
        
        return {
            'take_profit_level': take_profit,
            'position_units': units,
            'profit_per_unit': profit_per_unit,
            'total_profit': total_profit,
            'profit_percentage': profit_percentage,
            'final_position_value': position_size + total_profit
        }