"""
Monetary stop loss implementation.
"""

from typing import Optional, Dict, Any
import logging

from ..core.base import BaseStopLossCalculator
from ..core.exceptions import ValidationError, CalculationError
from ...strategy_builder.data.dtos import StopLossResult
from ...strategy_builder.core.domain.models import StopLoss


class MonetaryStopLossCalculator(BaseStopLossCalculator):
    """Calculator for monetary-based stop loss levels."""
    
    def __init__(self, config: StopLoss, pip_value: float, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != "monetary":
            raise ValidationError(
                f"Expected 'monetary' stop loss type, got {config.type}",
                field_name="type"
            )
        
        # For monetary stop loss, config.value should be the dollar amount
        if not hasattr(config, 'value') or config.value <= 0:
            raise ValidationError(
                "Stop loss value (monetary amount) must be positive",
                field_name="value"
            )
        
        self.pip_value = pip_value
    
    def calculate_stop_loss(
        self,
        entry_price: float,
        is_long: bool,
        market_data: Optional[Dict[str, Any]] = None,
        position_size: Optional[float] = None,
        **kwargs
    ) -> StopLossResult:
        """
        Calculate monetary-based stop loss level.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Not used for monetary stop loss
            position_size: Position size in units/lots (required)
            **kwargs: Additional parameters
            
        Returns:
            StopLossResult with calculated stop loss details
        """
        self._validate_inputs(entry_price, is_long, market_data)
        
        if position_size is None or position_size <= 0:
            raise CalculationError(
                "Position size is required for monetary stop loss calculation",
                calculation_type="monetary_stop_loss"
            )
        
        # Calculate the price distance needed for the monetary amount
        # For a $500 loss: stop_distance = $500 / position_size
        stop_distance = self.config.value / position_size
        
        # Calculate stop level
        stop_level = self._calculate_stop_level(entry_price, stop_distance, is_long)
        
        # Handle trailing configuration if present
        trailing = False
        step = None
        if hasattr(self.config, 'trailing') and self.config.trailing:
            trailing = bool(self.config.trailing) if isinstance(self.config.trailing, bool) else False
            # For monetary trailing, step would be the step amount in dollars
            if hasattr(self.config, 'step'):
                step = self.config.step
        
        self.logger.debug(
            f"Monetary stop loss: entry={entry_price}, is_long={is_long}, "
            f"monetary_amount=${self.config.value}, position_size={position_size}, "
            f"stop_distance={stop_distance}, stop_level={stop_level}"
        )
        
        return StopLossResult(
            type="monetary",
            level=stop_level,
            trailing=trailing,
            step=step
        )
    
    def update_trailing_stop(
        self,
        current_stop: float,
        current_price: float,
        is_long: bool,
        step: float,
        position_size: float
    ) -> float:
        """
        Update trailing stop loss level for monetary-based stops.
        
        Args:
            current_stop: Current stop loss level
            current_price: Current market price
            is_long: Whether this is a long position
            step: Trailing step size in monetary terms
            position_size: Position size in units/lots
            
        Returns:
            Updated stop loss level
        """
        # Convert monetary step to price distance
        step_distance = step / position_size
        
        if is_long:
            # For long positions, only move stop up
            potential_new_stop = current_price - step_distance
            new_stop = max(current_stop, potential_new_stop)
        else:
            # For short positions, only move stop down
            potential_new_stop = current_price + step_distance
            new_stop = min(current_stop, potential_new_stop)
        
        self.logger.debug(
            f"Monetary trailing stop update: current_stop={current_stop}, "
            f"current_price={current_price}, new_stop={new_stop}, "
            f"is_long={is_long}, step=${step}, step_distance={step_distance}"
        )
        
        return new_stop