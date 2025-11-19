"""
Trailing stop loss implementation.
"""

from typing import Optional, Dict, Any
import logging

from ..core.base import BaseStopLossCalculator
from ..core.exceptions import ValidationError
from ...strategy_builder.data.dtos import StopLossResult
from ...strategy_builder.core.domain.models import TrailingStopLossOnly


class TrailingStopLossCalculator(BaseStopLossCalculator):
    """Calculator for trailing stop loss levels."""
    
    def __init__(self, config: TrailingStopLossOnly, pip_value: float, logger: Optional[logging.Logger] = None):
        super().__init__(config, logger)
        
        if config.type != "trailing":
            raise ValidationError(
                f"Expected 'trailing' stop loss type, got {config.type}",
                field_name="type"
            )
        
        if config.step <= 0:
            raise ValidationError(
                "Trailing step must be positive",
                field_name="step"
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
        Calculate initial trailing stop loss level.
        
        Args:
            entry_price: The entry price for the trade
            is_long: Whether this is a long position
            market_data: Current market data (optional)
            **kwargs: Additional parameters
            
        Returns:
            StopLossResult with calculated trailing stop loss details
        """
        self._validate_inputs(entry_price, is_long, market_data)
        
        # Calculate initial stop level
        initial_stop = self._calculate_initial_stop_level(entry_price, is_long)
        
        self.logger.debug(
            f"Trailing stop loss: entry={entry_price}, is_long={is_long}, "
            f"step={self.config.step}, initial_stop={initial_stop}, "
            f"activation_price={getattr(self.config, 'activation_price', None)}"
        )
        
        return StopLossResult(
            type="fixed",  # Trailing stops are implemented as dynamic fixed stops
            level=initial_stop,
            trailing=True,
            step=self.config.step
        )
    
    def _calculate_initial_stop_level(self, entry_price: float, is_long: bool) -> float:
        """
        Calculate the initial stop loss level for trailing stop.
        
        Args:
            entry_price: Entry price
            is_long: Whether this is a long position
            
        Returns:
            Initial stop loss level
        """
        # Check if there's an activation price
        if hasattr(self.config, 'activation_price') and self.config.activation_price:
            # Convert activation price from pips to price distance
            activation_distance = self.config.activation_price / self.pip_value if self.config.activation_price >= 10 else self.config.activation_price
            # Use activation price as initial stop
            if is_long:
                return entry_price - activation_distance
            else:
                return entry_price + activation_distance
        else:
            # Convert step from pips to price distance
            step_distance = self.config.step / self.pip_value if self.config.step >= 10 else self.config.step
            # Use step as initial distance
            if is_long:
                return entry_price - step_distance
            else:
                return entry_price + step_distance
    
    def update_trailing_stop(
        self,
        current_stop: float,
        current_price: float,
        highest_price: float,
        lowest_price: float,
        is_long: bool,
        **kwargs
    ) -> float:
        """
        Update trailing stop loss based on price movement.
        
        Args:
            current_stop: Current stop loss level
            current_price: Current market price
            highest_price: Highest price since entry (for long positions)
            lowest_price: Lowest price since entry (for short positions)
            is_long: Whether this is a long position
            **kwargs: Additional parameters
            
        Returns:
            Updated stop loss level
        """
        # Convert step from pips to price distance
        step_distance = self.config.step / self.pip_value if self.config.step >= 10 else self.config.step
        
        if is_long:
            # For long positions, trail based on highest price
            new_stop = highest_price - step_distance
            
            # Only move stop up (never down)
            updated_stop = max(current_stop, new_stop)
            
            # Apply cap if specified
            if hasattr(self.config, 'cap') and self.config.cap:
                cap_distance = self.config.cap / self.pip_value if self.config.cap >= 10 else self.config.cap
                max_stop = current_price - cap_distance
                updated_stop = min(updated_stop, max_stop)
        else:
            # For short positions, trail based on lowest price
            new_stop = lowest_price + step_distance
            
            # Only move stop down (never up)
            updated_stop = min(current_stop, new_stop)
            
            # Apply cap if specified
            if hasattr(self.config, 'cap') and self.config.cap:
                cap_distance = self.config.cap / self.pip_value if self.config.cap >= 10 else self.config.cap
                min_stop = current_price + cap_distance
                updated_stop = max(updated_stop, min_stop)
        
        self.logger.debug(
            f"Trailing stop update: current_stop={current_stop}, "
            f"current_price={current_price}, "
            f"extreme_price={'highest' if is_long else 'lowest'}="
            f"{highest_price if is_long else lowest_price}, "
            f"updated_stop={updated_stop}"
        )
        
        return updated_stop
    
    def should_activate_trailing(
        self,
        entry_price: float,
        current_price: float,
        is_long: bool
    ) -> bool:
        """
        Check if trailing stop should be activated based on activation price.
        
        Args:
            entry_price: Entry price
            current_price: Current market price
            is_long: Whether this is a long position
            
        Returns:
            True if trailing should be activated
        """
        if not hasattr(self.config, 'activation_price') or not self.config.activation_price:
            # No activation price specified, always active
            return True
        
        # Convert activation price from pips to price distance
        activation_distance = self.config.activation_price / self.pip_value if self.config.activation_price >= 10 else self.config.activation_price
        
        if is_long:
            # For long positions, activate when price moves up by activation amount
            return current_price >= (entry_price + activation_distance)
        else:
            # For short positions, activate when price moves down by activation amount
            return current_price <= (entry_price - activation_distance)
    
    def calculate_profit_protection(
        self,
        entry_price: float,
        current_price: float,
        is_long: bool,
        protection_percentage: float = 50.0
    ) -> float:
        """
        Calculate a stop level that protects a percentage of current profit.
        
        Args:
            entry_price: Entry price
            current_price: Current market price
            is_long: Whether this is a long position
            protection_percentage: Percentage of profit to protect (0-100)
            
        Returns:
            Stop level that protects the specified profit percentage
        """
        if protection_percentage < 0 or protection_percentage > 100:
            raise ValidationError(
                "Protection percentage must be between 0 and 100",
                field_name="protection_percentage"
            )
        
        # Convert step from pips to price distance
        step_distance = self.config.step / self.pip_value if self.config.step >= 10 else self.config.step
        
        if is_long:
            if current_price <= entry_price:
                # No profit to protect
                return entry_price - step_distance
            
            profit = current_price - entry_price
            protected_profit = profit * (protection_percentage / 100.0)
            return entry_price + protected_profit
        else:
            if current_price >= entry_price:
                # No profit to protect
                return entry_price + step_distance
            
            profit = entry_price - current_price
            protected_profit = profit * (protection_percentage / 100.0)
            return entry_price - protected_profit
    
    def get_trailing_statistics(
        self,
        entry_price: float,
        current_stop: float,
        current_price: float,
        is_long: bool
    ) -> Dict[str, float]:
        """
        Get statistics about the trailing stop performance.
        
        Args:
            entry_price: Entry price
            current_stop: Current stop level
            current_price: Current market price
            is_long: Whether this is a long position
            
        Returns:
            Dictionary with trailing stop statistics
        """
        if is_long:
            unrealized_pnl = current_price - entry_price
            risk_amount = abs(current_stop - entry_price)  # Always positive risk amount
            protected_profit = current_stop - entry_price if current_stop > entry_price else 0
        else:
            unrealized_pnl = entry_price - current_price
            risk_amount = abs(current_stop - entry_price)  # Always positive risk amount
            protected_profit = entry_price - current_stop if current_stop < entry_price else 0
        
        return {
            'unrealized_pnl': unrealized_pnl,
            'risk_amount': risk_amount,
            'protected_profit': protected_profit,
            'risk_reward_ratio': unrealized_pnl / risk_amount if risk_amount > 0 else 0,
            'profit_protection_pct': (protected_profit / unrealized_pnl * 100) if unrealized_pnl > 0 else 0
        }